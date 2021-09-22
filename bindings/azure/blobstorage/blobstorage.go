// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package blobstorage

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
)

const (
	// Used to reference the blob relative to the container
	metadataKeyBlobName = "blobName"
	metadataKeyOffset = "offset"
	metadataKeyCount = "count"
	metadataKeyData = "data"
	// A string value that identifies the portion of the list to be returned with the next list operation.
	// The operation returns a marker value within the response body if the list returned was not complete. The marker
	// value may then be used in a subsequent call to request the next set of list items.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs#uri-parameters
	metadataKeyMarker = "marker"
	// The number of blobs that will be returned in a list operation
	metadataKeyNumber = "number"
	// Defines if the user defined metadata should be returned in the get operation
	metadataKeyIncludeMetadata = "includeMetadata"
	// Defines the delete snapshots option for the delete operation.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob#request-headers
	metadataKeyDeleteSnapshots = "deleteSnapshots"
	// HTTP headers to be associated with the blob.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#request-headers-all-blob-types
	metadataKeyContentType        = "contentType"
	metadataKeyContentMD5         = "contentMD5"
	metadataKeyContentEncoding    = "contentEncoding"
	metadataKeyContentLanguage    = "contentLanguage"
	metadataKeyContentDisposition = "contentDisposition"
	meatdataKeyCacheControl       = "cacheControl"
	// Specifies the maximum number of HTTP GET requests that will be made while reading from a RetryReader. A value
	// of zero means that no additional HTTP GET requests will be made
	defaultGetBlobRetryCount = 10
	// Specifies the maximum number of blobs to return, including all BlobPrefix elements. If the request does not
	// specify maxresults the server will return up to 5,000 items.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs#uri-parameters
	maxResults = 5000

	// TODO: remove the pascal case support when the component moves to GA
	// See: https://github.com/dapr/components-contrib/pull/999#issuecomment-876890210
	metadataKeyContentTypeBC           = "ContentType"
	metadataKeyContentMD5BC            = "ContentMD5"
	metadataKeyContentEncodingBC       = "ContentEncoding"
	metadataKeyContentLanguageBC       = "ContentLanguage"
	metadataKeyContentDispositionBC    = "ContentDisposition"
	metadataKeyCacheControlBC          = "CacheControl"
	metadataKeyDeleteSnapshotOptionsBC = "DeleteSnapshotOptions"

	Head = "head"
	Put = "put"
	PutBlockList ="putblocklist"
)

var ErrMissingBlobName = errors.New("blobName is a required attribute")

// AzureBlobStorage allows saving blobs to an Azure Blob Storage account
type AzureBlobStorage struct {
	metadata     *blobStorageMetadata
	containerURL azblob.ContainerURL
	BlockIDs []string

	logger logger.Logger
}

type blobStorageMetadata struct {
	StorageAccount    string                  `json:"storageAccount"`
	StorageAccessKey  string                  `json:"storageAccessKey"`
	Container         string                  `json:"container"`
	GetBlobRetryCount int                     `json:"getBlobRetryCount,string"`
	DecodeBase64      bool                    `json:"decodeBase64,string"`
	PublicAccessLevel azblob.PublicAccessType `json:"publicAccessLevel"`
}

type createResponse struct {
	BlobURL string `json:"blobURL"`
}

type listInclude struct {
	Copy             bool `json:"copy"`
	Metadata         bool `json:"metadata"`
	Snapshots        bool `json:"snapshots"`
	UncommittedBlobs bool `json:"uncommittedBlobs"`
	Deleted          bool `json:"deleted"`
}

type listPayload struct {
	Marker     string      `json:"marker"`
	Prefix     string      `json:"prefix"`
	MaxResults int32       `json:"maxResults"`
	Include    listInclude `json:"include"`
}

// NewAzureBlobStorage returns a new Azure Blob Storage instance
func NewAzureBlobStorage(logger logger.Logger) *AzureBlobStorage {
	return &AzureBlobStorage{logger: logger}
}

// Init performs metadata parsing
func (a *AzureBlobStorage) Init(metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	credential, err := azblob.NewSharedKeyCredential(m.StorageAccount, m.StorageAccessKey)
	if err != nil {
		return fmt.Errorf("invalid credentials with error: %w", err)
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	containerName := a.metadata.Container
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", m.StorageAccount, containerName))
	containerURL := azblob.NewContainerURL(*URL, p)

	ctx := context.Background()
	_, err = containerURL.Create(ctx, azblob.Metadata{}, m.PublicAccessLevel)
	// Don't return error, container might already exist
	a.logger.Debugf("error creating container: %w", err)
	a.containerURL = containerURL
	a.BlockIDs = make([]string, 0)

	return nil
}

func (a *AzureBlobStorage) parseMetadata(metadata bindings.Metadata) (*blobStorageMetadata, error) {
	connInfo := metadata.Properties
	b, err := json.Marshal(connInfo)
	if err != nil {
		return nil, err
	}

	var m blobStorageMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	if m.GetBlobRetryCount == 0 {
		m.GetBlobRetryCount = defaultGetBlobRetryCount
	}

	if !a.isValidPublicAccessType(m.PublicAccessLevel) {
		return nil, fmt.Errorf("invalid public access level: %s; allowed: %s",
			m.PublicAccessLevel, azblob.PossiblePublicAccessTypeValues())
	}

	return &m, nil
}

func (a *AzureBlobStorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
		Head,
		Put,
		PutBlockList,
	}
}

func (a *AzureBlobStorage) create(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobHTTPHeaders azblob.BlobHTTPHeaders
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
		delete(req.Metadata, metadataKeyBlobName)
	} else {
		blobURL = a.getBlobURL(uuid.New().String())
	}

	if val, ok := req.Metadata[metadataKeyContentType]; ok && val != "" {
		blobHTTPHeaders.ContentType = val
		delete(req.Metadata, metadataKeyContentType)
	}
	if val, ok := req.Metadata[metadataKeyContentMD5]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return nil, fmt.Errorf("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
		}
		blobHTTPHeaders.ContentMD5 = sDec
		delete(req.Metadata, metadataKeyContentMD5)
	}
	if val, ok := req.Metadata[metadataKeyContentEncoding]; ok && val != "" {
		blobHTTPHeaders.ContentEncoding = val
		delete(req.Metadata, metadataKeyContentEncoding)
	}
	if val, ok := req.Metadata[metadataKeyContentLanguage]; ok && val != "" {
		blobHTTPHeaders.ContentLanguage = val
		delete(req.Metadata, metadataKeyContentLanguage)
	}
	if val, ok := req.Metadata[metadataKeyContentDisposition]; ok && val != "" {
		blobHTTPHeaders.ContentDisposition = val
		delete(req.Metadata, metadataKeyContentDisposition)
	}
	if val, ok := req.Metadata[meatdataKeyCacheControl]; ok && val != "" {
		blobHTTPHeaders.CacheControl = val
		delete(req.Metadata, meatdataKeyCacheControl)
	}

	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	if a.metadata.DecodeBase64 {
		decoded, decodeError := b64.StdEncoding.DecodeString(string(req.Data))
		if decodeError != nil {
			return nil, decodeError
		}
		req.Data = decoded
	}

	_, err = azblob.UploadBufferToBlockBlob(context.Background(), req.Data, blobURL, azblob.UploadToBlockBlobOptions{
		Parallelism:     16,
		Metadata:        req.Metadata,
		BlobHTTPHeaders: blobHTTPHeaders,
	})
	if err != nil {
		return nil, fmt.Errorf("error uploading az blob: %w", err)
	}

	resp := createResponse{
		BlobURL: blobURL.String(),
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("error marshalling create response for azure blob: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (a *AzureBlobStorage) get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
	} else {
		return nil, ErrMissingBlobName
	}

	ctx := context.TODO()
	offset, err := strconv.ParseInt(req.Metadata[metadataKeyOffset],10,64)
	if err != nil {
		offset = 0
	}
	count, err := strconv.ParseInt(req.Metadata[metadataKeyCount],10,64)
	if err != nil {
		count = 0
	}
	resp, err := blobURL.Download(ctx, offset, count, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, fmt.Errorf("error downloading az blob: %w", err)
	}

	bodyStream := resp.Body(azblob.RetryReaderOptions{MaxRetryRequests: a.metadata.GetBlobRetryCount})
	b := bytes.Buffer{}
	_, err = b.ReadFrom(bodyStream)
	if err != nil {
		return nil, fmt.Errorf("error reading az blob body: %w", err)
	}

	metadata := make(map[string]string)
	fetchMetadata, err := req.GetMetadataAsBool(metadataKeyIncludeMetadata)
	if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if fetchMetadata {
		props, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
		if err != nil {
			return nil, fmt.Errorf("error reading blob metadata: %w", err)
		}

		metadata = props.NewMetadata()
	}

	return &bindings.InvokeResponse{
		Data:     b.Bytes(),
		Metadata: metadata,
	}, nil
}

func (a *AzureBlobStorage) delete(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
	} else {
		return nil, ErrMissingBlobName
	}

	deleteSnapshotsOptions := azblob.DeleteSnapshotsOptionNone
	if val, ok := req.Metadata[metadataKeyDeleteSnapshots]; ok && val != "" {
		deleteSnapshotsOptions = azblob.DeleteSnapshotsOptionType(val)
		if !a.isValidDeleteSnapshotsOptionType(deleteSnapshotsOptions) {
			return nil, fmt.Errorf("invalid delete snapshot option type: %s; allowed: %s",
				deleteSnapshotsOptions, azblob.PossibleDeleteSnapshotsOptionTypeValues())
		}
	}

	_, err := blobURL.Delete(context.Background(), deleteSnapshotsOptions, azblob.BlobAccessConditions{})

	return nil, err
}

func (a *AzureBlobStorage) list(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	options := azblob.ListBlobsSegmentOptions{}

	var payload listPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	options.Details.Copy = payload.Include.Copy
	options.Details.Metadata = payload.Include.Metadata
	options.Details.Snapshots = payload.Include.Snapshots
	options.Details.UncommittedBlobs = payload.Include.UncommittedBlobs
	options.Details.Deleted = payload.Include.Deleted

	if payload.MaxResults != int32(0) {
		options.MaxResults = payload.MaxResults
	} else {
		options.MaxResults = maxResults
	}

	if payload.Prefix != "" {
		options.Prefix = payload.Prefix
	}

	var initialMarker azblob.Marker
	if payload.Marker != "" {
		initialMarker = azblob.Marker{Val: &payload.Marker}
	} else {
		initialMarker = azblob.Marker{}
	}

	var blobs []azblob.BlobItem
	metadata := map[string]string{}
	ctx := context.Background()
	for currentMaker := initialMarker; currentMaker.NotDone(); {
		var listBlob *azblob.ListBlobsFlatSegmentResponse
		listBlob, err = a.containerURL.ListBlobsFlatSegment(ctx, currentMaker, options)
		if err != nil {
			return nil, fmt.Errorf("error listing blobs: %w", err)
		}

		blobs = append(blobs, listBlob.Segment.BlobItems...)

		numBlobs := len(blobs)
		currentMaker = listBlob.NextMarker
		metadata[metadataKeyMarker] = *currentMaker.Val
		metadata[metadataKeyNumber] = strconv.FormatInt(int64(numBlobs), 10)

		if options.MaxResults-maxResults > 0 {
			options.MaxResults -= maxResults
		} else {
			break
		}
	}

	jsonResponse, err := json.Marshal(blobs)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal blobs to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data:     jsonResponse,
		Metadata: metadata,
	}, nil
}

func (a *AzureBlobStorage) head(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
	} else {
		return nil, ErrMissingBlobName
	}

	ctx := context.TODO()
	metadata := make(map[string]string)
	props, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return nil, fmt.Errorf("error reading blob metadata: %w", err)
	}
	resp := props.Response()
	for k, v := range resp.Header {
		if(k=="Content-Length"){
				metadata[k] = v[0]
		}
	}

	if _, ok := metadata["Content-Length"]; !ok {
		return nil, fmt.Errorf("Missing Content Length")
	}

	return &bindings.InvokeResponse{
		Metadata: metadata,
	}, nil
}

func (a *AzureBlobStorage) put(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
	} else {
		return nil, ErrMissingBlobName
	}

	buffer, _ := hex.DecodeString(req.Metadata[metadataKeyData])

	var BlockID string
	Leadingzeros := make([]byte, 64-len(req.Metadata[metadataKeyOffset]))
	//Add leading zeroes to make all BlockIds have the same length
	for i:=0; i<len(BlockID); i++ {
		Leadingzeros[i] = '0'
	}
	BlockID = string(Leadingzeros) + req.Metadata[metadataKeyOffset]
	a.BlockIDs = append(a.BlockIDs, BlockID)

	ctx := context.TODO()
	_, err := blobURL.StageBlock(ctx,BlockID,bytes.NewReader(buffer), azblob.LeaseAccessConditions{}, nil)

	return &bindings.InvokeResponse{}, err
}

func (a *AzureBlobStorage) putblocklist(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobURL azblob.BlockBlobURL
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobURL = a.getBlobURL(val)
	} else {
		return nil, ErrMissingBlobName
	}

	sort.Strings(a.BlockIDs)
	ctx := context.TODO()
	ModifiedAccessConditions := azblob.ModifiedAccessConditions{}
	LeaseAccessConditions := azblob.LeaseAccessConditions{}
	BlobAccessConditions := azblob.BlobAccessConditions{ModifiedAccessConditions: ModifiedAccessConditions, LeaseAccessConditions: LeaseAccessConditions}

	_, err := blobURL.CommitBlockList(ctx, a.BlockIDs,azblob.BlobHTTPHeaders{},azblob.Metadata{},BlobAccessConditions)
	a.BlockIDs = nil

	return &bindings.InvokeResponse{}, err
}


func (a *AzureBlobStorage) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	req.Metadata = a.handleBackwardCompatibilityForMetadata(req.Metadata)

	switch req.Operation {
	case bindings.CreateOperation:
		return a.create(req)
	case bindings.GetOperation:
		return a.get(req)
	case bindings.DeleteOperation:
		return a.delete(req)
	case bindings.ListOperation:
		return a.list(req)
	case Head:
		return a.head(req)
	case Put:
		return a.put(req)
	case PutBlockList:
		return a.putblocklist(req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (a *AzureBlobStorage) getBlobURL(name string) azblob.BlockBlobURL {
	blobURL := a.containerURL.NewBlockBlobURL(name)

	return blobURL
}

func (a *AzureBlobStorage) isValidPublicAccessType(accessType azblob.PublicAccessType) bool {
	validTypes := azblob.PossiblePublicAccessTypeValues()
	for _, item := range validTypes {
		if item == accessType {
			return true
		}
	}

	return false
}

func (a *AzureBlobStorage) isValidDeleteSnapshotsOptionType(accessType azblob.DeleteSnapshotsOptionType) bool {
	validTypes := azblob.PossibleDeleteSnapshotsOptionTypeValues()
	for _, item := range validTypes {
		if item == accessType {
			return true
		}
	}

	return false
}

// TODO: remove the pascal case support when the component moves to GA
// See: https://github.com/dapr/components-contrib/pull/999#issuecomment-876890210
func (a *AzureBlobStorage) handleBackwardCompatibilityForMetadata(metadata map[string]string) map[string]string {
	if val, ok := metadata[metadataKeyContentTypeBC]; ok && val != "" {
		metadata[metadataKeyContentType] = val
		delete(metadata, metadataKeyContentTypeBC)
	}

	if val, ok := metadata[metadataKeyContentMD5BC]; ok && val != "" {
		metadata[metadataKeyContentMD5] = val
		delete(metadata, metadataKeyContentMD5BC)
	}

	if val, ok := metadata[metadataKeyContentEncodingBC]; ok && val != "" {
		metadata[metadataKeyContentEncoding] = val
		delete(metadata, metadataKeyContentEncodingBC)
	}

	if val, ok := metadata[metadataKeyContentLanguageBC]; ok && val != "" {
		metadata[metadataKeyContentLanguage] = val
		delete(metadata, metadataKeyContentLanguageBC)
	}

	if val, ok := metadata[metadataKeyContentDispositionBC]; ok && val != "" {
		metadata[metadataKeyContentDisposition] = val
		delete(metadata, metadataKeyContentDispositionBC)
	}

	if val, ok := metadata[metadataKeyCacheControlBC]; ok && val != "" {
		metadata[meatdataKeyCacheControl] = val
		delete(metadata, metadataKeyCacheControlBC)
	}

	if val, ok := metadata[metadataKeyDeleteSnapshotOptionsBC]; ok && val != "" {
		metadata[metadataKeyDeleteSnapshots] = val
		delete(metadata, metadataKeyDeleteSnapshotOptionsBC)
	}

	return metadata
}
