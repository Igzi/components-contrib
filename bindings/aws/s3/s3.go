// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package s3

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_auth "github.com/dapr/components-contrib/authentication/aws"
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
)

// AWSS3 is a binding for an AWS S3 storage bucket
type AWSS3 struct {
	metadata  *s3Metadata
	session *session.Session
	uploader *s3manager.Uploader
	downloader *s3manager.Downloader
	logger   logger.Logger
	backups map[string]Backup
	S3 *s3.S3
}

type Backup struct{
	PartNum int
	MultipartResponse *s3.CreateMultipartUploadOutput
	CompletedParts []Part
}

type Part struct{
	CompletedPart *s3.CompletedPart
	Offset int64
}

type s3Metadata struct {
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Bucket       string `json:"bucket"`
}

// NewAWSS3 returns a new AWSS3 instance
func NewAWSS3(logger logger.Logger) *AWSS3 {
	return &AWSS3{logger: logger}
}

// Init does metadata parsing and connection creation
func (s *AWSS3) Init(metadata bindings.Metadata) error {
	m, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	err = s.getClient(m)
	if err != nil {
		return err
	}
	s.metadata = m
	s.backups = make(map[string]Backup)

	return nil
}

func (s *AWSS3) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		"head",
		"put",
		"putblocklist",
	}
}

func (s *AWSS3) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {

	switch req.Operation {
	case bindings.CreateOperation:
		return s.create(req)
	case bindings.GetOperation:
		return s.get(req)
	case bindings.DeleteOperation:
		return s.delete(req)
	case "head":
		return s.head(req)
	case "put":
		return s.put(req)
	case "putblocklist":
		return s.putblocklist(req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (s *AWSS3) create(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	//Check if the file already exists
	_, err := s.head(req)
	if(err == nil){
		return nil, fmt.Errorf("File already exisists")
	}

	key := ""
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		s.logger.Debugf("key not found. generating key %s", key)
	}

	r := bytes.NewReader(req.Data)
	requestInput := s3manager.UploadInput{
		Bucket: aws.String(s.metadata.Bucket),
		Key:    aws.String(key),
		Body:   r,
	}

	_, err = s.uploader.Upload(&requestInput)

	return nil, err
}

func (s *AWSS3) delete(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	key := ""
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		s.logger.Debugf("key not found. generating key %s", key)
	}

	requestInput := s3.DeleteObjectInput{
		Bucket: aws.String(s.metadata.Bucket),
		Key:    aws.String(key),
	}

	_, err := s.S3.DeleteObject(&requestInput)

	return nil, err
}

func (s *AWSS3) get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	b := aws.NewWriteAtBuffer([]byte{})
	metadata := make(map[string]string)

	offset, offseterr := strconv.ParseInt(req.Metadata[metadataKeyOffset],10,64)
	count, counterr := strconv.ParseInt(req.Metadata[metadataKeyCount],10,64)

	if(offseterr == nil && counterr == nil) {
		s3range := "bytes=" + req.Metadata[metadataKeyOffset] + "-" + strconv.FormatInt(offset + count -1, 10)

		requestInput := s3.GetObjectInput{
			Bucket: aws.String(s.metadata.Bucket),
			Key:    aws.String(req.Metadata[metadataKeyBlobName]),
			Range:  aws.String(s3range),
		}

		s.downloader.Download(b,&requestInput)
	} else{

		requestInput := s3.GetObjectInput{
			Bucket: aws.String(s.metadata.Bucket),
			Key:    aws.String(req.Metadata[metadataKeyBlobName]),
		}

		s.downloader.Download(b,&requestInput)
	}

	return &bindings.InvokeResponse{
		Data:     b.Bytes(),
		Metadata: metadata,
	}, nil
}

func (s *AWSS3) head(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	requestInput := s3.HeadObjectInput{
		Bucket: aws.String(s.metadata.Bucket),
		Key:    aws.String(req.Metadata[metadataKeyBlobName]),
	}


	result, err := s.S3.HeadObject(&requestInput)

	metadata := make(map[string]string)
	metadata["Content-Length"] = strconv.FormatInt(aws.Int64Value(result.ContentLength), 10)

	return &bindings.InvokeResponse{
	Data:     nil,
	Metadata: metadata,
	}, err
}

func (s *AWSS3) initbackup(key string ) (error){
	backup, ok := s.backups[key]

	if(ok) {
		return nil
	}

	MultipartResponse, err := s.S3.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.metadata.Bucket),
		Key:    aws.String(key),
	})
	if(err != nil){
		return err
	}
	backup.MultipartResponse = MultipartResponse
	backup.PartNum = 1
	backup.CompletedParts = nil
	s.backups[key] = backup

	return nil
}

func (s *AWSS3) put(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	buffer, _ := hex.DecodeString(req.Metadata[metadataKeyData])
	key, _ := req.Metadata[metadataKeyBlobName]
	err := s.initbackup(key)

	if(err != nil){
		return nil,err
	}

	backup := s.backups[key]
	uploadResp, err := s.S3.UploadPart(&s3.UploadPartInput{
		Body:          bytes.NewReader(buffer),
		Bucket:        backup.MultipartResponse.Bucket,
		Key:           backup.MultipartResponse.Key,
		PartNumber:    aws.Int64(int64(s.backups[key].PartNum)),
		UploadId:      backup.MultipartResponse.UploadId,
		ContentLength: aws.Int64(int64(len(buffer))),
	})

	if err != nil {
		s.S3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   backup.MultipartResponse.Bucket,
			Key:      backup.MultipartResponse.Key,
			UploadId: backup.MultipartResponse.UploadId,
		})
		delete(s.backups,key)
	} else{
		CompletedPart := &s3.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int64(int64(backup.PartNum)),
		}
		Offset, _ := strconv.ParseInt(req.Metadata[metadataKeyOffset],10,64)
		backup.CompletedParts = append(backup.CompletedParts, Part{CompletedPart: CompletedPart, Offset: Offset})
		backup.PartNum++
		s.backups[key] = backup
	}

	return nil, err
}

func (s *AWSS3) putblocklist(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	key, _ := req.Metadata[metadataKeyBlobName]
	err := s.initbackup(key)

	if(err != nil){
		return nil, err
	}

	backup := s.backups[key]
	sort.SliceStable(backup.CompletedParts, func(i, j int) bool {
		return backup.CompletedParts[i].Offset < backup.CompletedParts[j].Offset
	})

	var CompletedParts = make([]*s3.CompletedPart,len(backup.CompletedParts))
	for i:=0; i < len(backup.CompletedParts); i++{
		CompletedParts[i] = backup.CompletedParts[i].CompletedPart
	}

	_,err = s.S3.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   backup.MultipartResponse.Bucket,
		Key:      backup.MultipartResponse.Key,
		UploadId: backup.MultipartResponse.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: CompletedParts,
		},
	})

	delete(s.backups,key)
	return nil, err
}

func (s *AWSS3) parseMetadata(metadata bindings.Metadata) (*s3Metadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m s3Metadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AWSS3) getClient(metadata *s3Metadata) (error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return err
	}

	s.S3 = s3.New(sess)
	s.uploader = s3manager.NewUploader(sess)
	s.downloader = s3manager.NewDownloader(sess)

	return nil
}
