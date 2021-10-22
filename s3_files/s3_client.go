package s3_files

import (
	"bytes"
	"errors"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// ErrInvalidNumOfUploadFiles is returned when the number of upload files that should be concatenate
// is greater than 10000
var ErrInvalidNumOfUploadFiles = errors.New("number of upload files should not be greater than 10000")

type S3Client struct {
	S3Client   *s3.S3
	BucketName string
}

func NewS3Client(user, password, region, bucketName string) *S3Client {
	sess := session.Must(
		session.NewSession(
			&aws.Config{
				Credentials: credentials.NewStaticCredentials(
					user,
					password,
					"",
				),
				CredentialsChainVerboseErrors: aws.Bool(true),
			},
		),
	)

	s3Client := s3.New(
		sess,
		&aws.Config{
			Endpoint:         aws.String("http://localhost:9000"),
			Region:           aws.String(region),
			S3ForcePathStyle: aws.Bool(true),
		},
	)

	return &S3Client{
		S3Client:   s3Client,
		BucketName: bucketName,
	}
}

// CreateBucket creates a new S3 bucket
func (s3Client *S3Client) CreateBucket() error {
	_, err := s3Client.S3Client.CreateBucket(
		&s3.CreateBucketInput{
			Bucket: aws.String(s3Client.BucketName),
		},
	)

	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Error() == s3.ErrCodeBucketAlreadyExists && awsErr.Error() == s3.ErrCodeBucketAlreadyOwnedByYou {
			return nil
		}

		return err
	}

	return nil
}

// UploadFile uploads a file to S3
func (s3Client *S3Client) UploadFile(filePath string, content []byte) error {
	uploader := s3manager.NewUploaderWithClient(s3Client.S3Client)

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3Client.BucketName),
		Key:    aws.String(filePath),
		Body:   bytes.NewReader(content),
	})

	return err
}

// DeleteFile deletes the file from S3
func (s3Client *S3Client) DeleteFile(filePath string) error {
	_, err := s3Client.S3Client.DeleteObject(
		&s3.DeleteObjectInput{
			Bucket: aws.String(s3Client.BucketName),
			Key:    aws.String(filePath),
		},
	)

	return err
}

// DeleteFiles deletes files from S3
func (s3Client *S3Client) DeleteFiles(filePaths []string) error {
	for _, filePath := range filePaths {
		err := s3Client.DeleteFile(filePath)
		if err != nil {
			return err
		}
	}

	return nil
}

// ConcatenateFiles concatenate multiple files with a given file paths
// under the specified file path, will merge all the files into one file in the given order
// and delete them
func (s3Client *S3Client) ConcatenateFiles(
	targetFilePath string,
	filePaths []string,
) error {
	// We let the service know that we want to do a multipart upload
	output, err := s3Client.S3Client.CreateMultipartUpload(
		&s3.CreateMultipartUploadInput{
			Bucket: aws.String(s3Client.BucketName),
			Key:    aws.String(targetFilePath),
			ACL:    aws.String("public-read"),
		},
	)
	if err != nil {
		return err
	}

	parts, err := s3Client.prepareConcatenationParts(targetFilePath, filePaths, output.UploadId)
	if err != nil {
		return err
	}

	// We finally complete the multipart upload.
	_, err = s3Client.S3Client.CompleteMultipartUpload(
		&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(s3Client.BucketName),
			Key:      aws.String(targetFilePath),
			UploadId: output.UploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: parts,
			},
		},
	)

	// delete chunk files
	return s3Client.DeleteFiles(filePaths)
}

// prepareConcatenationParts will contenate all keys under the specified objKey
func (s3Client *S3Client) prepareConcatenationParts(
	targetFilePath string,
	filePaths []string,
	uploadID *string,
) ([]*s3.CompletedPart, error) {
	var tagObjects []*s3.CompletedPart

	// PartNumber in UploadPartCopyInput should be a positive integer between 1 and 10,000.
	// In our case the value of the PartNumber is the index value of the element in the array,
	// therefore we should check if the number of upload files is greater than 10000
	if len(filePaths) > 10000 {
		return nil, ErrInvalidNumOfUploadFiles
	}

	for i, path := range filePaths {
		partNum := i + 1

		uploadPartCopyInput := &s3.UploadPartCopyInput{
			Bucket:     aws.String(s3Client.BucketName),
			CopySource: aws.String(url.QueryEscape(s3Client.BucketName + "/" + path)),
			PartNumber: aws.Int64(int64(partNum)),
			Key:        aws.String(targetFilePath),
			UploadId:   uploadID,
		}

		copyOutput, err := s3Client.S3Client.UploadPartCopy(uploadPartCopyInput)
		if err != nil {
			return tagObjects, err
		}

		tagObjects = append(
			tagObjects,
			&s3.CompletedPart{
				ETag:       copyOutput.CopyPartResult.ETag,
				PartNumber: uploadPartCopyInput.PartNumber,
			},
		)

	}

	return tagObjects, nil
}
