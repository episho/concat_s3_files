package main

import (
	"bytes"
	"fmt"

	"elena/concat_s3_files/s3_files"

	"github.com/spf13/cobra"
)

func main() {
	var user, password, region, bucketName string

	rootCmd := &cobra.Command{
		Use: "concat",
		RunE: func(cmd *cobra.Command, args []string) error {
			s3Client := s3_files.NewS3Client(user, password, region, bucketName)
			err := s3Client.CreateBucket()
			if err != nil {
				return fmt.Errorf("failed to create bucket err: %v", err)
			}

			// create file with 5MB because the minimal multipart upload size is 5Mb
			// except the size of the last file
			bigBuff := bytes.Repeat([]byte("A"), 5*1024*1024)
			err = s3Client.UploadFile("testFile-1", bigBuff)
			if err != nil {
				return fmt.Errorf("failed to updoad file %s", "testFile-1")
			}

			// upload the second file that we want to concatenate
			err = s3Client.UploadFile("testFile-2", []byte("B"))
			if err != nil {
				return fmt.Errorf("failed to write file %s", "testFile-2")
			}

			// concatenate the two files
			err = s3Client.ConcatenateFiles("test", []string{"testFile-1", "testFile-2"})
			if err != nil {
				return fmt.Errorf("failed to concatenate files err: %v", err)
			}

			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(
		&user,
		"user",
		"user",
		"MinIO root user",
	)

	rootCmd.PersistentFlags().StringVar(
		&password,
		"password",
		"password",
		"MinIO root password",
	)

	rootCmd.PersistentFlags().StringVar(
		&bucketName,
		"bucketName",
		"test",
		"S3 bucket name",
	)

	rootCmd.PersistentFlags().StringVar(
		&region,
		"region",
		"us-east-1",
		"AWS region",
	)

	rootCmd.Execute()
}
