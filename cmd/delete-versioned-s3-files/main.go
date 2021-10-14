package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

func main() {
	awsRegion := flag.String("region", endpoints.ApSoutheast2RegionID, "AWS Region of the bucket")
	bucketNamePtr := flag.String("bucket", "", "S3 Bucket name")
	keyPrefixPtr := flag.String("prefix", "", "Delete objects that start with this value")
	dryRunPtr := flag.Bool("dryrun", false, "Display items to delete without actually deleting them")

	flag.Parse()

	dryRun := *dryRunPtr
	bucketName := *bucketNamePtr
	if len(bucketName) == 0 {
		log.Fatal("Bucket name required")
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Region: awsRegion,
	}))

	s3Svc := s3.New(sess)

	if dryRun {
		fmt.Println("Performing a Dry Run for now...")
		time.Sleep(2 * time.Second)
	}

	if err := execute(s3Svc, bucketName, *keyPrefixPtr, dryRun); err != nil {
		log.Fatal(err)
	}
}

func execute(s3Svc s3iface.S3API, bucketName, keyPrefix string, dryRun bool) error {
	params := &s3.ListObjectVersionsInput{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int64(1000),
	}
	if len(keyPrefix) > 0 {
		params.Prefix = aws.String(keyPrefix)
	}

	err := s3Svc.ListObjectVersionsPages(params,
		func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
			for _, item := range page.Versions {
				key := *item.Key
				versionID := *item.VersionId
				fmt.Println(deleteVersionedItem(s3Svc, bucketName, key, versionID, dryRun))
			}

			for _, item := range page.DeleteMarkers {
				key := *item.Key
				versionID := *item.VersionId
				fmt.Println(deleteVersionedItem(s3Svc, bucketName, key, versionID, dryRun))
			}

			return !lastPage
		})

	return err
}

func deleteVersionedItem(s3Svc s3iface.S3API, bucket, key, versionID string, dryRun bool) string {
	if dryRun {
		return fmt.Sprintf("Dry run delete: %s, %s", key, versionID)
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: []*s3.ObjectIdentifier{
				{
					Key:       aws.String(key),
					VersionId: aws.String(versionID),
				},
			},
			Quiet: aws.Bool(false),
		},
	}

	_, err := s3Svc.DeleteObjects(input)
	if err != nil {
		return fmt.Sprintf("Unable to delete: %s, %s, err: %v", key, versionID, err)
	}

	return fmt.Sprintf("Deleted: %s, %s", key, versionID)
}
