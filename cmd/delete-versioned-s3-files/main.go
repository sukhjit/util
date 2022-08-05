package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	s3Svc      *s3.S3
	bucketName string
	dryRun     bool
)

// func createTask()

func main() {
	var sess *session.Session

	awsRegion := flag.String("region", endpoints.ApSoutheast2RegionID, "AWS Region of the bucket")
	bucketNamePtr := flag.String("bucket", "od-stg-ds-gracenote-data-backup", "S3 Bucket name")
	keyPrefixPtr := flag.String("prefix", "", "Delete objects that start with this value")
	dryRunPtr := flag.Bool("dryrun", false, "Display items to delete without actually deleting them")
	concurrencyPtr := flag.Int("workers", 1, "Number of workers to process jobs")

	flag.Parse()

	if os.Getenv("AWS_PROFILE") != "" {
		sess = session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Profile:           os.Getenv("AWS_PROFILE"),
		}))
	} else {
		sess = session.Must(session.NewSession(&aws.Config{
			Region: awsRegion,
		}))
	}

	concurrency := *concurrencyPtr
	s3Svc = s3.New(sess)
	bucketName = *bucketNamePtr
	if bucketName == "" {
		log.Fatal("Bucket name required")
	}

	dryRun = *dryRunPtr
	if dryRun {
		fmt.Println("Performing a Dry Run for now...")
		time.Sleep(2 * time.Second)
	}

	tasks := make(chan taskItem)
	results := make(chan string)

	go func() {
		if err := execute(s3Svc, bucketName, *keyPrefixPtr, dryRun, tasks); err != nil {
			log.Fatal(err)
		}
		close(tasks)
	}()

	var wg sync.WaitGroup
	wg.Add(concurrency)
	go func() {
		wg.Wait()
		close(results)
	}()

	for w := 1; w <= concurrency; w++ {
		log.Println("starting worker:", w)

		go func() {
			defer wg.Done()
			worker(tasks, results)
		}()
	}
	log.Println("all workers started")

	for r := range results {
		fmt.Println(r)
	}
}

type taskItem struct {
	key       string
	versionID string
}

func execute(s3Svc s3iface.S3API, bucketName, keyPrefix string, dryRun bool, tasks chan<- taskItem) error {
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
				item := taskItem{
					key:       *item.Key,
					versionID: *item.VersionId,
				}

				tasks <- item
			}

			for _, item := range page.DeleteMarkers {
				item := taskItem{
					key:       *item.Key,
					versionID: *item.VersionId,
				}

				tasks <- item
			}

			return !lastPage
		})

	return err
}

func worker(tasks <-chan taskItem, results chan<- string) {
	for t := range tasks {
		results <- deleteVersionedItem(s3Svc, bucketName, t.key, t.versionID, dryRun)
	}
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
