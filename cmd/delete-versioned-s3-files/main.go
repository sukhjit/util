package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sukhjit/util/pkg/ptr"
)

var (
	s3Svc      *s3.Client
	bucketName string
	dryRun     bool
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	s3Svc = s3.NewFromConfig(cfg)

	bucketNamePtr := flag.String("bucket", "s3-example-bucket", "S3 Bucket name")
	keyPrefixPtr := flag.String("prefix", "", "Delete objects that start with this value")
	dryRunPtr := flag.Bool(
		"dryrun",
		false,
		"Display items to delete without actually deleting them",
	)
	concurrencyPtr := flag.Int("workers", 1, "Number of workers to process jobs")

	flag.Parse()

	concurrency := *concurrencyPtr
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
		if err := execute(bucketName, *keyPrefixPtr, tasks); err != nil {
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

func execute(
	bucketName string,
	keyPrefix string,
	tasks chan<- taskItem,
) error {
	input := &s3.ListObjectVersionsInput{
		Bucket:  ptr.Ptr(bucketName),
		MaxKeys: ptr.Ptr(int32(1000)),
	}
	if len(keyPrefix) > 0 {
		input.Prefix = ptr.Ptr(keyPrefix)
	}

	pgntr := s3.NewListObjectVersionsPaginator(s3Svc, input)

	for pgntr.HasMorePages() {
		page, err := pgntr.NextPage(context.TODO())
		if err != nil {
			return err
		}

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
	}

	return nil
}

func worker(tasks <-chan taskItem, results chan<- string) {
	for t := range tasks {
		results <- deleteVersionedItem(s3Svc, bucketName, t.key, t.versionID, dryRun)
	}
}

func deleteVersionedItem(
	s3Svcv2 *s3.Client,
	bucket, key, versionID string,
	dryRun bool,
) string {
	if dryRun {
		return fmt.Sprintf("Dry run delete: %s, %s", key, versionID)
	}

	input := &s3.DeleteObjectsInput{
		Bucket: ptr.Ptr(bucket),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{
					Key:       ptr.Ptr(key),
					VersionId: ptr.Ptr(versionID),
				},
			},
			Quiet: ptr.Ptr(false),
		},
	}

	if _, err := s3Svcv2.DeleteObjects(context.Background(), input); err != nil {
		return fmt.Sprintf("Unable to delete: %s, %s, err: %v", key, versionID, err)
	}

	return fmt.Sprintf("Deleted: %s, %s", key, versionID)
}
