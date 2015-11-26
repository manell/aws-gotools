package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
)

const (
	PublicGrantee = "http://acs.amazonaws.com/groups/global/AllUsers"
)

// An Object is a representation of a s3 Object
type Object struct {
	Name   string
	Size   int64
	Public bool
}

// A Bucket is a representation of a S3 bucket
type Bucket struct {
	Name          string
	Objects       []*Object
	PublicObjects []*Object
	Size          int64
}

func BuildBucket(ses *session.Session, bucketName *string, region string) (*Bucket, error) {
	// We must create a new sessions for each different region
	svc := s3.New(ses, aws.NewConfig().WithRegion(region))

	// Get all objects inside the bucket
	loParams := &s3.ListObjectsInput{
		Bucket: bucketName,
	}

	// 1000 objects
	loResp, err := svc.ListObjects(loParams)
	if err != nil {
		return nil, err
	}

	bucket := &Bucket{
		Name: *bucketName,
		Size: 0,
	}

	if len(loResp.Contents) == 0 {
		return bucket, nil
	}

	// Channel for returning object values
	objectList := make(chan *Object, len(loResp.Contents))

	// Channel for handling errors
	errChan := make(chan error, 1)

	for _, object := range loResp.Contents {
		go func(o *s3.Object) {
			goaParams := &s3.GetObjectAclInput{
				Bucket: bucketName,
				Key:    o.Key,
			}
			aclResp, err := svc.GetObjectAcl(goaParams)
			if err != nil {
				errChan <- err
			}

			file := &Object{
				Name: *o.Key,
				Size: *o.Size,
			}

			// Check whether any file has public access
			for _, grant := range aclResp.Grants {
				uri := grant.Grantee.URI
				if (uri != nil) && (*uri == PublicGrantee) {
					file.Public = true
				}
			}

			objectList <- file
		}(object)
	}

	// Check for errors
	if len(errChan) > 0 {
		return nil, <- errChan
	}

	// Wait for all channels
	for i := 0; i < len(loResp.Contents); i = i + 1 {
		obj := <-objectList
		bucket.Size += obj.Size
		bucket.Objects = append(bucket.Objects, obj)
		if obj.Public {
			bucket.PublicObjects = append(bucket.PublicObjects, obj)
		}
	}

	return bucket, nil
}

func MapBucketByLocation(svc *s3.S3, buckets []*s3.Bucket) (map[string][]*string, error) {
	result := make(map[string][]*string)

	for _, b := range buckets {
		params := &s3.GetBucketLocationInput{
			Bucket: b.Name,
		}
		resp, err := svc.GetBucketLocation(params)
		if err != nil {
			return nil, err
		}

		region := "us-east-1" //Use a map!
		if resp.LocationConstraint != nil {
			region = *resp.LocationConstraint
			if region == "EU" {
				region = "eu-west-1"
			}
		}
		result[region] = append(result[region], b.Name)
	}

	return result, nil
}

func main() {
	awsRegion := "eu-west-1"

	ses := session.New(&aws.Config{Region: aws.String(awsRegion)})
	svc := s3.New(ses)

	res, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	buckets, err := MapBucketByLocation(svc, res.Buckets)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var totalSize int64
	totalObjects := 0
	totalPublicObjects := 0
	for region := range buckets {
		for _, b := range buckets[region] {
			bucket, err := BuildBucket(ses, b, region)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Println(fmt.Sprintf("Bucket name: %s", bucket.Name))
			fmt.Println(fmt.Sprintf("Bucket region: %s", region))
			fmt.Println(fmt.Sprintf("Bucket size: %d", bucket.Size))
			fmt.Println(fmt.Sprintf("Total objects: %d", len(bucket.Objects)))
			fmt.Println(fmt.Sprintf("Public objects: %d", len(bucket.PublicObjects)))
			for _, obj := range bucket.PublicObjects {
				fmt.Println(fmt.Sprintf("  %s", obj.Name))
			}
			fmt.Println()
			
			totalSize += bucket.Size
			totalObjects += len(bucket.Objects)
			totalPublicObjects += len(bucket.PublicObjects)
		}
	}
	
	fmt.Println(fmt.Sprintf("Total buckets size: %d", totalSize))
	fmt.Println(fmt.Sprintf("Total objects: %d", totalObjects))
	fmt.Println(fmt.Sprintf("Total public objects: %d", totalPublicObjects))
}
