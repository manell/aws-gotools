package main

import(
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"fmt"
)

// An Object is a representation of a s3 Object
type Object struct {
	Name string
	Size int64
}

// A Bucket is a representation of a S3 bucket
type Bucket struct {
	Name string
	Objects []*Object
}

func BuildBucket(ses *session.Session, bucketName *string, region string) (*Bucket, error) {	
	// We must create a new sessions for each different region
	svc := s3.New(ses, aws.NewConfig().WithRegion(region))
	
	// Get all objects inside the bucket
	loParams := &s3.ListObjectsInput{
		Bucket:	bucketName,
	}

	// 1000 objects 
	loResp, err := svc.ListObjects(loParams)
	if err != nil {
		return nil, err
	}

	bucket := &Bucket{
		Name: *bucketName,
	}
	
	if len(loResp.Contents) == 0 {
		return bucket, nil
	}
	
	// Channel for returning object values
	objectList := make(chan *Object, len(loResp.Contents))
	
	// Channel for handling erros
	errChan := make(chan error, 1)
	
	for _, object := range loResp.Contents {
		go func(o *s3.Object){
			goaParams := &s3.GetObjectAclInput{
				Bucket:       bucketName,
				Key:          o.Key,
			}
			aclResp, err := svc.GetObjectAcl(goaParams)
			if err != nil {
				errChan <- err
			}
			
			file := &Object{
				Name: *o.Key,
				Size: *o.Size,
			}
			objectList <- file
			errChan <- nil
	
			_ = aclResp
		}(object)
	}
	
	if err :=  <- errChan; err != nil {
		return nil, err
	}
	
	for i := 0; i < len(loResp.Contents); i = i + 1 {
		fmt.Println(i)
		bucket.Objects = append(bucket.Objects, <- objectList)
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

	for region := range buckets {
		fmt.Println(region)
		for _, b := range buckets[region] {
			fmt.Println(*b)
			_, err := BuildBucket(ses, b, region);
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}
	}
}
