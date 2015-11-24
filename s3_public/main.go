package main

import(
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"fmt"
)

func ListBucketFiles(svc *s3.S3, bucket *string, region string) (error) {
	params := &s3.ListObjectsInput{
		Bucket:	bucket,
	}
	
	resp, err := svc.ListObjects(params)
	if err != nil {
		return err
	}
	
	fmt.Println(resp.Marker)
	
	return nil
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
		
		region := "US"
		if resp.LocationConstraint != nil {
			region = *resp.LocationConstraint
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
	
	for k := range buckets {
		fmt.Println(k)
	}
	
	/*for i, b := range res.Buckets {
		fmt.Println(i)
		
		if err := ListBucketFiles(svc, b.Name, awsRegion); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}*/
}