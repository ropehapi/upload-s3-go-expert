package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
	"io"
	"log"
	"os"
	"sync"
)

var (
	s3Client *s3.S3
	s3Bucket string
	wg       sync.WaitGroup
)

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String(os.Getenv("AWS_REGION")),
			Credentials: credentials.NewStaticCredentials(
				os.Getenv("AWS_ACCESS_KEY_ID"),
				os.Getenv("AWS_SECRET_ACCESS_KEY"),
				"",
			),
		},
	)
	if err != nil {
		panic(err)
	}
	s3Client = s3.New(sess)
	s3Bucket = "go-expert-bucket-test"
}

func main() {
	dir, err := os.Open("./tmp")
	if err != nil {
		panic(err)
	}
	defer dir.Close()
	uploadControl := make(chan struct{}, 100)
	errorFileUpload := make(chan string, 10)

	go func() {
		for {
			select {
			case filename := <-errorFileUpload:
				uploadControl <- struct{}{}
				wg.Add(1)
				go uploadFile(filename, uploadControl, errorFileUpload)
			}
		}
	}()

	for {
		files, err := dir.ReadDir(1)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading directory: %s\n", err)
			continue
		}
		wg.Add(1)
		uploadControl <- struct{}{}
		go uploadFile(files[0].Name(), uploadControl, errorFileUpload)
	}
	wg.Wait()
}

func uploadFile(filename string, uploadControl <-chan struct{}, errorFileUpload chan<- string) {
	defer wg.Done()
	completeFileName := fmt.Sprintf("./tmp/%s", filename)
	fmt.Printf("Uploading file %s to bucket %s\n", completeFileName, s3Bucket)
	f, err := os.Open(completeFileName)
	if err != nil {
		fmt.Printf("Error opening file %s\n", completeFileName)
		<-uploadControl
		errorFileUpload <- completeFileName
		return
	}
	defer f.Close()
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(filename),
		Body:   f,
	})
	if err != nil {
		fmt.Printf("Error uploading file %s\n", completeFileName)
		<-uploadControl
		errorFileUpload <- completeFileName
		return
	}
	fmt.Printf("File %s uploaded successfully\n", completeFileName)
	<-uploadControl
}
