package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Kept it dry for demo purposes
type Dog struct {
	ID      string `json:id`
	Name    string `json:name`
	IsAlive bool   `json:is_alive`
	IsEaten bool   `json:is_eaten`
}

type MyRequest struct {
	Quantity int `json:"quantity"`
}

type DogGroup struct {
	Dogs []Dog `json:dogs`
}

type MyResponse struct {
	Message string `json:"message:"`
}

func randStringBytes(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	if n < 6 {
		n = 6
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func encodeDog(dog Dog) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(dog); err != nil {
		return nil, errors.New("encoding error! ")
	}
	return buffer.Bytes(), nil
}

func Handler(ctx context.Context, req MyRequest) (MyResponse, error) {
	fmt.Printf("Quantity of dogs that are caught is %d by dog catcher right now!\n", req.Quantity)

	rand.Seed(time.Now().UnixNano())
	var dogGroup DogGroup
	for i := 0; i < req.Quantity; i++ {
		dogGroup.Dogs = append(dogGroup.Dogs, Dog{
			ID:      strconv.Itoa(rand.Intn(100)+5) + "_" + strconv.Itoa(time.Now().UTC().Local().Nanosecond()),
			Name:    randStringBytes(i),
			IsAlive: true,
			IsEaten: false,
		})
	}

	hostname := os.Getenv("LOCALSTACK_HOSTNAME")
	if hostname == "" {
		log.Fatal("empty host name")
	}
	//fmt.Println("HOSTNAME: ", hostname)

	sess, err := session.NewSession(&aws.Config{
		Region:                        aws.String("ap-southeast-2"),
		CredentialsChainVerboseErrors: aws.Bool(true),
		Credentials:                   credentials.NewStaticCredentials("fake", "fake", ""),
		Endpoint:                      aws.String(fmt.Sprintf("http://%s:4566", hostname)),
	})
	if err != nil {
		log.Fatalf("session error! %s\n", err)
	}

	pubsub := kinesis.New(sess)

	for _, dog := range dogGroup.Dogs {
		dogInBytes, err := encodeDog(dog)
		if err != nil {
			log.Fatal("encoding error!")
		}

		_, err = pubsub.PutRecord(&kinesis.PutRecordInput{
			Data:         dogInBytes,
			StreamName:   aws.String("caughtDogs"),
			PartitionKey: aws.String("catcher-partition-key"),
		})
		if err != nil {
			return MyResponse{Message: fmt.Sprintf("Kinesis put item error! %s", err)}, err
		}
	}

	return MyResponse{Message: fmt.Sprintf("%d dogs are successfully sent", len(dogGroup.Dogs))}, nil
}

func main() {
	lambda.Start(Handler)
}
