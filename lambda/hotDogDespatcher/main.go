package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
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

type DogGroup struct {
	Dogs []Dog `json:dogs`
}

func decodeDog(data []byte) Dog {
	var dog Dog
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&dog); err != nil {
		log.Fatal("decoding error!")
	}
	return dog
}

func encodeDog(dog Dog) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(dog); err != nil {
		return nil, errors.New("encoding error! ")
	}
	return buffer.Bytes(), nil
}

func Handler(ctx context.Context, event events.KinesisEvent) error {
	fmt.Printf("%d number of hot dogs are being eaten by people now!\n", len(event.Records))

	var dogGroup DogGroup
	for _, record := range event.Records {
		dataBytes := record.Kinesis.Data
		dog := decodeDog(dataBytes)
		dogGroup.Dogs = append(dogGroup.Dogs, dog)
		fmt.Printf("%s Despatcher Received Data = %#v \n", record.EventName, dog)
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
		if strings.Contains(strings.ToLower(dog.Name), "e") || strings.Contains(strings.ToLower(dog.Name), "a") {
			fmt.Printf("warning: hot dog %s hasn't been eaten by anyone \n", dog.Name)
			continue
		}

		dogInBytes, err := encodeDog(dog)
		if err != nil {
			log.Fatal("encoding error!")
		}

		_, err = pubsub.PutRecord(&kinesis.PutRecordInput{
			Data:         dogInBytes,
			StreamName:   aws.String("eatenHotDogs"),
			PartitionKey: aws.String("despatcher-partition-key"),
		})
		if err != nil {
			fmt.Printf("warning: kinesis put item error! %s\n", err)
			continue
		}
	}

	return nil
}

func main() {
	lambda.Start(Handler)
}
