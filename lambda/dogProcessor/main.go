package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
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
	fmt.Printf("%d number of dogs are being processed by dog processor now!\n", len(event.Records))

	var dogGroup DogGroup
	for _, record := range event.Records {
		dataBytes := record.Kinesis.Data
		dog := decodeDog(dataBytes)
		dogGroup.Dogs = append(dogGroup.Dogs, dog)
		fmt.Printf("%s Processor Data = %#v \n", record.EventName, dog)
	}

	hostname := os.Getenv("LOCALSTACK_HOSTNAME")
	if hostname == "" {
		log.Fatal("empty host name")
	}
	fmt.Println("HOSTNAME: ", hostname)

	sess, err := session.NewSession(&aws.Config{
		Region:                        aws.String("ap-southeast-2"),
		CredentialsChainVerboseErrors: aws.Bool(true),
		Credentials:                   credentials.NewStaticCredentials("fake", "fake", ""),
		Endpoint:                      aws.String(fmt.Sprintf("http://%s:4566", hostname)),
	})
	if err != nil {
		log.Fatalf("session error! %s\n", err)
	}

	db := dynamodb.New(sess)

	for _, dog := range dogGroup.Dogs {
		if dog.IsAlive {
			// alive dogs get streamed from dog catcher
			dog.IsAlive = false
			av, err := dynamodbattribute.MarshalMap(dog)
			if err != nil {
				fmt.Printf("warning: dynamodb marshalling error! %s\n", err)
				continue
			}

			_, err = db.PutItem(&dynamodb.PutItemInput{
				Item:      av,
				TableName: aws.String("dogs"),
			})
			if err != nil {
				fmt.Printf("warning: dynamodb put item error! %s\n", err)
				continue
			}

			pubsub := kinesis.New(sess)

			dogInBytes, err := encodeDog(dog)
			if err != nil {
				log.Fatal("encoding error!")
			}

			_, err = pubsub.PutRecord(&kinesis.PutRecordInput{
				Data:         dogInBytes,
				StreamName:   aws.String("hotDogs"),
				PartitionKey: aws.String("processor-partition-key"),
			})
			if err != nil {
				fmt.Printf("warning: kinesis put item error! %s\n", err)
				continue
			}

		} else {
			//dead dogs get streamed from hot dog despatcher
			var deadDog *Dog
			result, err := db.GetItem(&dynamodb.GetItemInput{
				Key: map[string]*dynamodb.AttributeValue{
					"ID": {
						S: aws.String(dog.ID),
					},
				},
				TableName: aws.String(("dogs")),
			})
			if err != nil {
				fmt.Printf("warning: dynamodb get item error! %s\n", err)
				continue
			}

			err = dynamodbattribute.UnmarshalMap(result.Item, deadDog)
			if err != nil {
				fmt.Printf("warning: dynamodb unmarshalling error! %s\n", err)
				continue
			}
			if deadDog == nil {
				fmt.Printf("warning there is no dog")
				continue
			}

			if !deadDog.IsAlive {
				deadDog.IsEaten = true
				av, err := dynamodbattribute.MarshalMap(deadDog)
				if err != nil {
					fmt.Printf("warning: dynamodb marshalling error! %s\n", err)
					continue
				}

				_, err = db.PutItem(&dynamodb.PutItemInput{
					Item:      av,
					TableName: aws.String("dogs"),
				})
				if err != nil {
					fmt.Printf("warning: dynamodb put item error! %s\n", err)
					continue
				}
			}
		}
	}

	return nil
}

func main() {
	lambda.Start(Handler)
}
