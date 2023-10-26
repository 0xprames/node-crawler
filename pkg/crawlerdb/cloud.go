package crawlerdb

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type CloudDB struct {
	dynamoClient *dynamodb.DynamoDB
}

func (db *CloudDB) insertPeer(peer PeerData) {
	av, err := dynamodbattribute.MarshalMap(peer)
	if err != nil {
		log.Fatalf("Got error marshalling new peer item: %s", err)
	}

	// Create item in table Movies
	tableName := "geth-peerdata"

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	_, err = db.dynamoClient.PutItem(input)
	if err != nil {
		log.Fatalf("Got error calling PutItem: %s", err)
	}

}

func NewDynamoPeerDataClient() *CloudDB {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)
	return &CloudDB{
		dynamoClient: svc,
	}
}
