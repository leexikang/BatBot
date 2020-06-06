package main
import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	//"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/sqs"
	_ "github.com/go-sql-driver/mysql"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)


type Request  struct{
  //Sender string `json:send"sender"`
  MinApiVersion int `json:"min_api_version"`
  BroadcastList []string `json:"broadcast_list"`
  Type string `json:"type"`
  Text string `json:"text"`
}

type Payload struct{
  Id string `json:"id"`
  Subscribed_At string `json:"subscribed_at"`
  Content string
}

type Message struct{
  Body string
}

type SQSPayload struct {
  Id string `json:"id"`
  Subscribed_At string `json:"subscribed_at"`
}
type Item struct{
  Id string
  Subscribed_at string
}
var content string

func main(){
  lambda.Start(Handler)
}

func Handler(ctx context.Context, sqsEvent events.SQSEvent){
  message := sqsEvent.Records[0]
  if len(sqsEvent.Records) == 0 {
    log.Println("No Messages!")
  }
  nextPayload, err := receiver(message)
  if err != nil {
    log.Print(err)
  }
  log.Print("nextPayload:", nextPayload)
  if nextPayload.Id != "" && nextPayload.Subscribed_At != ""{
    payload := Payload{
      Id: nextPayload.Id,
      Subscribed_At: nextPayload.Subscribed_At,
      Content: content,
    }
    producer(&payload)
  }
}


func receiver(message events.SQSMessage) (*SQSPayload, error){
  var body Payload;
  err := json.Unmarshal([]byte(message.Body), &body)
  if err != nil {
    log.Println("Error", err)
    return nil, err
  }
  content = body.Content
  var startKey map[string]*dynamodb.AttributeValue
  if(body.Id != "" && body.Subscribed_At != ""){
    sqsPayLoad := SQSPayload{
      Id: body.Id,
      Subscribed_At: body.Subscribed_At,
    }
    startKey, err = dynamodbattribute.MarshalMap(sqsPayLoad)
    if err != nil {
      return nil, err
    }
  }
  log.Print("Start Key", startKey)
  inputs := &dynamodb.ScanInput{
    TableName: aws.String("subscribers"),
    Limit: aws.Int64(1),
    ExclusiveStartKey: startKey,
  }
  pageNum := 0
  var endKey map[string]*dynamodb.AttributeValue
  subList := []string{}
  conf := aws.Config{}
  sess := session.New(&conf)
  svc := dynamodb.New(sess)
  err = svc.ScanPages(inputs, func(p *dynamodb.ScanOutput, last bool) bool {
    pageNum ++
    log.Print(p.Items)
    var subscriber struct{
      Id string
    }
    for _, item := range p.Items{
      dynamodbattribute.UnmarshalMap(item, &subscriber)
      subList = append(subList, subscriber.Id)
    }
    endKey= p.LastEvaluatedKey
    return pageNum <= 1
  })
  if err != nil{
    return nil, err
  }

  success := broadcastMessage(subList, body.Content)
  if len(subList) > 0 {
    if success {
      sqsCon  := getSQSClient()
      qURL := os.Getenv("SQS_URL")
      _, err := sqsCon.DeleteMessage(&sqs.DeleteMessageInput{
        QueueUrl:      &qURL,
        ReceiptHandle: &message.ReceiptHandle,
      })
      if err != nil {
        return nil, err
      }
    }
  }
  var sqsPayLoad SQSPayload
  _ = dynamodbattribute.UnmarshalMap(endKey, &sqsPayLoad)
  return &sqsPayLoad, nil
}

func producer(payload *Payload){
  sqsCon := getSQSClient()
  qURL := os.Getenv("SQS_URL")
  body, err := json.Marshal(payload)
  if err != nil {
    log.Print(err)
    return
  }
  result, err := sqsCon.SendMessage(&sqs.SendMessageInput{
    DelaySeconds: aws.Int64(1),
    MessageBody: aws.String(string(body)),
    QueueUrl: &qURL,
  })
  if err != nil{
    log.Print(err)
  }
  log.Print(result)
}

func getSQSClient() *sqs.SQS {
  conf := aws.Config{}
  sess := session.New(&conf)
  svc := sqs.New(sess)
  return svc
}

func broadcastMessage(subscribers []string, content string) bool{
  request := Request{
    MinApiVersion: 1,
    BroadcastList: subscribers,
    Type: "text",
    Text: content,
  }
  body, parsedErr := json.Marshal(request)
  if parsedErr != nil {
    log.Print(parsedErr)
  }
  log.Print(string(body))
  req, err := http.NewRequest("POST", "https://chatapi.viber.com/pa/broadcast_message", bytes.NewBuffer(body))
  if err != nil {
    log.Print("post error")
    log.Print(err)
    return false
  }
  req.Header.Set("Context-Type", "application/json")
  req.Header.Set("X-Viber-Auth-Token", os.Getenv("VIBER_TOKEN"))
  client := &http.Client{}
  log.Print("POST")
  _, postErr := client.Do(req)
  if postErr != nil {
    log.Print(postErr)
    return false
  }
  return true
}

func getMessage() sqs.Message{
  svc := getSQSClient()
  qURL := os.Getenv("SQS_URL")

  result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
    QueueUrl:            &qURL,
    MaxNumberOfMessages: aws.Int64(1),
    VisibilityTimeout:   aws.Int64(20),  // 20 seconds
    WaitTimeSeconds:     aws.Int64(0),
    })
    if err != nil{
      log.Print(err)
    }
    return *result.Messages[0]
}

