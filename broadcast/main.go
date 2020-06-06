package main
import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)
type Payload struct{
  Id string
  Subscribed_At string
  Content string
}

type Request struct{
  Content string
}

func main(){
  lambda.Start(Handler)
}

func produce(content string) error{
  conf := aws.Config{}
  sess := session.New(&conf)
  svc := sqs.New(sess)
  qURL := os.Getenv("SQS_URL")
  page := Payload{
    Id: "",
    Subscribed_At: "",
    Content: content,
  }
  log.Print(page)
  body, err := json.Marshal(page)
  if err != nil {
    log.Print(err)
    return err
  }
  _, sendErr := svc.SendMessage(&sqs.SendMessageInput{
    DelaySeconds: aws.Int64(1),
    MessageBody: aws.String(string(body)),
    QueueUrl:&qURL,
  })
  if sendErr != nil{
    log.Print(err)
    return sendErr
  }
  return nil
}

func Handler(ctx context.Context, request events.APIGatewayProxyRequest)(events.APIGatewayProxyResponse, error){
  var message Request
  err := json.Unmarshal([]byte(request.Body), &message)
  if err != nil {
    log.Print(err)
    return events.APIGatewayProxyResponse{StatusCode: 412}, err
  }
  produceErr := produce(message.Content)
  if produceErr != nil{
  return events.APIGatewayProxyResponse{StatusCode: 412}, produceErr
  }
  return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}

