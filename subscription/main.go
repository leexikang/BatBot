package main
import (
  "context"
  "encoding/json"
  "log"
  "time"

  "github.com/aws/aws-lambda-go/events"
  _ "github.com/go-sql-driver/mysql"

  "github.com/aws/aws-lambda-go/lambda"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/dynamodb"
  "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Request struct{
  Event string
  TimeStamp int
}

type SubscribeRequest struct{
  Event string
  TimeStamp int
  User User
}

type ConversationStartedRequest struct{
  Event string
  TimeStamp int
  Subscribed bool
  User User
}

type User struct{
  Id string
  Name string
  Country string
}

type Subscriber struct{
  Id string
  Name string
  subscribedAt string
}

func main(){
  lambda.Start(Handler)
}

func Handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error){
  var res Request
  err := json.Unmarshal([]byte(request.Body), &res)
  if err != nil {
    log.Print(err)
    return events.APIGatewayProxyResponse{StatusCode: 412}, err
  }
  if(res.Event == "subscribed"){
    var subscribeEvent SubscribeRequest
    err := json.Unmarshal([]byte(request.Body), &subscribeEvent)
    if err != nil {
      log.Print(err)
      return events.APIGatewayProxyResponse{StatusCode: 412}, err
    }
    errAdding := addSubscriber(&subscribeEvent)
    if errAdding != nil {
      log.Print(errAdding)
      return events.APIGatewayProxyResponse{StatusCode: 412}, err
    }
  }

  //if(res.Event == "conversation_started")/*{*/
    //var conversationStartedEvent ConversationStartedRequest
    //err := json.Unmarshal([]byte(request.Body), &res)
    //if err != nil {
      //log.Print(err)
    //}
    //if (conversationStartedEvent.Subscribed == false){
      //_, errQuery := con.Query("INSERT INTO subscribers (id, name, country) VALUES (?, ?, ?)", conversationStartedEvent.User.Id,
      //conversationStartedEvent.User.Name,
      //conversationStartedEvent.User.Country)
      //if errQuery != nil {
        //log.Print(err)
      //}
    //}
  /*}*/
  return events.APIGatewayProxyResponse{StatusCode: 200}, nil
}


func addSubscriber(subReq *SubscribeRequest) error{
  svc := dynamodbCon()
  subscribed_at := time.Now().Format(time.RFC3339)
  data := map[string]interface{}{ "id":  subReq.User.Id,
  "subscribed_at": subscribed_at,
  "country": subReq.User.Country,}
  av, err := dynamodbattribute.MarshalMap(data)
  if err != nil {
    return err
  }
  input := &dynamodb.PutItemInput{
    Item:      av,
    TableName: aws.String("subscribers"),
  }
  _, insertErr := svc.PutItem(input)
  if insertErr != nil {
    return insertErr
  }
  return nil
}

func dynamodbCon() *dynamodb.DynamoDB{
  conf := aws.Config{}
  sess := session.New(&conf)
  return dynamodb.New(sess)
}
