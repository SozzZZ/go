package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type MongoOptionsConfig struct {
	RetryWrites *bool `json:"retryWrites,omitempty"`

	WriteConcern *string `json:"writeConcern,omitempty"`

	ReplicaSet *string `json:"replicaSet,omitempty"`
}

type MongoConfig struct {
	User string `json:"user"`

	Pass string `json:"pass"`

	Host string `json:"host"`

	Port int `json:"port,string"`

	Name string `json:"name"`

	Options *MongoOptionsConfig `json:"options,omitempty"`
}

type PriceAlertHistoryModel struct {
	Date      time.Time
	Price     float64
	ProductId string `bson:"productId"`
}

var mongoClient *mongo.Client = nil
var mongoLock sync.Mutex

var mongoConfig = MongoConfig{
	Host: "127.0.0.1",
	Port: 27017,
	User: "",
	Pass: "",
	Name: "",
}

func MongoDB() *mongo.Database {
	mongoLock.Lock()
	defer mongoLock.Unlock()

	if mongoClient == nil {
		connectMongo()
	}

	return mongoClient.Database(mongoConfig.Name)
}

func connectMongo() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := getConnectionOptions()

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatalf("Failed to connect mongodb: %v\n", err)
		panic(err)
	}

	mongoClient = client
}

func getConnectionOptions() *options.ClientOptions {
	cfg := mongoConfig
	uri := fmt.Sprintf(
		"mongodb://%s:%d/",
		cfg.Host,
		cfg.Port,
	)
	opts := options.Client().ApplyURI(uri)

	opts.SetAuth(options.Credential{
		Username:   cfg.User,
		Password:   cfg.Pass,
		AuthSource: cfg.Name,
	})

	opts.SetDirect(true)

	if cfg.Options != nil {
		if cfg.Options.RetryWrites != nil {
			opts.SetRetryWrites(*cfg.Options.RetryWrites)
		}

		if cfg.Options.WriteConcern != nil {
			switch *cfg.Options.WriteConcern {
			case "majority":
				opts.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
			}
		}

		if cfg.Options.ReplicaSet != nil {
			opts.SetReplicaSet(*cfg.Options.ReplicaSet)
		}
	}

	return opts
}

func main() {
	log.Printf("start pariceAlert log script!")
	// duration, _ := time.ParseDuration("168h")
	duration, _ := time.ParseDuration("24h")
	weekAgo := time.Now().Add(-duration)

	log.Printf("目标: %v 之前的priceAlertHistory数据", weekAgo)

	mongoClient := MongoDB()
	var priceAlertHistories []PriceAlertHistoryModel

	opts := options.Find()
	filterBson := bson.D{{Key: "date", Value: bson.D{{Key: "$gt", Value: weekAgo}}}}
	cursor, mongoErr := mongoClient.Collection("priceAlertHistory").Find(
		context.Background(),
		filterBson,
		opts,
	)
	if mongoErr != nil {
		log.Printf("mongoErr:%v", mongoErr)
	}
	defer cursor.Close(context.Background())

	mongoErr = cursor.All(context.Background(), &priceAlertHistories)
	if mongoErr != nil {
		log.Printf("mongoErr:%v", mongoErr)
	}

	log.Printf("Total:%v条数据", len(priceAlertHistories))

	if len(priceAlertHistories) <= 0 {
		log.Printf("There 0 priceAlertHistory data. ended!")
		return
	}

	writeLogCsv(priceAlertHistories)

	deleteOpts := options.Delete()
	res, mongoErr := mongoClient.Collection("priceAlertHistory").DeleteMany(
		context.Background(),
		filterBson,
		deleteOpts,
	)
	if mongoErr != nil {
		log.Printf("mongoErr:%v", mongoErr)
	}

	log.Printf("deleted %v priceAlertHistory", res.DeletedCount)

	log.Printf("pariceAlert log complete!")
}

func writeLogCsv(priceAlertHistories []PriceAlertHistoryModel) {
	data := "date,productId,price\n"
	for _, priceAlertHistory := range priceAlertHistories {
		data += fmt.Sprintf("%v,%v,%v\n",
			priceAlertHistory.Date.Format("2006-01-02 15:04:05"),
			priceAlertHistory.ProductId,
			fmt.Sprintf("%v", priceAlertHistory.Price),
		)
	}

	title := fmt.Sprintf("priceAlertHistoryLog-%v.csv", time.Now().Format("2006-01-02"))
	f, err := os.Create(title)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.WriteString("\xEF\xBB\xBF")
	w := bufio.NewWriter(f)
	w.Write([]byte(data))
	w.Flush()
}
