package database

import (
	"context"
	"log"
	"time"

	"github.com/umangutkarsh/graphql-go/graph/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var connectionString string = `mongodb+srv://umangutkarsh:mongodb123@cluster0.msjhkm4.mongodb.net/db?retryWrites=true&w=majority&appName=Cluster0`

type DB struct {
	client *mongo.Client
}

func Connect() *DB {
	client, err := mongo.NewClient(options.Client().ApplyURI(connectionString))
	if err != nil {
		log.Fatal("Error connecting to db", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal("Cloud not connect client", err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal("Could not ping db", err)
	}

	return &DB{
		client: client,
	}
}

func (db *DB) GetJob(id string) *model.JobListing {
	var jobListing model.JobListing
	jobCollection := db.client.Database("graphql-job-board").Collection("jobs")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	err := jobCollection.FindOne(ctx, filter).Decode(&jobListing)
	if err != nil {
		log.Fatal("Could not fetch a job from db", err)
	}
	return &jobListing
}

func (db *DB) GetJobs() []*model.JobListing {
	var jobListings []*model.JobListing
	jobCollection := db.client.Database("graphql-job-board").Collection("jobs")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cursor, err := jobCollection.Find(ctx, bson.D{})
	if err != nil {
		log.Fatal("Could not fetch all jobs from db", err)
	}
	err = cursor.All(context.TODO(), &jobListings)
	if err != nil {
		log.Fatal("Could not parse cursors", err)
	}
	return jobListings
}

func (db *DB) CreateJobListing(jobInfo model.CreateJobListingInput) *model.JobListing {
	var jobListing model.JobListing
	jobCollection := db.client.Database("graphql-job-board").Collection("jobs")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	inserted, err := jobCollection.InsertOne(ctx, bson.M{
		"title":       jobInfo.Title,
		"description": jobInfo.Description,
		"company":     jobInfo.Company,
		"url":         jobInfo.URL,
	})
	if err != nil {
		log.Fatal("Error inserting into db", err)
	}
	insertedId := inserted.InsertedID.(primitive.ObjectID).Hex()
	jobListing = model.JobListing{
		ID:          insertedId,
		Title:       jobInfo.Title,
		Description: jobInfo.Description,
		Company:     jobInfo.Company,
		URL:         jobInfo.URL,
	}
	return &jobListing
}

func (db *DB) UpdateJobListing(id string, jobInfo model.UpdateJobListingInput) *model.JobListing {
	var jobListing model.JobListing
	jobCollection := db.client.Database("graphql-job-board").Collection("jobs")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	updateJobInfo := bson.M{}
	if jobInfo.Title != "" {
		updateJobInfo["title"] = jobInfo.Title
	}
	if jobInfo.Description != "" {
		updateJobInfo["description"] = jobInfo.Description
	}
	if jobInfo.URL != "" {
		updateJobInfo["url"] = jobInfo.URL
	}

	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	update := bson.M{"$set": updateJobInfo}

	results := jobCollection.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate())
	err := results.Decode(&jobListing)
	if err != nil {
		log.Fatal("could not decode in update", err)
	}

	return &jobListing
}

func (db *DB) DeleteJobListing(id string) *model.DeleteJobResponse {
	var deleteJobRes model.DeleteJobResponse
	jobCollection := db.client.Database("graphql-job-board").Collection("jobs")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	_, err := jobCollection.DeleteOne(ctx, filter)
	if err != nil {
		log.Fatal("Could not delete one", err)
	}
	deleteJobRes = model.DeleteJobResponse{
		DeleteJobID: id,
	}
	return &deleteJobRes
}
