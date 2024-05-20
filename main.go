package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func createStream(ctx context.Context, kc *kinesis.Client, streamName string) string {
	_, err := kc.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: &streamName,
		StreamModeDetails: &types.StreamModeDetails{
			StreamMode: types.StreamModeProvisioned,
		},
		ShardCount: aws.Int32(1),
	})

	var exists *types.ResourceInUseException
	if err != nil && !errors.As(err, &exists) {
		panic(err)
	}

	for {
		s, err := kc.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
			StreamName: &streamName,
		})
		if err != nil {
			panic(err)
		}
		if s.StreamDescriptionSummary.StreamStatus != types.StreamStatusCreating {
			return *s.StreamDescriptionSummary.StreamARN
		}
	}
}

func deleteStream(ctx context.Context, kc *kinesis.Client, streamName, streamARN string) {
	_, err := kc.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: &streamName,
		StreamARN:  &streamARN,
	})
	if err != nil {
		panic(err)
	}
	for {
		_, err := kc.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
			StreamName: &streamName,
		})
		if err != nil {
			var rnf *types.ResourceNotFoundException
			if errors.As(err, &rnf) {
				break
			}
			panic(err)
		}
	}
}

func putRecords(ctx context.Context, kc *kinesis.Client, streamName, streamARN string) {
	for i := range 10 {
		_, err := kc.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   &streamName,
			StreamARN:    &streamARN,
			PartitionKey: aws.String(fmt.Sprint(i)),
			Data:         []byte(fmt.Sprint(i)),
		})
		if err != nil {
			panic(err)
		}
	}
}

func getRecords(ctx context.Context, kc *kinesis.Client, streamName, streamARN string) {
	shards, err := kc.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: &streamName,
		StreamARN:  &streamARN,
	})
	if err != nil {
		panic(err)
	}

	for _, shard := range shards.Shards {
		readShard(ctx, kc, streamName, streamARN, shard.ShardId)
	}
}

func readShard(ctx context.Context, kc *kinesis.Client, streamName, streamARN string, shardID *string) {
	o, err := kc.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		ShardId:           shardID,
		StreamName:        &streamName,
		StreamARN:         &streamARN,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		panic(err)
	}

	iterator := o.ShardIterator
	for {
		records, err := kc.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iterator,
			StreamARN:     &streamARN,
		})
		if err != nil {
			panic(err)
		}
		for i, r := range records.Records {
			fmt.Println("ITEM: ", i, " - ", *r.PartitionKey, string(r.Data))
		}
		if records.NextShardIterator == nil {
			break
		} else {
			iterator = records.NextShardIterator
		}
		if *records.MillisBehindLatest == 0 {
			break
		}
	}
}

func main() {
	ctx := context.TODO()
	streamName := "persistence-test"
	conf, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	kc := kinesis.NewFromConfig(conf)
	arn := createStream(ctx, kc, streamName)
	fmt.Println("CREATED: ", arn)
	putRecords(ctx, kc, streamName, arn)
	getRecords(ctx, kc, streamName, arn)
	deleteStream(ctx, kc, streamName, arn)
	fmt.Println("DELETED")
	fmt.Println("============")
}
