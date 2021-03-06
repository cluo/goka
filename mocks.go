package goka

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

type clientMock struct {
	topics        []string
	partitions    []int32
	newestOffsets map[string]int64
	oldestOffsets map[string]int64
}

func newClientMock() *clientMock {
	return &clientMock{
		newestOffsets: make(map[string]int64),
		oldestOffsets: make(map[string]int64),
	}
}

// newest offset is the highwatermark
func (cm *clientMock) setNewestOffset(topic string, partitionID int32, offset int64) {
	cm.newestOffsets[fmt.Sprintf("%s/%d", topic, partitionID)] = offset
}

func (cm *clientMock) setOldestOffset(topic string, partitionID int32, offset int64) {
	cm.oldestOffsets[fmt.Sprintf("%s/%d", topic, partitionID)] = offset
}

func (cm *clientMock) Config() *sarama.Config {
	return nil
}

func (cm *clientMock) Topics() ([]string, error) {
	return cm.topics, nil
}

func (cm *clientMock) Partitions(topic string) ([]int32, error) {
	return cm.partitions, nil
}

func (cm *clientMock) WritablePartitions(topic string) ([]int32, error) {
	return cm.partitions, nil
}

func (cm *clientMock) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	return nil, nil
}

func (cm *clientMock) Replicas(topic string, partitionID int32) ([]int32, error) {
	return nil, nil
}

func (cm *clientMock) RefreshMetadata(topics ...string) error {
	return nil
}

func (cm *clientMock) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	var offset int64
	var hasOffset bool
	switch time {
	case sarama.OffsetNewest:
		offset, hasOffset = cm.newestOffsets[fmt.Sprintf("%s/%d", topic, partitionID)]
	case sarama.OffsetOldest:
		offset, hasOffset = cm.oldestOffsets[fmt.Sprintf("%s/%d", topic, partitionID)]
	default:
		log.Panic("we don't mock this case")
	}

	if hasOffset {
		return offset, nil
	}
	if time == sarama.OffsetNewest {
		return 0, nil
	}

	return 0, nil
}

func (cm *clientMock) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	return nil, nil
}

func (cm *clientMock) RefreshCoordinator(consumerGroup string) error {
	return nil
}

func (cm *clientMock) Close() error {
	return nil
}

func (cm *clientMock) Closed() bool {
	return false
}
