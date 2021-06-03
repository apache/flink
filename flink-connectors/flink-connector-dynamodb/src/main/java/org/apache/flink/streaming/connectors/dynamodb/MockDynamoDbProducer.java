package org.apache.flink.streaming.connectors.dynamodb;

import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/** Mock dynamo db producer, used as stub. TODO: remove once we can create instances */
public class MockDynamoDbProducer implements DynamoDbProducer {

    public MockDynamoDbProducer(Listener listener) {}

    @Override
    public void close() throws Exception {}

    @Override
    public long getOutstandingRecordsCount() {
        return 0;
    }

    @Override
    public void flush() throws Exception {}

    @Override
    public void produce(PutItemRequest request) {}

    @Override
    public void produce(DeleteItemRequest request) {}

    @Override
    public void produce(UpdateItemRequest request) {}
}
