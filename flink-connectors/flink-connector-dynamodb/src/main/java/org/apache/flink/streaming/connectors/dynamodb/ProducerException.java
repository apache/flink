package org.apache.flink.streaming.connectors.dynamodb;

/**
 * Exception is thrown when DynamoDb producer failed to write data. Messages in transit might have
 * been lost.
 */
public class ProducerException extends RuntimeException {
    public ProducerException(String message) {
        super(message);
    }

    public ProducerException(String message, Throwable cause) {
        super(message, cause);
    }
}
