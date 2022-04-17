package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.annotation.Internal;

/**
 * DynamoDb {@link software.amazon.awssdk.services.dynamodb.model.WriteRequest} type.
 *
 * <ul>
 *   <li>PUT - {@link software.amazon.awssdk.services.dynamodb.model.PutRequest}
 *   <li>DELETE - {@link software.amazon.awssdk.services.dynamodb.model.DeleteRequest}
 * </ul>
 */
@Internal
public enum DynamoDbWriteRequestType {
    PUT,
    DELETE,
}
