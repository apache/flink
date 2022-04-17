package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.annotation.Internal;

/** enum representing the dynamodb types. */
@Internal
public enum DynamoDbType {
    STRING,
    NUMBER,
    BOOLEAN,
    NULL,
    BINARY,
    STRING_SET,
    NUMBER_SET,
    BINARY_SET,
    LIST,
    MAP,
}
