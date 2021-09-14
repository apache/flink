/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.dynamodb;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.Serializable;

/**
 * Creates multiple {@link WriteRequest WriteRequests} from an element in a stream.
 *
 * <p>This is used by sinks to prepare elements for sending them to DynamoDB.
 *
 * <p>Example:
 *
 * <pre>{@code
 * 				private static class TestDynamoDBSinkFunction implements
 * 					DynamoDBSinkFunction<Tuple2<Integer, String>> {
 *
 * 				public PutItemRequest createPutItemRequest(Tuple2<Integer, String> element) {
 *   					Map<String, AttributeValue> item = new HashMap<>();
 *   					item.put("data", AttributeValue.build().s(element.f1).build());
 *   				    return PutItemRequest.builder().item(item).build();
 *              }
 *
 * 			public void process(Tuple2<Integer, String> value, RuntimeContext ctx, DynamoDbProducer dynamoDbProducer) {
 * 				dynamoDbProducer.produce(createPutItemRequest(value));
 * 			}
 * 	}
 *
 * }</pre>
 *
 * @param <IN> The type of the element handled by this {@code DynamoDbSinkFunction}
 */
public interface DynamoDbSinkFunction<IN> extends Serializable, Function {

    /**
     * Process the incoming element to produce {@link PutRequest putRequests}. The produced requests
     * should be added to the provided {@link DynamoDbProducer}.
     *
     * @param value incoming value to process
     * @param context runtime context containing information about the sink instance
     * @param dynamoDbProducer DynamoDb producer that {@code WriteRequest} should be added to
     */
    void process(IN value, RuntimeContext context, DynamoDbProducer dynamoDbProducer);
}
