/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Schema used for deserializing DynamoDB streams records.
 */
public class DynamoDBStreamsSchema implements KinesisDeserializationSchema<Record> {
	private static final ObjectMapper MAPPER = new ObjectMapper();

	@Override
	public Record deserialize(byte[] message, String partitionKey, String seqNum,
			long approxArrivalTimestamp, String stream, String shardId) throws IOException {
		return MAPPER.readValue(message, Record.class);
	}

	@Override
	public TypeInformation<Record> getProducedType() {
		return TypeInformation.of(Record.class);
	}

}
