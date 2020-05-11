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

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;

import com.google.pubsub.v1.PubsubMessage;

/**
 * This class wraps a {@link DeserializationSchema} so it can be used in a {@link PubSubSource} as a {@link PubSubDeserializationSchema}.
 */
class DeserializationSchemaWrapper<T> implements PubSubDeserializationSchema<T> {
	private final DeserializationSchema<T> deserializationSchema;

	DeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		this.deserializationSchema.open(context);
	}

	@Override
	public boolean isEndOfStream(T t) {
		return deserializationSchema.isEndOfStream(t);
	}

	@Override
	public T deserialize(PubsubMessage pubsubMessage) throws Exception {
		return deserializationSchema.deserialize(pubsubMessage.getData().toByteArray());
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializationSchema.getProducedType();
	}
}
