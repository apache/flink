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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * A test for the {@link TypeSerializer TypeSerializers} used for
 * {@link FlinkKafkaProducer011.NextTransactionalIdHint}.
 */
public class NextTransactionalIdHintSerializerTest extends
	SerializerTestBase<FlinkKafkaProducer011.NextTransactionalIdHint> {

	@Override
	protected TypeSerializer<FlinkKafkaProducer011.NextTransactionalIdHint> createSerializer() {
		return new FlinkKafkaProducer011.NextTransactionalIdHintSerializer();
	}

	@Override
	protected int getLength() {
		return Long.BYTES + Integer.BYTES;
	}

	@Override
	protected Class<FlinkKafkaProducer011.NextTransactionalIdHint> getTypeClass() {
		return (Class) FlinkKafkaProducer011.NextTransactionalIdHint.class;
	}

	@Override
	protected FlinkKafkaProducer011.NextTransactionalIdHint[] getTestData() {
		return new FlinkKafkaProducer011.NextTransactionalIdHint[] {
			new FlinkKafkaProducer011.NextTransactionalIdHint(1, 0L),
			new FlinkKafkaProducer011.NextTransactionalIdHint(1, 1L),
			new FlinkKafkaProducer011.NextTransactionalIdHint(1, -1L),
			new FlinkKafkaProducer011.NextTransactionalIdHint(2, 0L),
			new FlinkKafkaProducer011.NextTransactionalIdHint(2, 1L),
			new FlinkKafkaProducer011.NextTransactionalIdHint(2, -1L),
		};
	}
}
