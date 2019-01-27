/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.kafka.v2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import java.io.IOException;

/** Kafka MSG deserialization. */
public class KafkaMessageDeserialization implements KeyedDeserializationSchema<BaseRow> {
	private BaseRowTypeInfo baseRowTypeInfo;

	public KafkaMessageDeserialization(BaseRowTypeInfo baseRowTypeInfo) {
		this.baseRowTypeInfo = baseRowTypeInfo;
	}

	@Override
	public GenericRow deserialize(byte[] bytes, byte[] bytes1, String s, int i, long l) throws IOException {
		GenericRow row = new GenericRow(5);
		row.update(0, bytes);
		row.update(1, bytes1);
		row.update(2, BinaryString.fromString(s));
		row.update(3, i);
		row.update(4, l);
		return row;
	}

	@Override
	public boolean isEndOfStream(BaseRow kafkaMessage) {
		return false;
	}

	@Override
	public TypeInformation<BaseRow> getProducedType() {
		return baseRowTypeInfo;
	}
}
