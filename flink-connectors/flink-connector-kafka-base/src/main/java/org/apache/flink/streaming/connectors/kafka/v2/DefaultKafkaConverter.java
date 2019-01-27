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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.kafka.v2.common.TableBaseInfo;
import org.apache.flink.types.Row;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Record converter for Kafka. */
public class DefaultKafkaConverter extends TableBaseInfo implements KafkaConverter<Row>, Serializable {

	private static final long serialVersionUID = 1L;
	private List<Integer> keyIndex;
	private List<Integer> valueIndex;

	@Override
	public ProducerRecord convert(Row row, String topic, int[] partitions) {
		byte[] key = null;
		byte[] value = null;
		if (keyIndex.size() != 0) {
			key = (byte[]) row.getField(keyIndex.get(0));
		}
		if (valueIndex.size() != 0) {
			value = (byte[]) row.getField(valueIndex.get(0));
		}
		return new ProducerRecord(topic, key, value);
	}

	@Override
	public void open(RuntimeContext context) {
		keyIndex = new ArrayList<>();
		valueIndex = new ArrayList<>();
		if (null != primaryKeys && primaryKeys.size() != 0) {
			keyIndex.add(rowTypeInfo.getFieldIndex(primaryKeys.get(0)));
			for (String field : rowTypeInfo.getFieldNames()) {
				if (!primaryKeys.contains(field)) {
					valueIndex.add(rowTypeInfo.getFieldIndex(field));
				}
			}
		}
	}

	@Override
	public void close() {

	}
}
