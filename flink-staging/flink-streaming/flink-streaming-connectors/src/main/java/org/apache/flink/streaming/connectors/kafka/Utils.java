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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.ByteArrayInputView;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;

public class Utils {
	public static class TypeInformationSerializationSchema<T>
			implements DeserializationSchema<T>, SerializationSchema<T, byte[]> {
		private final TypeSerializer<T> serializer;
		private final TypeInformation<T> ti;

		public TypeInformationSerializationSchema(Object type, ExecutionConfig ec) {
			this.ti = (TypeInformation<T>) TypeExtractor.getForObject(type);
			this.serializer = ti.createSerializer(ec);
		}
		@Override
		public T deserialize(byte[] message) {
			try {
				return serializer.deserialize(new ByteArrayInputView(message));
			} catch (IOException e) {
				throw new RuntimeException("Unable to deserialize message", e);
			}
		}

		@Override
		public boolean isEndOfStream(T nextElement) {
			return false;
		}

		@Override
		public byte[] serialize(T element) {
			DataOutputSerializer dos = new DataOutputSerializer(16);
			try {
				serializer.serialize(element, dos);
			} catch (IOException e) {
				throw new RuntimeException("Unable to serialize record", e);
			}
			return dos.getByteArray();
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return ti;
		}
	}
}
