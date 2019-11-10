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

package org.apache.flink.formats.protobuf;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.Message;

/**
 Decorates a protobuf Serializer to ensure that IDL 'string' fields of deserialized Message objects
 are eagerly unpacked as Strings, not ByteArrays.

 This is a workaround for an issue that surfaces when consuming a DataSource from Kafka in a Flink
 TableEnvironment. For fields declared with type 'string' in .proto, the corresponding field on the
 Java class has declared type 'Object'. The actual type of these fields on objects returned by
 Message.parseFrom(byte[]) is 'ByteArray'. But the getter methods for these fields return 'String',
 lazily replacing the underlying ByteArray field with a String, when necessary.

 Before this lazy conversion, the actual type of the field is a ByteArray, but the TypeInformation
 derived from the TypeDescriptor declares that the PojoField is a String. This leads to the
 following exception when running Flink jobs in a TableEnvironment on a DataStream from Kafka:

 <pre>
 Caused by: java.lang.ClassCastException: com.google.protobuf.ByteString$LiteralByteString cannot be
     cast to java.lang.String
     at DataStreamSourceConversion$300.processElement(Unknown Source)
     at org.apache.flink.table.runtime.CRowOutputProcessRunner.processElement(CRowOutputProcessRunner\
     .scala:67)
     ...
     at org.apache.flink.streaming.connectors.kafka.internal.Kafka010Fetcher.emitRecord(\
     Kafka010Fetcher.java:87)
     at org.apache.flink.streaming.connectors.kafka.internal.Kafka09Fetcher.runFetchLoop(\
     Kafka09Fetcher.java:151)
     at org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase.run(\
     FlinkKafkaConsumerBase.java:652)
     ...
  </pre>
 This class invokes the .toString() method on each marshalled Message instance, which invokes the
 getter method for each field and ensures that all Strings actually have Java type String.
 */
public class NonLazyProtobufSerializer extends Serializer<Message> {

	private final Serializer serializer;

	public NonLazyProtobufSerializer() {
		this(new ProtobufKryoSerializer());
	}

	public NonLazyProtobufSerializer(Serializer serializer) {
		this.serializer = serializer;
	}

	@Override
	public void write(Kryo kryo, Output output, Message mes) {
		this.serializer.write(kryo, output, mes);
	}

	@Override
	public Message read(Kryo kryo, Input input, Class<Message> pbClass) {
		Message msg = (Message) this.serializer.read(kryo, input, pbClass);
		// .toString() has useful side-effects, see JavaDoc on the class
		msg.toString();
		return msg;
	}

	@Override
	public Message copy(Kryo kryo, Message original) {
		Message msg = (Message) this.serializer.copy(kryo, original);
		// .toString() has useful side-effects, see JavaDoc on the class
		msg.toString();
		return msg;
	}
}
