/**
 *
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
 *
 */

package org.apache.flink.streaming.api.streamrecord;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.io.network.serialization.DataInputDeserializer;
import org.apache.flink.runtime.io.network.serialization.DataOutputSerializer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.junit.Test;

public class StreamRecordTest {

	@Test
	public void testReadWrite() throws IOException {
		StreamRecord<Tuple2<Integer, String>> streamRecord = new StreamRecord<Tuple2<Integer, String>>();
		Tuple2<Integer, String> tuple = new Tuple2<Integer, String>(2, "a");
		streamRecord.setTuple(tuple).setId(1);

		TupleSerializer<Tuple2<Integer, String>> ts = (TupleSerializer<Tuple2<Integer, String>>) TypeExtractor
				.getForObject(tuple).createSerializer();

		SerializationDelegate<Tuple2<Integer, String>> sd = new SerializationDelegate<Tuple2<Integer, String>>(
				ts);
		streamRecord.setSeralizationDelegate(sd);

		DataOutputSerializer out = new DataOutputSerializer(64);
		streamRecord.write(out);

		ByteBuffer buff = out.wrapAsByteBuffer();

		DataInputDeserializer in = new DataInputDeserializer(buff);

		StreamRecord<Tuple2<Integer, String>> streamRecord2 = new StreamRecord<Tuple2<Integer, String>>();

		streamRecord2.setDeseralizationDelegate(
				new DeserializationDelegate<Tuple2<Integer, String>>(ts), ts);

		streamRecord2.read(in);

		assertEquals(streamRecord.getTuple(), streamRecord2.getTuple());
	}

}
