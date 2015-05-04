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

package org.apache.flink.streaming.api.collector;

import static org.junit.Assert.assertArrayEquals;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamtask.MockRecordWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockRecordWriterFactory;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class StreamCollectorTest {

	@Test
	public void testCollect() {
		MockRecordWriter recWriter = MockRecordWriterFactory.create();
		SerializationDelegate<StreamRecord<Tuple1<Integer>>> sd = new SerializationDelegate<StreamRecord<Tuple1<Integer>>>(
				null);
		sd.setInstance(new StreamRecord<Tuple1<Integer>>().setObject(new Tuple1<Integer>()));

		Collector<Tuple1<Integer>> collector = new StreamOutput<Tuple1<Integer>>(recWriter, sd);
		collector.collect(new Tuple1<Integer>(3));
		collector.collect(new Tuple1<Integer>(4));
		collector.collect(new Tuple1<Integer>(5));
		collector.collect(new Tuple1<Integer>(6));

		assertArrayEquals(new Integer[] { 3, 4, 5, 6 }, recWriter.emittedRecords.toArray());
	}

	@Test
	public void testClose() {
	}

}
