/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.collector;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.collector.StreamCollector;
import eu.stratosphere.streaming.api.streamcomponent.MockRecordWriter;
import eu.stratosphere.streaming.util.MockRecordWriterFactory;

public class StreamCollectorTest {

	@Test
	public void testStreamCollector() {
		MockRecordWriter recWriter = MockRecordWriterFactory.create();

		StreamCollector<Tuple1<Integer>> collector = new StreamCollector<Tuple1<Integer>>(10, 1000,
				0, null, recWriter,0);
		assertEquals(10, collector.batchSize);
	}

	@Test
	public void testCollect() {
		MockRecordWriter recWriter = MockRecordWriterFactory.create();

		StreamCollector<Tuple1<Integer>> collector = new StreamCollector<Tuple1<Integer>>(2, 1000,
				0, null, recWriter,0);
		collector.collect(new Tuple1<Integer>(3));
		collector.collect(new Tuple1<Integer>(4));
		collector.collect(new Tuple1<Integer>(5));
		collector.collect(new Tuple1<Integer>(6));

	}

	@Test
	public void testBatchSize() throws InterruptedException {
		MockRecordWriter recWriter = MockRecordWriterFactory.create();

		StreamCollector<Tuple1<Integer>> collector = new StreamCollector<Tuple1<Integer>>(3, 100,
				0, null, recWriter,0);
		collector.collect(new Tuple1<Integer>(0));
		collector.collect(new Tuple1<Integer>(0));
		collector.collect(new Tuple1<Integer>(0));

		Thread.sleep(200);
		collector.collect(new Tuple1<Integer>(2));
		collector.collect(new Tuple1<Integer>(3));
	}

	@Test
	public void recordWriter() {
		MockRecordWriter recWriter = MockRecordWriterFactory.create();

		StreamCollector<Tuple1<Integer>> collector = new StreamCollector<Tuple1<Integer>>(2, 1000,
				0, null, recWriter,0);
		collector.collect(new Tuple1<Integer>(3));
		collector.collect(new Tuple1<Integer>(4));
		collector.collect(new Tuple1<Integer>(5));
		collector.collect(new Tuple1<Integer>(6));

		assertEquals((Integer) 3, recWriter.emittedRecords.get(0).getTuple(0).getField(0));
		assertEquals((Integer) 6, recWriter.emittedRecords.get(1).getTuple(1).getField(0));
	}

	@Test
	public void testClose() {
	}

}
