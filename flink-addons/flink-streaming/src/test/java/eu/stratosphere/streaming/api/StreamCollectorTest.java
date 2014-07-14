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

package eu.stratosphere.streaming.api;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;

import org.junit.Test;
import org.mockito.Mockito;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.streamcomponent.MockRecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class StreamCollectorTest {

	@Test
	public void testStreamCollector() {
		StreamCollector collector = new StreamCollector(10, 1000, 0, null);
		assertEquals(10, collector.batchSize);
	}

	@Test
	public void testCollect() {
		StreamCollector collector = new StreamCollector(2, 1000, 0, null);
		collector.collect(new Tuple1<Integer>(3));
		collector.collect(new Tuple1<Integer>(4));
		collector.collect(new Tuple1<Integer>(5));
		collector.collect(new Tuple1<Integer>(6));

	}

	@Test
	public void testBatchSize() throws InterruptedException {
		System.out.println("---------------");
		StreamCollector collector = new StreamCollector(3, 100, 0, null);
		collector.collect(new Tuple1<Integer>(0));
		collector.collect(new Tuple1<Integer>(0));
		collector.collect(new Tuple1<Integer>(0));

		Thread.sleep(200);
		collector.collect(new Tuple1<Integer>(2));
		collector.collect(new Tuple1<Integer>(3));
		System.out.println("---------------");
	}

	@Test
	public void recordWriter() {
		MockRecordWriter recWriter = mock(MockRecordWriter.class);
		
		Mockito.when(recWriter.initList()).thenCallRealMethod();
		doCallRealMethod().when(recWriter).emit(Mockito.any(StreamRecord.class));
		
		recWriter.initList();
		
		ArrayList<RecordWriter<StreamRecord>> rwList = new ArrayList<RecordWriter<StreamRecord>>();
		rwList.add(recWriter);

		StreamCollector collector = new StreamCollector(1, 1000, 0, null, rwList);
		collector.collect(new Tuple1<Integer>(3));
		
		assertEquals((Integer) 3, recWriter.emittedRecords.get(0).getTuple(0).getField(0));
	}

	@Test
	public void testClose() {
	}

}
