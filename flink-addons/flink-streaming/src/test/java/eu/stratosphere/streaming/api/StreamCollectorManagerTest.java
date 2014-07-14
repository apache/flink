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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.streaming.api.streamcomponent.MockRecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.util.MockRecordWriterFactory;

public class StreamCollectorManagerTest {

	StreamCollectorManager<Tuple> collector;
	
	@Test
	public void testCollect() {
		List<Integer> batchSizesOfNotPartitioned = new ArrayList<Integer>();
		List<Integer> batchSizesOfPartitioned = new ArrayList<Integer>();
		batchSizesOfPartitioned.add(2);
		batchSizesOfPartitioned.add(3);
		List<Integer> parallelismOfOutput = new ArrayList<Integer>();
		parallelismOfOutput.add(2);
		parallelismOfOutput.add(2);
		int keyPosition = 0;
		long batchTimeout = 1000;
		int channelID = 1;
		
		List<RecordWriter<StreamRecord>> fOut = new ArrayList<RecordWriter<StreamRecord>>();
		
		MockRecordWriter rw1 = MockRecordWriterFactory.create();
		MockRecordWriter rw2 = MockRecordWriterFactory.create();
		
		fOut.add(rw1);
		fOut.add(rw2);
		
		collector = new StreamCollectorManager<Tuple>(batchSizesOfNotPartitioned, batchSizesOfPartitioned, parallelismOfOutput, keyPosition, batchTimeout, channelID, null, fOut,fOut);
		Tuple1<Integer> t = new Tuple1<Integer>();
		
		t.f0 = 0;
		collector.collect(t);
		t.f0 = 1;
		collector.collect(t);		
		t.f0 = 0;
		collector.collect(t);
		
		StreamRecord r1 = rw1.emittedRecords.get(0);
		assertEquals(1, rw1.emittedRecords.size());
		assertEquals(0, r1.getTuple(0).getField(0));
		assertEquals(0, r1.getTuple(1).getField(0));
		
		t.f0 = 1;
		collector.collect(t);

		StreamRecord r2 = rw1.emittedRecords.get(1);
		assertEquals(2, rw1.emittedRecords.size());
		assertEquals(1, r2.getTuple(0).getField(0));
		assertEquals(1, r2.getTuple(1).getField(0));
		
		assertEquals(0, rw2.emittedRecords.size());
		
		t.f0 = 5;
		collector.collect(t);
		assertEquals(2, rw1.emittedRecords.size());
		assertEquals(1, rw2.emittedRecords.size());
	}
}
