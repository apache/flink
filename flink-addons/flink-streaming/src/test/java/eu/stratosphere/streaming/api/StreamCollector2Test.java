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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class StreamCollector2Test {

	StreamCollector2<Tuple> collector;

	@Test
	public void testCollect() {
		List<Integer> batchSizesOfNotPartitioned = new ArrayList<Integer>();
		List<Integer> batchSizesOfPartitioned = new ArrayList<Integer>();
		batchSizesOfPartitioned.add(2);
		batchSizesOfPartitioned.add(2);
		List<Integer> parallelismOfOutput = new ArrayList<Integer>();
		parallelismOfOutput.add(2);
		parallelismOfOutput.add(2);
		int keyPosition = 0;
		long batchTimeout = 1000;
		int channelID = 1;
		
		List<RecordWriter<StreamRecord>> fOut = new ArrayList<RecordWriter<StreamRecord>>();
		
		fOut.add(null);
		fOut.add(null);
		
		collector = new StreamCollector2<Tuple>(batchSizesOfNotPartitioned, batchSizesOfPartitioned, parallelismOfOutput, keyPosition, batchTimeout, channelID, null, fOut,fOut);
	
		Tuple1<Integer> t = new Tuple1<Integer>();
		StreamCollector<Tuple> sc1 = new StreamCollector<Tuple>(1, batchTimeout, channelID, null);
		
		t.f0 = 0;
		collector.collect(t);
		t.f0 = 1;
		collector.collect(t);
		t.f0 = 0;
		collector.collect(t);
		t.f0 = 1;
		collector.collect(t);
	}
	
	@Test
	public void testClose() {
	}

}
