/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.event;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.functions.aggregators.Aggregator;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactString;
import eu.stratosphere.types.Value;


public class EventWithAggregatorsTest {
	
	private ClassLoader cl = ClassLoader.getSystemClassLoader();
	
	@Test
	public void testSerializationOfEmptyEvent() {
		AllWorkersDoneEvent e = new AllWorkersDoneEvent();
		IterationEventWithAggregators deserialized = pipeThroughSerialization(e);
		
		Assert.assertEquals(0, deserialized.getAggregatorNames().length);
		Assert.assertEquals(0, deserialized.getAggregates(cl).length);
	}
	
	@Test
	public void testSerializationOfEventWithAggregateValues() {
		PactString stringValue = new PactString("test string");
		PactLong longValue = new PactLong(68743254);
		
		String stringValueName = "stringValue";
		String longValueName = "longValue";
		
		Aggregator<PactString> stringAgg = new TestAggregator<PactString>(stringValue);
		Aggregator<PactLong> longAgg = new TestAggregator<PactLong>(longValue);
		
		Map<String, Aggregator<?>> aggMap = new HashMap<String,  Aggregator<?>>();
		aggMap.put(stringValueName, stringAgg);
		aggMap.put(longValueName, longAgg);
		
		Set<String> allNames = new HashSet<String>();
		allNames.add(stringValueName);
		allNames.add(longValueName);
		
		Set<Value> allVals = new HashSet<Value>();
		allVals.add(stringValue);
		allVals.add(longValue);
		
		// run the serialization
		AllWorkersDoneEvent e = new AllWorkersDoneEvent(aggMap);
		IterationEventWithAggregators deserialized = pipeThroughSerialization(e);
		
		// verify the result
		String[] names = deserialized.getAggregatorNames();
		Value[] aggregates = deserialized.getAggregates(cl);
		
		Assert.assertEquals(allNames.size(), names.length);
		Assert.assertEquals(allVals.size(), aggregates.length);
		
		// check that all the correct names and values are returned
		for (String s : names) {
			allNames.remove(s);
		}
		for (Value v : aggregates) {
			allVals.remove(v);
		}
		
		Assert.assertTrue(allNames.isEmpty());
		Assert.assertTrue(allVals.isEmpty());
	}
	
	private IterationEventWithAggregators pipeThroughSerialization(IterationEventWithAggregators event) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(baos);
			event.write(out);
			out.flush();
			
			byte[] data = baos.toByteArray();
			out.close();
			baos.close();
			
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
			IterationEventWithAggregators newEvent = event.getClass().newInstance();
			newEvent.read(in);
			in.close();
			
			return newEvent;
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test threw an exception: " + e.getMessage());
			return null;
		}
	}
	
	private static class TestAggregator<T extends Value> implements Aggregator<T> {

		private final T val;
		
		
		public TestAggregator(T val) {
			this.val = val;
		}

		@Override
		public T getAggregate() {
			return val;
		}

		@Override
		public void aggregate(T element) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void reset() {
			throw new UnsupportedOperationException();
		}
	}

}
