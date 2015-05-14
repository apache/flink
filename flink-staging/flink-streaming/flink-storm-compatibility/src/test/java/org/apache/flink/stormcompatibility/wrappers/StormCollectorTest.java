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

package org.apache.flink.stormcompatibility.wrappers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import backtype.storm.tuple.Values;





public class StormCollectorTest extends AbstractTest {
	
	@Test
	public void testSpoutStormCollector() throws InstantiationException, IllegalAccessException {
		for(int i = 0; i < 26; ++i) {
			this.testStromCollector(true, i);
		}
	}
	
	@Test
	public void testBoltStormCollector() throws InstantiationException, IllegalAccessException {
		for(int i = 0; i < 26; ++i) {
			this.testStromCollector(false, i);
		}
	}
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	private void testStromCollector(final boolean spoutTest, final int numberOfAttributes)
		throws InstantiationException, IllegalAccessException {
		assert ((0 <= numberOfAttributes) && (numberOfAttributes <= 25));
		
		final Collector flinkCollector = mock(Collector.class);
		Tuple flinkTuple = null;
		final Values tuple = new Values();
		
		StormCollector<?> collector = null;
		
		if(numberOfAttributes == 0) {
			collector = new StormCollector(numberOfAttributes, flinkCollector);
			tuple.add(new Integer(this.r.nextInt()));
			
		} else {
			collector = new StormCollector(numberOfAttributes, flinkCollector);
			flinkTuple = Tuple.getTupleClass(numberOfAttributes).newInstance();
			
			for(int i = 0; i < numberOfAttributes; ++i) {
				tuple.add(new Integer(this.r.nextInt()));
				flinkTuple.setField(tuple.get(i), i);
			}
		}
		
		final String streamId = "streamId";
		final Collection anchors = mock(Collection.class);
		final List<Integer> taskIds;
		final Object messageId = new Integer(this.r.nextInt());
		if(spoutTest) {
			taskIds = collector.emit(streamId, tuple, messageId);
		} else {
			taskIds = collector.emit(streamId, anchors, tuple);
		}
		
		Assert.assertNull(taskIds);
		
		if(numberOfAttributes == 0) {
			verify(flinkCollector).collect(tuple.get(0));
		} else {
			verify(flinkCollector).collect(flinkTuple);
		}
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testReportError() {
		new StormCollector<Object>(1, null).reportError(null);
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test(expected = UnsupportedOperationException.class)
	public void testBoltEmitDirect() {
		new StormCollector<Object>(1, null).emitDirect(0, (String)null, (Collection)null, (List)null);
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test(expected = UnsupportedOperationException.class)
	public void testSpoutEmitDirect() {
		new StormCollector<Object>(1, null).emitDirect(0, (String)null, (List)null, (Object)null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testAck() {
		new StormCollector<Object>(1, null).ack(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testFail() {
		new StormCollector<Object>(1, null).fail(null);
	}
	
}
