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

package org.apache.flink.storm.wrappers;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.storm.util.AbstractTest;
import org.apache.flink.streaming.api.operators.Output;

import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for the BoltCollector.
 */
public class BoltCollectorTest extends AbstractTest {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testBoltStormCollector() throws InstantiationException, IllegalAccessException {
		for (int numberOfAttributes = -1; numberOfAttributes < 26; ++numberOfAttributes) {
			final Output flinkCollector = mock(Output.class);
			Tuple flinkTuple = null;
			final Values tuple = new Values();

			BoltCollector<?> collector;

			final String streamId = "streamId";
			HashMap<String, Integer> attributes = new HashMap<String, Integer>();
			attributes.put(streamId, numberOfAttributes);

			if (numberOfAttributes == -1) {
				collector = new BoltCollector(attributes, -1, flinkCollector);
				tuple.add(new Integer(this.r.nextInt()));

			} else {
				collector = new BoltCollector(attributes, -1, flinkCollector);
				flinkTuple = Tuple.getTupleClass(numberOfAttributes).newInstance();

				for (int i = 0; i < numberOfAttributes; ++i) {
					tuple.add(new Integer(this.r.nextInt()));
					flinkTuple.setField(tuple.get(i), i);
				}
			}

			final Collection anchors = mock(Collection.class);
			final List<Integer> taskIds;
			taskIds = collector.emit(streamId, anchors, tuple);

			Assert.assertNull(taskIds);

			if (numberOfAttributes == -1) {
				verify(flinkCollector).collect(tuple.get(0));
			} else {
				verify(flinkCollector).collect(flinkTuple);
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testBoltStormCollectorWithTaskId() throws InstantiationException, IllegalAccessException {
		for (int numberOfAttributes = 0; numberOfAttributes < 25; ++numberOfAttributes) {
			final Output flinkCollector = mock(Output.class);
			final int taskId = 42;
			final String streamId = "streamId";

			HashMap<String, Integer> attributes = new HashMap<String, Integer>();
			attributes.put(streamId, numberOfAttributes);

			BoltCollector<?> collector = new BoltCollector(attributes, taskId, flinkCollector);

			final Values tuple = new Values();
			final Tuple flinkTuple = Tuple.getTupleClass(numberOfAttributes + 1).newInstance();

			for (int i = 0; i < numberOfAttributes; ++i) {
				tuple.add(new Integer(this.r.nextInt()));
				flinkTuple.setField(tuple.get(i), i);
			}
			flinkTuple.setField(taskId, numberOfAttributes);

			final Collection anchors = mock(Collection.class);
			final List<Integer> taskIds;
			taskIds = collector.emit(streamId, anchors, tuple);

			Assert.assertNull(taskIds);

			verify(flinkCollector).collect(flinkTuple);
		}
	}

	@SuppressWarnings("unchecked")
	@Test(expected = UnsupportedOperationException.class)
	public void testToManyAttributes() {
		HashMap<String, Integer> attributes = new HashMap<String, Integer>();
		attributes.put("", 26);

		new BoltCollector<Object>(attributes, -1, mock(Output.class));
	}

	@SuppressWarnings("unchecked")
	@Test(expected = UnsupportedOperationException.class)
	public void testToManyAttributesWithTaskId() {
		HashMap<String, Integer> attributes = new HashMap<String, Integer>();
		attributes.put("", 25);

		new BoltCollector<Object>(attributes, 42, mock(Output.class));
	}

	@SuppressWarnings("unchecked")
	@Test(expected = UnsupportedOperationException.class)
	public void testRawStreamWithTaskId() {
		HashMap<String, Integer> attributes = new HashMap<String, Integer>();
		attributes.put("", -1);

		new BoltCollector<Object>(attributes, 42, mock(Output.class));
	}

	@SuppressWarnings("unchecked")
	@Test(expected = UnsupportedOperationException.class)
	public void testEmitDirect() {
		new BoltCollector<Object>(mock(HashMap.class), -1, mock(Output.class)).emitDirect(0, null,
				null, null);
	}

}
