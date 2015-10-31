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

import backtype.storm.tuple.Values;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.storm.util.AbstractTest;
import org.apache.flink.storm.wrappers.BoltCollector;
import org.apache.flink.streaming.api.operators.Output;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
				collector = new BoltCollector(attributes, flinkCollector);
				tuple.add(new Integer(this.r.nextInt()));

			} else {
				collector = new BoltCollector(attributes, flinkCollector);
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

	@SuppressWarnings("unchecked")
	@Test(expected = UnsupportedOperationException.class)
	public void testEmitDirect() {
		new BoltCollector<Object>(mock(HashMap.class), mock(Output.class)).emitDirect(0, null,
				null, null);
	}

}
