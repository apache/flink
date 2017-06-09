/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.storm.util;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the NullTerminatingSpout.
 */
public class NullTerminatingSpoutTest {

	@Test
	public void testMethodCalls() {
		Map<String, Object> compConfig = new HashMap<String, Object>();

		IRichSpout spoutMock = mock(IRichSpout.class);
		when(spoutMock.getComponentConfiguration()).thenReturn(compConfig);

		Map<?, ?> conf = mock(Map.class);
		TopologyContext context = mock(TopologyContext.class);
		Object msgId = mock(Object.class);
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

		NullTerminatingSpout spout = new NullTerminatingSpout(spoutMock);

		spout.open(conf, context, null);
		spout.close();
		spout.activate();
		spout.deactivate();
		spout.ack(msgId);
		spout.fail(msgId);
		spout.declareOutputFields(declarer);
		Map<String, Object> c = spoutMock.getComponentConfiguration();

		verify(spoutMock).open(same(conf), same(context), any(SpoutOutputCollector.class));
		verify(spoutMock).close();
		verify(spoutMock).activate();
		verify(spoutMock).deactivate();
		verify(spoutMock).ack(same(msgId));
		verify(spoutMock).fail(same(msgId));
		verify(spoutMock).declareOutputFields(same(declarer));
		Assert.assertSame(compConfig, c);
	}

	@Test
	public void testReachedEnd() {
		NullTerminatingSpout finiteSpout = new NullTerminatingSpout(new TestDummySpout());
		finiteSpout.open(null, null, mock(SpoutOutputCollector.class));

		Assert.assertFalse(finiteSpout.reachedEnd());

		finiteSpout.nextTuple();
		Assert.assertFalse(finiteSpout.reachedEnd());
		finiteSpout.nextTuple();
		Assert.assertTrue(finiteSpout.reachedEnd());
	}

}
