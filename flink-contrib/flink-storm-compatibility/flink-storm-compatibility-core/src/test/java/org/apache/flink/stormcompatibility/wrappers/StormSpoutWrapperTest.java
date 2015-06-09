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

import backtype.storm.topology.IRichSpout;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.stormcompatibility.util.AbstractTest;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.LinkedList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StormWrapperSetupHelper.class)
public class StormSpoutWrapperTest extends AbstractTest {

	@Test
	public void testRunExecuteCancelInfinite() throws Exception {
		final int numberOfCalls = 5 + this.r.nextInt(5);

		final IRichSpout spout = new FiniteTestSpout(numberOfCalls);
		final StormSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormSpoutWrapper<Tuple1<Integer>>(spout);
		spoutWrapper.setRuntimeContext(mock(StreamingRuntimeContext.class));

		spoutWrapper.cancel();
		final TestContext collector = new TestContext();
		spoutWrapper.run(collector);

		Assert.assertEquals(new LinkedList<Tuple1<Integer>>(), collector.result);
	}

	@Test
	public void testClose() throws Exception {
		final IRichSpout spout = mock(IRichSpout.class);
		final StormSpoutWrapper<Tuple1<Integer>> spoutWrapper = new StormSpoutWrapper<Tuple1<Integer>>(spout);

		spoutWrapper.close();

		verify(spout).close();
	}

}
