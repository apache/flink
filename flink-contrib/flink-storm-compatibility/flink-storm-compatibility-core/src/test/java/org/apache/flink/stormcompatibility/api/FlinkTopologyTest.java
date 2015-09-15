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

package org.apache.flink.stormcompatibility.api;

import org.junit.Assert;
import org.junit.Test;

public class FlinkTopologyTest {

	@Test
	public void testDefaultParallelism() {
		final FlinkTopology topology = new FlinkTopology();
		Assert.assertEquals(1, topology.getParallelism());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testExecute() throws Exception {
		new FlinkTopology().execute();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testExecuteWithName() throws Exception {
		new FlinkTopology().execute(null);
	}

	@Test
	public void testNumberOfTasks() {
		final FlinkTopology topology = new FlinkTopology();

		Assert.assertEquals(0, topology.getNumberOfTasks());

		topology.increaseNumberOfTasks(3);
		Assert.assertEquals(3, topology.getNumberOfTasks());

		topology.increaseNumberOfTasks(2);
		Assert.assertEquals(5, topology.getNumberOfTasks());

		topology.increaseNumberOfTasks(8);
		Assert.assertEquals(13, topology.getNumberOfTasks());
	}

	@Test(expected = AssertionError.class)
	public void testAssert() {
		new FlinkTopology().increaseNumberOfTasks(0);
	}

}
