/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.operator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.ArrayList;
import java.util.List;

public class SiddhiSyntaxTest {

	private SiddhiManager siddhiManager;

	@Before
	public void setUp() {
		siddhiManager = new SiddhiManager();
	}

	@After
	public void after() {
		siddhiManager = new SiddhiManager();
	}

	@Test
	public void testSimplePlan() throws InterruptedException {
		ExecutionPlanRuntime runtime = siddhiManager.createExecutionPlanRuntime(
			"define stream inStream (name string, value double);"
				+ "from inStream insert into outStream");
		runtime.start();

		final List<Object[]> received = new ArrayList<>(3);
		InputHandler inputHandler = runtime.getInputHandler("inStream");
		Assert.assertNotNull(inputHandler);

		try {
			runtime.getInputHandler("unknownStream");
			Assert.fail("Should throw exception for getting input handler for unknown streamId.");
		} catch (Exception ex) {
			// Expected exception for getting input handler for illegal streamId.
		}

		runtime.addCallback("outStream", new StreamCallback() {
			@Override
			public void receive(Event[] events) {
				for (Event event : events) {
					received.add(event.getData());
				}
			}
		});

		inputHandler.send(new Object[]{"a", 1.1});
		inputHandler.send(new Object[]{"b", 1.2});
		inputHandler.send(new Object[]{"c", 1.3});
		Thread.sleep(100);
		Assert.assertEquals(3, received.size());
		Assert.assertArrayEquals(received.get(0), new Object[]{"a", 1.1});
		Assert.assertArrayEquals(received.get(1), new Object[]{"b", 1.2});
		Assert.assertArrayEquals(received.get(2), new Object[]{"c", 1.3});
	}
}
