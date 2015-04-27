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

package org.apache.flink.streaming.api.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.runtime.state.StateCheckpoint;
import org.junit.Test;

public class OperatorStateTest {

	@Test
	public void testOperatorState() {
		OperatorState<Integer> os = new OperatorState<Integer>(5);

		StateCheckpoint<Integer> scp = os.checkpoint();

		assertTrue(os.stateEquals(scp.restore()));

		assertEquals((Integer) 5, os.getState());

		os.update(10);

		assertEquals((Integer) 10, os.getState());
	}

}
