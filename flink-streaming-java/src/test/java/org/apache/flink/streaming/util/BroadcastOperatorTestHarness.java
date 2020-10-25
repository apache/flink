/*
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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;

/**
 * A test harness for testing a {@link CoBroadcastWithNonKeyedOperator}.
 *
 * <p>This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements and
 * watermarks can be retrieved. They are safe to be modified.
 */
public class BroadcastOperatorTestHarness<IN1, IN2, OUT>
	extends AbstractBroadcastStreamOperatorTestHarness<IN1, IN2, OUT> {

	public BroadcastOperatorTestHarness(
		CoBroadcastWithNonKeyedOperator<IN1, IN2, OUT> operator,
		int maxParallelism,
		int numSubtasks,
		int subtaskIndex)
		throws Exception {
		super(operator, maxParallelism, numSubtasks, subtaskIndex);
	}

	public <KS, V> BroadcastState<KS, V> getBroadcastState(MapStateDescriptor<KS, V> stateDescriptor)
		throws Exception {
		return getOperator().getOperatorStateBackend().getBroadcastState(stateDescriptor);
	}
}
