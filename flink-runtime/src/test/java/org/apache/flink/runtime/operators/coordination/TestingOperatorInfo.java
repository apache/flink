/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;

/**
 * A testing implementation of the {@link OperatorInfo}.
 */
public class TestingOperatorInfo implements OperatorInfo {

	private final OperatorID operatorId;
	private final int parallelism;
	private final int maxParallelism;

	public TestingOperatorInfo() {
		this(new OperatorID(), 50, 256);
	}

	public TestingOperatorInfo(
			OperatorID operatorId,
			int parallelism,
			int maxParallelism) {
		this.operatorId = operatorId;
		this.parallelism = parallelism;
		this.maxParallelism = maxParallelism;
	}

	@Override
	public OperatorID operatorId() {
		return operatorId;
	}

	@Override
	public int maxParallelism() {
		return maxParallelism;
	}

	@Override
	public int currentParallelism() {
		return parallelism;
	}
}
