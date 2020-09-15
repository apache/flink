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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

/**
 * Table operator to invoke close always.
 */
public class TableStreamOperator<OUT> extends AbstractStreamOperator<OUT> {

	private volatile boolean closed = false;

	public TableStreamOperator() {
		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void close() throws Exception {
		super.close();
		closed = true;
	}

	@Override
	public void dispose() throws Exception {
		if (!closed) {
			close();
		}
		super.dispose();
	}

	/**
	 * Compute memory size from memory faction.
	 */
	public long computeMemorySize() {
		return getContainingTask().getEnvironment().getMemoryManager().computeMemorySize(
				getOperatorConfig().getManagedMemoryFractionOperatorUseCaseOfSlot(
					ManagedMemoryUseCase.BATCH_OP,
					getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration()));
	}
}
