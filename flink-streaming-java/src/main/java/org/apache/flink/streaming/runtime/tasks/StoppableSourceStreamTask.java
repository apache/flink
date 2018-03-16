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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.StoppableTask;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StoppableStreamSource;

/**
 * Stoppable task for executing stoppable streaming sources.
 *
 * @param <OUT> Type of the produced elements
 * @param <SRC> Stoppable source function
 */
public class StoppableSourceStreamTask<OUT, SRC extends SourceFunction<OUT> & StoppableFunction>
	extends SourceStreamTask<OUT, SRC, StoppableStreamSource<OUT, SRC>> implements StoppableTask {

	private volatile boolean stopped;

	public StoppableSourceStreamTask(Environment environment) {
		super(environment);
	}

	@Override
	protected void run() throws Exception {
		if (!stopped) {
			super.run();
		}
	}

	@Override
	public void stop() {
		stopped = true;
		if (this.headOperator != null) {
			this.headOperator.stop();
		}
	}
}
