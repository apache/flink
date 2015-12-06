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

package org.apache.flink.tez.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;


public class RegularProcessor<S extends Function, OT> extends AbstractLogicalIOProcessor {

	private TezTask<S,OT> task;
	protected Map<String, LogicalInput> inputs;
	protected Map<String, LogicalOutput> outputs;
	private List<KeyValueReader> readers;
	private List<KeyValueWriter> writers;
	private int numInputs;
	private int numOutputs;


	public RegularProcessor(ProcessorContext context) {
		super(context);
	}

	@Override
	public void initialize() throws Exception {
		UserPayload payload = getContext().getUserPayload();
		Configuration conf = TezUtils.createConfFromUserPayload(payload);

		TezTaskConfig taskConfig = (TezTaskConfig) EncodingUtils.decodeObjectFromString(conf.get(TezTaskConfig.TEZ_TASK_CONFIG), getClass().getClassLoader());
		taskConfig.setTaskName(getContext().getTaskVertexName());

		RuntimeUDFContext runtimeUdfContext = new RuntimeUDFContext(
				new TaskInfo(
						getContext().getTaskVertexName(),
						getContext().getTaskIndex(),
						getContext().getVertexParallelism(),
						getContext().getTaskAttemptNumber()
				),
				getClass().getClassLoader(),
				new ExecutionConfig(),
				new HashMap<String, Future<Path>>(),
				new HashMap<String, Accumulator<?, ?>>());

		this.task = new TezTask<S, OT>(taskConfig, runtimeUdfContext, this.getContext().getTotalMemoryAvailableToTask());
	}

	@Override
	public void handleEvents(List<Event> processorEvents) {

	}

	@Override
	public void close() throws Exception {
		task.getIOManager().shutdown();
	}

	@Override
	public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {

		this.inputs = inputs;
		this.outputs = outputs;
		final Class<? extends Driver<S, OT>> driverClass = this.task.getTaskConfig().getDriver();
		Driver<S,OT> driver = InstantiationUtil.instantiate(driverClass, Driver.class);
		this.numInputs = driver.getNumberOfInputs();
		this.numOutputs = outputs.size();


		this.readers = new ArrayList<KeyValueReader>(numInputs);
		//Ensure size of list is = numInputs
		for (int i = 0; i < numInputs; i++)
			this.readers.add(null);
		HashMap<String, ArrayList<Integer>> inputPositions = ((TezTaskConfig) this.task.getTaskConfig()).getInputPositions();
		if (this.inputs != null) {
			for (String name : this.inputs.keySet()) {
				LogicalInput input = this.inputs.get(name);
				//if (input instanceof AbstractLogicalInput) {
				//	((AbstractLogicalInput) input).initialize();
				//}
				input.start();
				ArrayList<Integer> positions = inputPositions.get(name);
				for (Integer pos : positions) {
					//int pos = inputPositions.get(name);
					readers.set(pos, (KeyValueReader) input.getReader());
				}
			}
		}

		this.writers = new ArrayList<KeyValueWriter>(numOutputs);
		if (this.outputs != null) {
			for (LogicalOutput output : this.outputs.values()) {
				output.start();
				writers.add((KeyValueWriter) output.getWriter());
			}
		}

		// Do the work
		task.invoke (readers, writers);
	}
}
