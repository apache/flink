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


import com.google.common.base.Preconditions;
import org.apache.flink.tez.util.EncodingUtils;
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
import java.util.List;
import java.util.Map;

public class UnionProcessor extends AbstractLogicalIOProcessor {

	private TezTaskConfig config;
	protected Map<String, LogicalInput> inputs;
	protected Map<String, LogicalOutput> outputs;
	private List<KeyValueReader> readers;
	private List<KeyValueWriter> writers;
	private int numInputs;
	private int numOutputs;

	public UnionProcessor(ProcessorContext context) {
		super(context);
	}

	@Override
	public void initialize() throws Exception {
		UserPayload payload = getContext().getUserPayload();
		Configuration conf = TezUtils.createConfFromUserPayload(payload);

		this.config = (TezTaskConfig) EncodingUtils.decodeObjectFromString(conf.get(TezTaskConfig.TEZ_TASK_CONFIG), getClass().getClassLoader());
		config.setTaskName(getContext().getTaskVertexName());
	}

	@Override
	public void handleEvents(List<Event> processorEvents) {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
		this.inputs = inputs;
		this.outputs = outputs;
		this.numInputs = inputs.size();
		this.numOutputs = outputs.size();

		this.readers = new ArrayList<KeyValueReader>(numInputs);
		if (this.inputs != null) {
			for (LogicalInput input: this.inputs.values()) {
				input.start();
				readers.add((KeyValueReader) input.getReader());
			}
		}

		this.writers = new ArrayList<KeyValueWriter>(numOutputs);
		if (this.outputs != null) {
			for (LogicalOutput output : this.outputs.values()) {
				output.start();
				writers.add((KeyValueWriter) output.getWriter());
			}
		}

		Preconditions.checkArgument(writers.size() == 1);
		KeyValueWriter writer = writers.get(0);

		for (KeyValueReader reader: this.readers) {
			while (reader.next()) {
				Object key = reader.getCurrentKey();
				Object value = reader.getCurrentValue();
				writer.write(key, value);
			}
		}
	}
}
