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
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.tez.runtime.input.FlinkInput;
import org.apache.flink.tez.runtime.output.TezChannelSelector;
import org.apache.flink.tez.runtime.output.TezOutputCollector;
import org.apache.flink.tez.runtime.output.TezOutputEmitter;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class DataSourceProcessor<OT> extends AbstractLogicalIOProcessor {

	private TezTaskConfig config;
	protected Map<String, LogicalOutput> outputs;
	private List<KeyValueWriter> writers;
	private int numOutputs;
	private Collector<OT> collector;

	private InputFormat<OT, InputSplit> format;
	private TypeSerializerFactory<OT> serializerFactory;
	private FlinkInput input;
	private ClassLoader userCodeClassLoader = getClass().getClassLoader();


	public DataSourceProcessor(ProcessorContext context) {
		super(context);
	}

	@Override
	public void initialize() throws Exception {
		UserPayload payload = getContext().getUserPayload();
		Configuration conf = TezUtils.createConfFromUserPayload(payload);

		this.config = (TezTaskConfig) EncodingUtils.decodeObjectFromString(conf.get(TezTaskConfig.TEZ_TASK_CONFIG), getClass().getClassLoader());
		config.setTaskName(getContext().getTaskVertexName());

		this.serializerFactory = config.getOutputSerializer(this.userCodeClassLoader);

		initInputFormat();
	}

	@Override
	public void handleEvents(List<Event> processorEvents) {
		int i = 0;
	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {

		Preconditions.checkArgument(inputs.size() == 1);
		LogicalInput logicalInput = inputs.values().iterator().next();
		if (!(logicalInput instanceof FlinkInput)) {
			throw new RuntimeException("Input to Flink Data Source Processor should be of type FlinkInput");
		}
		this.input = (FlinkInput) logicalInput;
		//this.reader = (KeyValueReader) input.getReader();

		// Initialize inputs, get readers and writers
		this.outputs = outputs;
		this.numOutputs = outputs.size();
		this.writers = new ArrayList<KeyValueWriter>(numOutputs);
		if (this.outputs != null) {
			for (LogicalOutput output : this.outputs.values()) {
				output.start();
				writers.add((KeyValueWriter) output.getWriter());
			}
		}
		this.invoke();
	}


	private void invoke () {
		final TypeSerializer<OT> serializer = this.serializerFactory.getSerializer();
		try {
			InputSplit split = input.getSplit();

			OT record = serializer.createInstance();
			final InputFormat<OT, InputSplit> format = this.format;
			format.open(split);

			int numOutputs = outputs.size();
			ArrayList<TezChannelSelector<OT>> channelSelectors = new ArrayList<TezChannelSelector<OT>>(numOutputs);
			ArrayList<Integer> numStreamsInOutputs = this.config.getNumberSubtasksInOutput();
			for (int i = 0; i < numOutputs; i++) {
				final ShipStrategyType strategy = config.getOutputShipStrategy(i);
				final TypeComparatorFactory<OT> compFactory = config.getOutputComparator(i, this.userCodeClassLoader);
				final DataDistribution dataDist = config.getOutputDataDistribution(i, this.userCodeClassLoader);
				if (compFactory == null) {
					channelSelectors.add(i, new TezOutputEmitter<OT>(strategy));
				} else if (dataDist == null){
					final TypeComparator<OT> comparator = compFactory.createComparator();
					channelSelectors.add(i, new TezOutputEmitter<OT>(strategy, comparator));
				} else {
					final TypeComparator<OT> comparator = compFactory.createComparator();
					channelSelectors.add(i,new TezOutputEmitter<OT>(strategy, comparator, dataDist));
				}
			}
			collector = new TezOutputCollector<OT>(writers, channelSelectors, serializerFactory.getSerializer(), numStreamsInOutputs);

			while (!format.reachedEnd()) {
				// build next pair and ship pair if it is valid
				if ((record = format.nextRecord(record)) != null) {
					collector.collect(record);
				}
			}
			format.close();

			collector.close();

		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			try {
				this.format.close();
			} catch (Throwable t) {}
		}
	}


	private void initInputFormat() {
		try {
			this.format = config.<InputFormat<OT, InputSplit>>getStubWrapper(this.userCodeClassLoader)
					.getUserCodeObject(InputFormat.class, this.userCodeClassLoader);

			// check if the class is a subclass, if the check is required
			if (!InputFormat.class.isAssignableFrom(this.format.getClass())) {
				throw new RuntimeException("The class '" + this.format.getClass().getName() + "' is not a subclass of '" +
						InputFormat.class.getName() + "' as is required.");
			}
		}
		catch (ClassCastException ccex) {
			throw new RuntimeException("The stub class is not a proper subclass of " + InputFormat.class.getName(),
					ccex);
		}
		// configure the stub. catch exceptions here extra, to report them as originating from the user code
		try {
			this.format.configure(this.config.getStubParameters());
		}
		catch (Throwable t) {
			throw new RuntimeException("The user defined 'configure()' method caused an error: " + t.getMessage(), t);
		}
	}


}
