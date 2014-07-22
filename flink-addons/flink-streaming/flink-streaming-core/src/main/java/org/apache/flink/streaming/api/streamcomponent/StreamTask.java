/**
 *
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
 *
 */

package org.apache.flink.streaming.api.streamcomponent;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.invokable.StreamRecordInvokable;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.MutableObjectIterator;

public class StreamTask<IN extends Tuple, OUT extends Tuple> extends
		SingleInputAbstractStreamComponent<IN, OUT> {

	private static final Log LOG = LogFactory.getLog(StreamTask.class);

	private MutableReader<IOReadableWritable> inputs;
	MutableObjectIterator<StreamRecord<IN>> inputIter;
	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
	private StreamRecordInvokable<IN, OUT> userFunction;
	private int[] numberOfOutputChannels;
	private static int numTasks;

	public StreamTask() {

		outputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		userFunction = null;
		numTasks = newComponent();
		instanceID = numTasks;
	}

	@Override
	public void registerInputOutput() {
		initialize();

		setSerializers();
		setCollector();
		inputs = getConfigInputs();
		setConfigOutputs(outputs);

		inputIter = createInputIterator(inputs, inTupleSerializer);

		numberOfOutputChannels = new int[outputs.size()];
		for (int i = 0; i < numberOfOutputChannels.length; i++) {
			numberOfOutputChannels[i] = configuration.getInteger("channels_" + i, 0);
		}

		setInvokable();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void setInvokable() {
		// Default value is a TaskInvokable even if it was called from a source
		Class<? extends UserTaskInvokable> userFunctionClass = configuration.getClass(
				"userfunction", UserTaskInvokable.class, UserTaskInvokable.class);
		userFunction = (UserTaskInvokable<IN, OUT>) getInvokable(userFunctionClass);
		userFunction.initialize(collector, inputIter, inTupleSerializer, isMutable);
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("TASK " + name + " invoked with instance id " + instanceID);
		}

		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.initializeSerializers();
		}

		userFunction.invoke();

		if (LOG.isDebugEnabled()) {
			LOG.debug("TASK " + name + " invoke finished with instance id " + instanceID);
		}

		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.flush();
		}
	}
}