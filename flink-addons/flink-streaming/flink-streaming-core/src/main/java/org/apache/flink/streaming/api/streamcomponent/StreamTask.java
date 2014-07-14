/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package org.apache.flink.streaming.api.streamcomponent;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.invokable.DefaultTaskInvokable;
import org.apache.flink.streaming.api.invokable.StreamRecordInvokable;
import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.api.AbstractRecordReader;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.api.RecordWriter;

public class StreamTask extends AbstractStreamComponent {

	private static final Log log = LogFactory.getLog(StreamTask.class);

	private AbstractRecordReader inputs;
	private List<RecordWriter<StreamRecord>> outputs;
	private List<ChannelSelector<StreamRecord>> partitioners;
	private StreamRecordInvokable<Tuple, Tuple> userFunction;
	private int[] numberOfOutputChannels;
	private static int numTasks;

	public StreamTask() {
		
		// TODO: Make configuration file visible and call setClassInputs() here
		outputs = new LinkedList<RecordWriter<StreamRecord>>();
		partitioners = new LinkedList<ChannelSelector<StreamRecord>>();
		userFunction = null;
		numTasks = newComponent();
		instanceID = numTasks;
	}

	@Override
	public void registerInputOutput() {
		initialize();

		try {
			setSerializers();
			setCollector();
			inputs = getConfigInputs();
			setConfigOutputs(outputs, partitioners);
		} catch (StreamComponentException e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot register inputs/outputs for " + getClass().getSimpleName(), e);
			}
		}

		numberOfOutputChannels = new int[outputs.size()];
		for (int i = 0; i < numberOfOutputChannels.length; i++) {
			numberOfOutputChannels[i] = configuration.getInteger("channels_" + i, 0);
		}

		setInvokable();

		// streamTaskHelper.setAckListener(recordBuffer, taskInstanceID,
		// outputs);
		// streamTaskHelper.setFailListener(recordBuffer, taskInstanceID,
		// outputs);
	}

	// TODO consider logging stack trace!
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void setInvokable() {
		// Default value is a TaskInvokable even if it was called from a source
		Class<? extends UserTaskInvokable> userFunctionClass = configuration.getClass(
				"userfunction", DefaultTaskInvokable.class, UserTaskInvokable.class);
		userFunction = (UserTaskInvokable<Tuple, Tuple>) getInvokable(userFunctionClass);
	}

	@Override
	public void invoke() throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("TASK " + name + " invoked with instance id " + instanceID);
		}

		for (RecordWriter<StreamRecord> output : outputs) {
			output.initializeSerializers();
		}

		invokeRecords(userFunction, inputs);

		if (log.isDebugEnabled()) {
			log.debug("TASK " + name + " invoke finished with instance id " + instanceID);
		}
	}
}