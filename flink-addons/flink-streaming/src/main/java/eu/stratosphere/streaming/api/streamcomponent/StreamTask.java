/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api.streamcomponent;

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.streaming.api.AckEvent;
import eu.stratosphere.streaming.api.FailEvent;
import eu.stratosphere.streaming.api.FaultToleranceBuffer;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class StreamTask extends AbstractTask {

	private List<RecordReader<StreamRecord>> inputs;
	private List<RecordWriter<StreamRecord>> outputs;
	private List<ChannelSelector<StreamRecord>> partitioners;
	private UserTaskInvokable userFunction;
	private static int numTasks = 0;
	private String taskInstanceID = "";
	StreamComponentHelper<StreamTask> streamTaskHelper;

	private FaultToleranceBuffer recordBuffer;

	public StreamTask() {
		// TODO: Make configuration file visible and call setClassInputs() here
		inputs = new LinkedList<RecordReader<StreamRecord>>();
		outputs = new LinkedList<RecordWriter<StreamRecord>>();
		partitioners = new LinkedList<ChannelSelector<StreamRecord>>();
		userFunction = null;
		numTasks++;
		taskInstanceID = Integer.toString(numTasks);
		streamTaskHelper = new StreamComponentHelper<StreamTask>();
	}

	@Override
	public void registerInputOutput() {
		Configuration taskConfiguration = getTaskConfiguration();

		try {
			streamTaskHelper.setConfigInputs(this, taskConfiguration, inputs);
			streamTaskHelper.setConfigOutputs(this, taskConfiguration, outputs,
					partitioners);
		} catch (StreamComponentException e) {
			e.printStackTrace();
		}

		recordBuffer = new FaultToleranceBuffer(outputs, taskInstanceID,taskConfiguration.getInteger("numberOfOutputChannels", -1));
		userFunction = (UserTaskInvokable) streamTaskHelper.getUserFunction(
				taskConfiguration, outputs, taskInstanceID, recordBuffer);
		streamTaskHelper.setAckListener(recordBuffer, taskInstanceID, outputs);
		streamTaskHelper.setFailListener(recordBuffer, taskInstanceID, outputs);
	}

	@Override
	public void invoke() throws Exception {
		boolean hasInput = true;
		while (hasInput) {
			hasInput = false;
			for (RecordReader<StreamRecord> input : inputs) {
				if (input.hasNext()) {
					hasInput = true;
					StreamRecord streamRecord = input.next();
					String id = streamRecord.getId();
					// TODO create method for concurrent publishing
					try {
						userFunction.invoke(streamRecord);
						streamTaskHelper.threadSafePublish(new AckEvent(id), input);
					} catch (Exception e) {
						streamTaskHelper.threadSafePublish(new FailEvent(id), input);
						e.printStackTrace();
					}
				}
			}
		}
	}

}
