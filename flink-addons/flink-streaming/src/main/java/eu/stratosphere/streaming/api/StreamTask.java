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

package eu.stratosphere.streaming.api;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.streaming.api.invokable.DefaultTaskInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.types.Record;

//TODO: Refactor, create common ancestor with StreamSource
public class StreamTask extends AbstractTask {

	private List<RecordReader<Record>> inputs;
	private List<RecordWriter<Record>> outputs;
	private List<ChannelSelector<Record>> partitioners;
	private UserTaskInvokable userFunction;
	private int numberOfInputs;
	private int numberOfOutputs;

	private static int numTasks = 0;
	private String taskInstanceID = "";

	private FaultTolerancyBuffer recordBuffer;

	public StreamTask() {
		// TODO: Make configuration file visible and call setClassInputs() here
		inputs = new LinkedList<RecordReader<Record>>();
		outputs = new LinkedList<RecordWriter<Record>>();
		partitioners = new LinkedList<ChannelSelector<Record>>();
		userFunction = null;
		numberOfInputs = 0;
		numberOfOutputs = 0;
		numTasks++;
		taskInstanceID = Integer.toString(numTasks);

	}

	public void setUserFunction(Configuration taskConfiguration) {
		Class<? extends UserTaskInvokable> userFunctionClass = taskConfiguration
				.getClass("userfunction", DefaultTaskInvokable.class,
						UserTaskInvokable.class);
		try {
			userFunction = userFunctionClass.newInstance();
			userFunction.declareOutputs(outputs, taskInstanceID, recordBuffer);
		} catch (Exception e) {

		}
	}

	@Override
	public void registerInputOutput() {
		Configuration taskConfiguration = getTaskConfiguration();

		numberOfInputs = StreamComponentFactory.setConfigInputs(this,
				taskConfiguration, inputs);
		numberOfOutputs = StreamComponentFactory.setConfigOutputs(this,
				taskConfiguration, outputs, partitioners);

		recordBuffer = new FaultTolerancyBuffer(outputs);

		setUserFunction(taskConfiguration);
		StreamComponentFactory
				.setAckListener(recordBuffer, taskInstanceID, outputs);
	}

	@Override
	public void invoke() throws Exception {
		boolean hasInput = true;
		while (hasInput) {
			hasInput = false;
			for (RecordReader<Record> input : inputs) {
				if (input.hasNext()) {
					hasInput = true;
					StreamRecord streamRecord = new StreamRecord(input.next());
					String id = streamRecord.popId();
					// TODO: Enclose invoke in try-catch to properly fail
					// records
					userFunction.invoke(streamRecord.getRecord());
					System.out.println(this.getClass().getName() + "-" + taskInstanceID);
					System.out.println(recordBuffer.getRecordBuffer());
					System.out.println("---------------------");
					input.publishEvent(new AckEvent(id));
				}
			}
		}
	}

}
