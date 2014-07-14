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

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.streaming.api.invokable.DefaultSinkInvokable;
import eu.stratosphere.streaming.api.invokable.DefaultTaskInvokable;
import eu.stratosphere.streaming.api.invokable.RecordInvokable;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.AckEvent;
import eu.stratosphere.streaming.faulttolerance.AckEventListener;
import eu.stratosphere.streaming.faulttolerance.FailEvent;
import eu.stratosphere.streaming.faulttolerance.FailEventListener;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceUtil;
import eu.stratosphere.streaming.partitioner.DefaultPartitioner;
import eu.stratosphere.streaming.partitioner.FieldsPartitioner;

public final class StreamComponentHelper<T extends AbstractInvokable> {
	private static final Log log = LogFactory.getLog(StreamComponentHelper.class);
	private static int numComponents = 0;

	public static int newComponent() {
		numComponents++;
		return numComponents;
	}

	public void setAckListener(FaultToleranceUtil recordBuffer, int sourceInstanceID,
			List<RecordWriter<StreamRecord>> outputs) {

		EventListener[] ackListeners = new EventListener[outputs.size()];

		for (int i = 0; i < outputs.size(); i++) {
			ackListeners[i] = new AckEventListener(sourceInstanceID, recordBuffer, i);
			outputs.get(i).subscribeToEvent(ackListeners[i], AckEvent.class);
		}

	}

	public void setFailListener(FaultToleranceUtil recordBuffer, int sourceInstanceID,
			List<RecordWriter<StreamRecord>> outputs) {

		EventListener[] failListeners = new EventListener[outputs.size()];

		for (int i = 0; i < outputs.size(); i++) {
			failListeners[i] = new FailEventListener(sourceInstanceID, recordBuffer, i);
			outputs.get(i).subscribeToEvent(failListeners[i], FailEvent.class);
		}

	}

	public void setConfigInputs(T taskBase, Configuration taskConfiguration, List<StreamRecordReader<StreamRecord>> inputs)
			throws StreamComponentException {
		int numberOfInputs = taskConfiguration.getInteger("numberOfInputs", 0);
		for (int i = 0; i < numberOfInputs; i++) {

			if (taskBase instanceof StreamTask) {
				inputs.add(new StreamRecordReader<StreamRecord>((StreamTask) taskBase, StreamRecord.class));
			} else if (taskBase instanceof StreamSink) {
				inputs.add(new StreamRecordReader<StreamRecord>((StreamSink) taskBase, StreamRecord.class));
			} else {
				throw new StreamComponentException("Nonsupported object passed to setConfigInputs");
			}
		}
	}

	public void setConfigOutputs(T taskBase, Configuration taskConfiguration, List<RecordWriter<StreamRecord>> outputs,
			List<ChannelSelector<StreamRecord>> partitioners) throws StreamComponentException {
		int numberOfOutputs = taskConfiguration.getInteger("numberOfOutputs", 0);
		for (int i = 0; i < numberOfOutputs; i++) {
			setPartitioner(taskConfiguration, i, partitioners);
		}
		for (ChannelSelector<StreamRecord> outputPartitioner : partitioners) {
			if (taskBase instanceof StreamTask) {
				outputs.add(new RecordWriter<StreamRecord>((StreamTask) taskBase, StreamRecord.class, outputPartitioner));
			} else if (taskBase instanceof StreamSource) {
				outputs.add(new RecordWriter<StreamRecord>((StreamSource) taskBase, StreamRecord.class,
						outputPartitioner));
			} else {
				throw new StreamComponentException("Nonsupported object passed to setConfigOutputs");
			}
		}
	}

	public UserSinkInvokable getUserFunction(Configuration taskConfiguration) {

		Class<? extends UserSinkInvokable> userFunctionClass = taskConfiguration.getClass("userfunction",
				DefaultSinkInvokable.class, UserSinkInvokable.class);
		UserSinkInvokable userFunction = null;

		try {
			userFunction = userFunctionClass.newInstance();
		} catch (Exception e) {
			log.error("Cannot instanciate user function: " + userFunctionClass.getSimpleName());
		}
		return userFunction;
	}

	public StreamInvokableComponent getUserFunction(Configuration taskConfiguration,
			List<RecordWriter<StreamRecord>> outputs, int instanceID, String name, FaultToleranceUtil recordBuffer) {

		// Default value is a TaskInvokable even if it was called from a source
		Class<? extends StreamInvokableComponent> userFunctionClass = taskConfiguration.getClass("userfunction",
				DefaultTaskInvokable.class, StreamInvokableComponent.class);
		StreamInvokableComponent userFunction = null;

		try {
			userFunction = userFunctionClass.newInstance();
			userFunction.declareOutputs(outputs, instanceID, name, recordBuffer);
		} catch (InstantiationException e) {
			log.error("Cannot instanciate user function: " + userFunctionClass.getSimpleName());
		} catch (Exception e) {
			log.error("Cannot use user function: " + userFunctionClass.getSimpleName());
		}
		return userFunction;
	}

	// TODO find a better solution for this
	public void threadSafePublish(AbstractTaskEvent event, StreamRecordReader<StreamRecord> input)
			throws InterruptedException, IOException {

		boolean concurrentModificationOccured = false;
		while (!concurrentModificationOccured) {
			try {
				input.publishEvent(event);
				concurrentModificationOccured = true;
			} catch (ConcurrentModificationException exeption) {
				log.trace("Waiting to publish " + event.getClass());
			}
		}
	}

	private void setPartitioner(Configuration taskConfiguration, int nrOutput,
			List<ChannelSelector<StreamRecord>> partitioners) {
		Class<? extends ChannelSelector<StreamRecord>> partitioner = taskConfiguration.getClass("partitionerClass_"
				+ nrOutput, DefaultPartitioner.class, ChannelSelector.class);

		try {
			if (partitioner.equals(FieldsPartitioner.class)) {
				int keyPosition = taskConfiguration.getInteger("partitionerIntParam_" + nrOutput, 1);

				partitioners.add(partitioner.getConstructor(int.class).newInstance(keyPosition));

			} else {
				partitioners.add(partitioner.newInstance());
			}
			log.trace("Partitioner set: " + partitioner.getSimpleName() + " with " + nrOutput + " outputs");
		} catch (Exception e) {
			log.error("Error while setting partitioner: " + partitioner.getSimpleName() + " with " + nrOutput
					+ " outputs", e);
		}
	}

	public void invokeRecords(RecordInvokable userFunction, List<StreamRecordReader<StreamRecord>> inputs, String name)
			throws Exception {
		List<StreamRecordReader<StreamRecord>> closedInputs = new LinkedList<StreamRecordReader<StreamRecord>>();
		boolean hasInput = true;
		while (hasInput) {
			hasInput = false;
			for (StreamRecordReader<StreamRecord> input : inputs) {
				if (input.hasNext()) {
					hasInput = true;
					StreamRecord record = input.next();
//					UID id = record.getId();
					userFunction.invoke(record);
//					threadSafePublish(new AckEvent(id), input);
//					log.debug("ACK: " + id + " -- " + name);
				}
				else if (input.isInputClosed()) {
					closedInputs.add(input);
				}
			}
			inputs.removeAll(closedInputs);
		}
	}

}