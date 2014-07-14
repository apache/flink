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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.runtime.io.api.ChannelSelector;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.streaming.api.invokable.DefaultSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class StreamSource extends AbstractStreamComponent {

	private static final Log log = LogFactory.getLog(StreamSource.class);

	private List<RecordWriter<StreamRecord>> outputs;
	private List<ChannelSelector<StreamRecord>> partitioners;
	private UserSourceInvokable<Tuple> userFunction;
	private static int numSources;
	private int[] numberOfOutputChannels;
	// private FaultToleranceUtil recordBuffer;
	// private FaultToleranceType faultToleranceType;

	public StreamSource() {

		
		outputs = new LinkedList<RecordWriter<StreamRecord>>();
		partitioners = new LinkedList<ChannelSelector<StreamRecord>>();
		userFunction = null;
		numSources = newComponent();
		instanceID = numSources;
	}

	@Override
	public void registerInputOutput() {
		initialize();
		
		try {
			setSerializers();
			setCollector();
			setConfigOutputs(outputs, partitioners);
		} catch (StreamComponentException e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot register outputs", e);
			}
		}

		numberOfOutputChannels = new int[outputs.size()];
		for (int i = 0; i < numberOfOutputChannels.length; i++) {
			numberOfOutputChannels[i] = configuration.getInteger("channels_" + i, 0);
		}

		setInvokable();
		// streamSourceHelper.setAckListener(recordBuffer, sourceInstanceID,
		// outputs);
		// streamSourceHelper.setFailListener(recordBuffer, sourceInstanceID,
		// outputs);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void setInvokable() {
		// Default value is a TaskInvokable even if it was called from a source
		Class<? extends UserSourceInvokable> userFunctionClass = configuration.getClass(
				"userfunction", DefaultSourceInvokable.class, UserSourceInvokable.class);
		userFunction = (UserSourceInvokable<Tuple>) getInvokable(userFunctionClass);
	}

	@Override
	public void invoke() throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("SOURCE " + name + " invoked with instance id " + instanceID);
		}

		for (RecordWriter<StreamRecord> output : outputs) {
			output.initializeSerializers();
		}
		
		userFunction.invoke(collectorManager);
		
		if (log.isDebugEnabled()) {
			log.debug("SOURCE " + name + " invoke finished with instance id " + instanceID);
		}
	}

}
