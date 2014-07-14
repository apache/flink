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

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.examples.DummyIS;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceUtil;

public class StreamSource extends AbstractInputTask<DummyIS> {

	private static final Log log = LogFactory.getLog(StreamSource.class);

	private List<RecordWriter<StreamRecord>> outputs;
	private List<ChannelSelector<StreamRecord>> partitioners;
	private UserSourceInvokable userFunction;
	private static int numSources;
	private String sourceInstanceID;
	private String name;
	private FaultToleranceUtil recordBuffer;
	StreamComponentHelper<StreamSource> streamSourceHelper;

	public StreamSource() {
		// TODO: Make configuration file visible and call setClassInputs() here
		outputs = new LinkedList<RecordWriter<StreamRecord>>();
		partitioners = new LinkedList<ChannelSelector<StreamRecord>>();
		userFunction = null;
		streamSourceHelper = new StreamComponentHelper<StreamSource>();
		numSources=StreamComponentHelper.newComponent();
		sourceInstanceID = Integer.toString(numSources);
	}

	@Override
	public DummyIS[] computeInputSplits(int requestedMinNumber) throws Exception {
		return null;
	}

	@Override
	public Class<DummyIS> getInputSplitType() {
		return null;
	}

	@Override
	public void registerInputOutput() {
		Configuration taskConfiguration = getTaskConfiguration();
		name = taskConfiguration.getString("componentName", "MISSING_COMPONENT_NAME");

		try {
			streamSourceHelper.setConfigOutputs(this, taskConfiguration, outputs,
					partitioners);
		} catch (StreamComponentException e) {
			log.error("Cannot register outputs", e);
		}
		
		int[] numberOfOutputChannels= new int[outputs.size()];
		for(int i=0; i<numberOfOutputChannels.length;i++ ){
			numberOfOutputChannels[i]=taskConfiguration.getInteger("channels_"+i, 0);
		}
		
		recordBuffer = new FaultToleranceUtil(outputs, sourceInstanceID, numberOfOutputChannels);
		userFunction = (UserSourceInvokable) streamSourceHelper.getUserFunction(
				taskConfiguration, outputs, sourceInstanceID, name, recordBuffer);
		streamSourceHelper.setAckListener(recordBuffer, sourceInstanceID, outputs);
		streamSourceHelper.setFailListener(recordBuffer, sourceInstanceID, outputs);
	}

	@Override
	public void invoke() throws Exception {
		log.debug("SOURCE " + name + " invoked with instance id " + sourceInstanceID);
		userFunction.invoke();
		// TODO print to file
		System.out.println(userFunction.getResult());
	}

}
