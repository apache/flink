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
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.streaming.api.FaultToleranceBuffer;
import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.test.RandIS;

public class StreamSource extends AbstractInputTask<RandIS> {

	private List<RecordWriter<StreamRecord>> outputs;
	private List<ChannelSelector<StreamRecord>> partitioners;
	private UserSourceInvokable userFunction;
	private static int numSources = 0;
	private String sourceInstanceID;
	private FaultToleranceBuffer recordBuffer;

	public StreamSource() {
		// TODO: Make configuration file visible and call setClassInputs() here
		outputs = new LinkedList<RecordWriter<StreamRecord>>();
		partitioners = new LinkedList<ChannelSelector<StreamRecord>>();
		userFunction = null;
		numSources++;
		sourceInstanceID = Integer.toString(numSources);

	}

	@Override
	public RandIS[] computeInputSplits(int requestedMinNumber) throws Exception {
		return null;
	}

	@Override
	public Class<RandIS> getInputSplitType() {
		return null;
	}

	@Override
	public void registerInputOutput() {
		Configuration taskConfiguration = getTaskConfiguration();
		StreamComponentHelper<StreamSource> streamSourceHelper = new StreamComponentHelper<StreamSource>();

		try {
			streamSourceHelper.setConfigOutputs(this, taskConfiguration, outputs,
					partitioners);
		} catch (StreamComponentException e) {
			e.printStackTrace();
		}

		recordBuffer = new FaultToleranceBuffer(outputs, sourceInstanceID);
		userFunction = (UserSourceInvokable) streamSourceHelper.getUserFunction(
				taskConfiguration, outputs, sourceInstanceID, recordBuffer);
		streamSourceHelper.setAckListener(recordBuffer, sourceInstanceID, outputs);
		streamSourceHelper.setFailListener(recordBuffer, sourceInstanceID, outputs);

	}

	@Override
	public void invoke() throws Exception {

		userFunction.invoke();
//		System.out.println(this.getClass().getName() + "-" + sourceInstanceID);
//		System.out.println("---------------------");

	}

}
