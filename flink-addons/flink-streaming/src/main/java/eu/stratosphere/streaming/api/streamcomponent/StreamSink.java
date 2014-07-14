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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.io.AbstractRecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.streamcomponent.StreamComponentHelper.RecordInvoker;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;

public class StreamSink extends AbstractOutputTask {

	private static final Log log = LogFactory.getLog(StreamSink.class);

	private AbstractRecordReader inputs;
	private UserSinkInvokable userFunction;
	private StreamComponentHelper<StreamSink> streamSinkHelper;
	private String name;
	private RecordInvoker invoker;

	public StreamSink() {
		// TODO: Make configuration file visible and call setClassInputs() here
		userFunction = null;
		streamSinkHelper = new StreamComponentHelper<StreamSink>();
	}

	@Override
	public void registerInputOutput() {
		Configuration taskConfiguration = getTaskConfiguration();
		name = taskConfiguration.getString("componentName", "MISSING_COMPONENT_NAME");

		try {
			streamSinkHelper.setSerializers(taskConfiguration);
			inputs = streamSinkHelper.getConfigInputs(this, taskConfiguration);
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot register inputs", e);
			}
		}

		FaultToleranceType faultToleranceType = FaultToleranceType.from(taskConfiguration
				.getInteger("faultToleranceType", 0));
		invoker = streamSinkHelper.getRecordInvoker(faultToleranceType);

		userFunction = streamSinkHelper.getUserFunction(taskConfiguration);
	}

	@Override
	public void invoke() throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("SINK " + name + " invoked");
		}
		streamSinkHelper.invokeRecords(invoker, userFunction, inputs, name);
		System.out.println("Result: " + userFunction.getResult());
		if (log.isDebugEnabled()) {
			log.debug("SINK " + name + " invoke finished");
		}
	}
}
