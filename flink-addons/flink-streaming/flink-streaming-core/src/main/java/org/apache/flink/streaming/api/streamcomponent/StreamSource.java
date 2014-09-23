/*
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
 */

package org.apache.flink.streaming.api.streamcomponent;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.SourceInvokable;

public class StreamSource<OUT extends Tuple> extends AbstractStreamComponent {

	protected OutputHandler<OUT> outputHandler;

	private SourceInvokable<OUT> sourceInvokable;
	
	private static int numSources;

	public StreamSource() {
		sourceInvokable = null;
		numSources = newComponent();
		instanceID = numSources;
	}

	@Override
	public void setInputsOutputs() {
		outputHandler = new OutputHandler<OUT>(this);
	}

	@Override
	protected void setInvokable() {
		sourceInvokable = configuration.getUserInvokable();
		sourceInvokable.setCollector(outputHandler.getCollector());
	}

	@Override
	public void invoke() throws Exception {
		outputHandler.invokeUserFunction("SOURCE", sourceInvokable);
	}
}
