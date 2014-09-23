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

import org.apache.flink.streaming.api.invokable.StreamOperatorInvokable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSink<IN> extends AbstractStreamComponent {

	private static final Logger LOG = LoggerFactory.getLogger(StreamSink.class);

	private InputHandler<IN> inputHandler;
	
	private StreamOperatorInvokable<IN, IN> userInvokable;

	public StreamSink() {
		userInvokable = null;
	}

	@Override
	public void setInputsOutputs() {
		inputHandler = new InputHandler<IN>(this);
	}

	@Override
	protected void setInvokable() {
		userInvokable = configuration.getUserInvokable();
		userInvokable.initialize(null, inputHandler.getInputIter(), inputHandler.getInputSerializer(),
				isMutable);
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK {} invoked", getName());
		}

		invokeUserFunction(userInvokable);

		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK {} invoke finished", getName());
		}
	}

}
