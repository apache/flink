/**
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
 */

package org.apache.flink.streaming.api.streamcomponent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.invokable.StreamRecordInvokable;

public class StreamSink<IN> extends SingleInputAbstractStreamComponent<IN, IN> {

	private static final Log LOG = LogFactory.getLog(StreamSink.class);

	private StreamRecordInvokable<IN, IN> userInvokable;

	public StreamSink() {
		userInvokable = null;
	}

	@Override
	public void setInputsOutputs() {
		try {
			setConfigInputs();
			setSinkSerializer();

			inputIter = createInputIterator(inputs, inputSerializer);
		} catch (Exception e) {
			throw new StreamComponentException("Cannot register inputs for "
					+ getClass().getSimpleName(), e);
		}
	}

	@Override
	protected void setInvokable() {
		userInvokable = getInvokable();
		userInvokable.initialize(collector, inputIter, inputSerializer, isMutable);
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK " + name + " invoked");
		}

		userInvokable.open(getTaskConfiguration());
		userInvokable.invoke();
		userInvokable.close();

		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK " + name + " invoke finished");
		}
	}

}
