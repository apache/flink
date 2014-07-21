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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.streaming.api.invokable.DefaultSinkInvokable;
import org.apache.flink.streaming.api.invokable.StreamRecordInvokable;
import org.apache.flink.streaming.api.invokable.UserSinkInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.MutableObjectIterator;

public class StreamSink<IN extends Tuple> extends AbstractStreamComponent<IN, IN> {

	private static final Log LOG = LogFactory.getLog(StreamSink.class);

	@SuppressWarnings("rawtypes")
	private MutableReader inputs;
	private MutableObjectIterator<StreamRecord<IN>> inputIter;
	private StreamRecordInvokable<IN, IN> userFunction;

	public StreamSink() {
		userFunction = null;
	}

	@Override
	public void registerInputOutput() {
		initialize();

		try {
			setSerializers();
			setSinkSerializer();
			inputs = getConfigInputs();
			inputIter = createInputIterator(inputs, inTupleSerializer);
		} catch (Exception e) {
			throw new StreamComponentException("Cannot register inputs for "
					+ getClass().getSimpleName(), e);
		}

		setInvokable();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void setInvokable() {
		Class<? extends UserSinkInvokable> userFunctionClass = configuration.getClass(
				"userfunction", DefaultSinkInvokable.class, UserSinkInvokable.class);
		userFunction = (UserSinkInvokable<IN>) getInvokable(userFunctionClass);
		userFunction.initialize(collector, inputIter, inTupleSerializer);
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK " + name + " invoked");
		}

		userFunction.invoke();

		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK " + name + " invoke finished");
		}
	}

}
