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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

public class StreamSource<OUT extends Tuple> extends SingleInputAbstractStreamComponent<Tuple, OUT> {

	private static final Log LOG = LogFactory.getLog(StreamSource.class);

	private List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> outputs;
	private SourceInvokable<OUT> userInvokable;
	private static int numSources;

	public StreamSource() {

		outputs = new LinkedList<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>>();
		userInvokable = null;
		numSources = newComponent();
		instanceID = numSources;
	}

	@Override
	public void setInputsOutputs() {
		try {
			setConfigOutputs(outputs);
		} catch (StreamComponentException e) {
			throw new StreamComponentException("Cannot register outputs for "
					+ getClass().getSimpleName(), e);
		}
	}

	@Override
	protected void setInvokable() {
		userInvokable = getInvokable();
		// setCollector();
		userInvokable.setCollector(collector);
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("SOURCE " + name + " invoked with instance id " + instanceID);
		}

		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.initializeSerializers();
		}

		userInvokable.open(getTaskConfiguration());
		userInvokable.invoke();
		userInvokable.close();

		if (LOG.isDebugEnabled()) {
			LOG.debug("SOURCE " + name + " invoke finished with instance id " + instanceID);
		}

		for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputs) {
			output.flush();
		}
	}

}
