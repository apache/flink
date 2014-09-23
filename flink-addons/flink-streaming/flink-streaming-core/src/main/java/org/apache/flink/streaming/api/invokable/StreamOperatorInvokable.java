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

package org.apache.flink.streaming.api.invokable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The StreamOperatorInvokable represents the base class for all operators in
 * the streaming topology.
 *
 * @param <IN>
 *            Input type of the operator
 * @param <OUT>
 *            Output type of the operator
 */
public abstract class StreamOperatorInvokable<IN, OUT> extends StreamInvokable<OUT> {

	public StreamOperatorInvokable(Function userFunction) {
		super(userFunction);
	}

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(StreamOperatorInvokable.class);

	protected MutableObjectIterator<StreamRecord<IN>> recordIterator;
	protected StreamRecordSerializer<IN> serializer;
	protected StreamRecord<IN> reuse;
	protected boolean isMutable;

	/**
	 * Initializes the {@link StreamOperatorInvokable} for input and output
	 * handling
	 * 
	 * @param collector
	 *            Collector object for collecting the outputs for the operator
	 * @param recordIterator
	 *            Iterator for reading in the input records
	 * @param serializer
	 *            Serializer used to deserialize inputs
	 * @param isMutable
	 *            Mutability setting for the operator
	 */
	public void initialize(Collector<OUT> collector,
			MutableObjectIterator<StreamRecord<IN>> recordIterator,
			StreamRecordSerializer<IN> serializer, boolean isMutable) {
		setCollector(collector);
		this.recordIterator = recordIterator;
		this.serializer = serializer;
		this.reuse = serializer.createInstance();
		this.isMutable = isMutable;
	}

	/**
	 * Re-initializes the object in which the next input record will be read in
	 */
	protected void resetReuse() {
		this.reuse = serializer.createInstance();
	}

	/**
	 * Method that will be called if the mutability setting is set to immutable
	 */
	protected abstract void immutableInvoke() throws Exception;

	/**
	 * Method that will be called if the mutability setting is set to mutable
	 */
	protected abstract void mutableInvoke() throws Exception;

	/**
	 * The call of the user implemented function should be implemented here
	 */
	protected abstract void callUserFunction() throws Exception;

	/**
	 * Method for logging exceptions thrown during the user function call
	 */
	protected void callUserFunctionAndLogException() {
		try {
			callUserFunction();
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Calling user function failed due to: {}",
						StringUtils.stringifyException(e));
			}
		}
	}

	@Override
	public void invoke() throws Exception {
		if (this.isMutable) {
			mutableInvoke();
		} else {
			immutableInvoke();
		}
	}
}
