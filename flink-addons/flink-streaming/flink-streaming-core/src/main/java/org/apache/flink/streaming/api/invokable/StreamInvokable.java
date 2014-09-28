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

import java.io.Serializable;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The StreamInvokable represents the base class for all invokables in the
 * streaming topology.
 *
 * @param <OUT>
 *            The output type of the invokable
 */
public abstract class StreamInvokable<IN, OUT> implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(StreamInvokable.class);

	protected MutableObjectIterator<StreamRecord<IN>> recordIterator;
	protected StreamRecordSerializer<IN> inSerializer;
	protected StreamRecord<IN> reuse;
	protected boolean isMutable;

	protected Collector<OUT> collector;
	protected Function userFunction;
	protected volatile boolean isRunning;

	public StreamInvokable(Function userFunction) {
		this.userFunction = userFunction;
	}

	/**
	 * Initializes the {@link StreamInvokable} for input and output
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
		this.collector = collector;
		this.recordIterator = recordIterator;
		this.inSerializer = serializer;
		if(this.inSerializer != null){
			this.reuse = serializer.createInstance();
		}
		this.isMutable = isMutable;
	}

	/**
	 * Re-initializes the object in which the next input record will be read in
	 */
	protected void resetReuse() {
		this.reuse = inSerializer.createInstance();
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

	/**
	 * Method that will be called when the stream starts. The user should encode
	 * the processing functionality in {@link #mutableInvoke()} and
	 * {@link #immutableInvoke()}
	 * 
	 */
	public void invoke() throws Exception {
		if (this.isMutable) {
			mutableInvoke();
		} else {
			immutableInvoke();
		}
	}

	/**
	 * Open method to be used if the user defined function extends the
	 * RichFunction class
	 * 
	 * @param parameters
	 *            The configuration parameters for the operator
	 */
	public void open(Configuration parameters) throws Exception {
		isRunning = true;
		if (userFunction instanceof RichFunction) {
			((RichFunction) userFunction).open(parameters);
		}
	}

	/**
	 * Close method to be used if the user defined function extends the
	 * RichFunction class
	 * 
	 */
	public void close() throws Exception {
		isRunning = false;
		if (userFunction instanceof RichFunction) {
			((RichFunction) userFunction).close();
		}
	}
}
