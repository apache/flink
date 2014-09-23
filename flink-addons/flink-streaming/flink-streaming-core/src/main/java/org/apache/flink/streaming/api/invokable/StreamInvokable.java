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
import org.apache.flink.util.Collector;

/**
 * The StreamInvokable represents the base class for all invokables in
 * the streaming topology.
 *
 * @param <OUT>
 *            The output type of the invokable
 */
public abstract class StreamInvokable<OUT> implements Serializable {

	private static final long serialVersionUID = 1L;

	protected Collector<OUT> collector;
	protected Function userFunction;
	protected volatile boolean isRunning;

	public StreamInvokable(Function userFunction) {
		this.userFunction = userFunction;
	}

	public void setCollector(Collector<OUT> collector) {
		this.collector = collector;
	}

	/**
	 * Open method to be used if the user defined function extends the
	 * RichFunction class
	 * 
	 * @param parameters
	 *            The configuration parameters for the operator
	 */
	public void open(Configuration parameters) throws Exception {
		isRunning=true;
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

	/**
	 * The method that will be called once when the operator is created, the
	 * working mechanics of the operator should be implemented here
	 * 
	 */
	public abstract void invoke() throws Exception;
}
