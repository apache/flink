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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.io.OutputFormat;

/**
 * Implementation of FileSinkFunction. Writes tuples to file in every millis
 * milliseconds.
 * 
 * @param <IN>
 *            Input type
 */
public class FileSinkFunctionByMillis<IN> extends FileSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private final long millis;
	private long lastTime;

	public FileSinkFunctionByMillis(OutputFormat<IN> format, long millis) {
		super(format);
		this.millis = millis;
		lastTime = System.currentTimeMillis();
	}

	/**
	 * Condition for writing the contents of tupleList and clearing it.
	 * 
	 * @return value of the updating condition
	 */
	@Override
	protected boolean updateCondition() {
		return System.currentTimeMillis() - lastTime >= millis;
	}

	/**
	 * Statements to be executed after writing a batch goes here.
	 */
	@Override
	protected void resetParameters() {
		tupleList.clear();
		lastTime = System.currentTimeMillis();
	}
}
