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

import org.apache.flink.annotation.PublicEvolving;

/**
 * Implementation of WriteSinkFunction. Writes tuples to file in every millis
 * milliseconds.
 *
 * @param <IN>
 *            Input tuple type
 *
 * @deprecated Please use the {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink StreamingFileSink}
 * for writing to files from a streaming program.
 */
@PublicEvolving
@Deprecated
public class WriteSinkFunctionByMillis<IN> extends WriteSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private final long millis;
	private long lastTime;

	public WriteSinkFunctionByMillis(String path, WriteFormat<IN> format, long millis) {
		super(path, format);
		this.millis = millis;
		lastTime = System.currentTimeMillis();
	}

	@Override
	protected boolean updateCondition() {
		return System.currentTimeMillis() - lastTime >= millis;
	}

	@Override
	protected void resetParameters() {
		tupleList.clear();
		lastTime = System.currentTimeMillis();
	}

}
