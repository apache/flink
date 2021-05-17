/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.NoOpOperator;

import java.io.IOException;

/**
 * This is dummy {@link NoOpOperator}, which context is {@link DummyExecutionEnvironment}.
 */
public class DummyNoOpOperator<IN> extends NoOpOperator<IN> {

	public DummyNoOpOperator(
			ExecutionEnvironment dummyExecEnv,
			DataSet<IN> input,
			TypeInformation<IN> resultType) {
		super(dummyExecEnv.createInput(new DummyInputFormat(), resultType), resultType);

		setInput(input);
	}

	/**
	 * Provides a {@link FileInputFormat} for {@link DummyNoOpOperator}.
	 * All the data it reads is NULL.
	 */
	public static class DummyInputFormat<IN> extends FileInputFormat<IN> {

		@Override
		public boolean reachedEnd() throws IOException {
			return false;
		}

		@Override
		public IN nextRecord(IN reuse) throws IOException {
			return null;
		}
	}
}
