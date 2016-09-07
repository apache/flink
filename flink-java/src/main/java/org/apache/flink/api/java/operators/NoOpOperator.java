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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Preconditions;

/**
 * This operator will be ignored during translation.
 *
 * @param <IN> The type of the data set passed through the operator.
 */
@Internal
public class NoOpOperator<IN> extends DataSet<IN> {

	private DataSet<IN> input;

	public NoOpOperator(DataSet<IN> input, TypeInformation<IN> resultType) {
		super(input.getExecutionEnvironment(), resultType);

		this.input = input;
	}

	public DataSet<IN> getInput() {
		return input;
	}

	public void setInput(DataSet<IN> input) {
		Preconditions.checkNotNull(input);

		this.input = input;
	}
}
