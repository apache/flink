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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * An operation that creates a new data set (data source). The operation acts as the
 * data set on which to apply further transformations. It encapsulates additional
 * configuration parameters, to customize the execution.
 * 
 * @param <OUT> The type of the elements produced by this data source.
 */
public class DataSource<OUT> extends Operator<OUT, DataSource<OUT>> {
	
	private final InputFormat<OUT, ?> inputFormat;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new data source.
	 * 
	 * @param context The environment in which the data source gets executed.
	 * @param inputFormat The input format that the data source executes.
	 * @param type The type of the elements produced by this input format.
	 */
	public DataSource(ExecutionEnvironment context, InputFormat<OUT, ?> inputFormat, TypeInformation<OUT> type) {
		super(context, type);
		
		if (inputFormat == null) {
			throw new IllegalArgumentException("The input format may not be null.");
		}
		
		this.inputFormat = inputFormat;
		
		if (inputFormat instanceof NonParallelInput) {
			this.dop = 1;
		}
	}

	/**
	 * Gets the input format that is executed by this data source.
	 * 
	 * @return The input format that is executed by this data source.
	 */
	public InputFormat<OUT, ?> getInputFormat() {
		return this.inputFormat;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected GenericDataSourceBase<OUT, ?> translateToDataFlow() {
		String name = this.name != null ? this.name : this.inputFormat.toString();
		if (name.length() > 100) {
			name = name.substring(0, 100);
		}
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		GenericDataSourceBase<OUT, ?> source = new GenericDataSourceBase(this.inputFormat,
				new OperatorInformation<OUT>(getType()), name);
		source.setDegreeOfParallelism(dop);
		
		return source;
	}
}
