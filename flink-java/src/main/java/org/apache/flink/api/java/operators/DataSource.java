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
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.SplitDataProperties;
import org.apache.flink.configuration.Configuration;

/**
 * An operation that creates a new data set (data source). The operation acts as the
 * data set on which to apply further transformations. It encapsulates additional
 * configuration parameters, to customize the execution.
 *
 * @param <OUT> The type of the elements produced by this data source.
 */
@Public
public class DataSource<OUT> extends Operator<OUT, DataSource<OUT>> {

	private final InputFormat<OUT, ?> inputFormat;

	private final String dataSourceLocationName;

	private Configuration parameters;

	private SplitDataProperties<OUT> splitDataProperties;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new data source.
	 *
	 * @param context The environment in which the data source gets executed.
	 * @param inputFormat The input format that the data source executes.
	 * @param type The type of the elements produced by this input format.
	 */
	public DataSource(ExecutionEnvironment context, InputFormat<OUT, ?> inputFormat, TypeInformation<OUT> type, String dataSourceLocationName) {
		super(context, type);

		this.dataSourceLocationName = dataSourceLocationName;

		if (inputFormat == null) {
			throw new IllegalArgumentException("The input format may not be null.");
		}

		this.inputFormat = inputFormat;

		if (inputFormat instanceof NonParallelInput) {
			this.parallelism = 1;
		}
	}

	/**
	 * Gets the input format that is executed by this data source.
	 *
	 * @return The input format that is executed by this data source.
	 */
	@Internal
	public InputFormat<OUT, ?> getInputFormat() {
		return this.inputFormat;
	}

	/**
	 * Pass a configuration to the InputFormat.
	 * @param parameters Configuration parameters
	 */
	public DataSource<OUT> withParameters(Configuration parameters) {
		this.parameters = parameters;
		return this;
	}

	/**
	 * @return Configuration for the InputFormat.
	 */
	public Configuration getParameters() {
		return this.parameters;
	}

	/**
	 * Returns the {@link org.apache.flink.api.java.io.SplitDataProperties} for the
	 * {@link org.apache.flink.core.io.InputSplit}s of this DataSource
	 * for configurations.
	 *
	 * <p>SplitDataProperties can help to generate more efficient execution plans.
	 *
	 *
	 * <p><b>
	 *     IMPORTANT: Incorrect configuration of SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @return The SplitDataProperties for the InputSplits of this DataSource.
	 */
	@PublicEvolving
	public SplitDataProperties<OUT> getSplitDataProperties() {
		if (this.splitDataProperties == null) {
			this.splitDataProperties = new SplitDataProperties<OUT>(this);
		}
		return this.splitDataProperties;
	}

	// --------------------------------------------------------------------------------------------

	protected GenericDataSourceBase<OUT, ?> translateToDataFlow() {
		String name = this.name != null ? this.name : "at " + dataSourceLocationName + " (" + inputFormat.getClass().getName() + ")";
		if (name.length() > 150) {
			name = name.substring(0, 150);
		}

		@SuppressWarnings({"unchecked", "rawtypes"})
		GenericDataSourceBase<OUT, ?> source = new GenericDataSourceBase(this.inputFormat,
			new OperatorInformation<OUT>(getType()), name);
		source.setParallelism(parallelism);
		if (this.parameters != null) {
			source.getParameters().addAll(this.parameters);
		}
		if (this.splitDataProperties != null) {
			source.setSplitDataProperties(this.splitDataProperties);
		}
		return source;
	}

}
