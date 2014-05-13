/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.operators.OperatorInformation;
import eu.stratosphere.api.common.operators.base.GenericDataSourceBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.types.TypeInformation;

/**
 * An operation that creates a new data set (data source). The operation acts as the
 * data set on which to apply further transformations. It encapsulates additional
 * configuration parameters, to customize the execution.
 * 
 * @param <OUT> The type of the elements produced by this data source.
 */
public class DataSource<OUT> extends DataSet<OUT> {
	
	private final InputFormat<OUT, ?> inputFormat;
	
	private String name;
	
	private int dop = -1;

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
	
	/**
	 * Sets the name of the data source operation. The name will be used for logging and other
	 * messages. The default name is a textual representation of the input format.
	 * 
	 * @param name The name for the data source.
	 * @return The data source object itself, to allow for function call chaining.
	 */
	public DataSource<OUT> name(String name) {
		this.name = name;
		return this;
	}
	
	/**
	 * Returns the degree of parallelism of this data source.
	 * 
	 * @return The degree of parallelism of this data source.
	 */
	public int getParallelism() {
		return this.dop;
	}
	
	/**
	 * Sets the degree of parallelism for this data source.
	 * The degree must be 1 or more.
	 * 
	 * @param dop The degree of parallelism for this data source.
	 * @return This data source with set degree of parallelism.
	 */
	public DataSource<OUT> setParallelism(int dop) {
		if(dop < 1) {
			throw new IllegalArgumentException("The parallelism of an operator must be at least 1.");
		}
		this.dop = dop;
		
		return this;
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
