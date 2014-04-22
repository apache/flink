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
import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.operators.translation.PlanDataSource;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 */
public class DataSource<OUT> extends DataSet<OUT> {
	
	private final InputFormat<OUT, ?> inputFormat;
	
	private String name;
	
	private int dop = -1;

	// --------------------------------------------------------------------------------------------
	
	public DataSource(ExecutionEnvironment context, InputFormat<OUT, ?> inputFormat, TypeInformation<OUT> type) {
		super(context, type);
		
		if (inputFormat == null)
			throw new IllegalArgumentException("The input format may not be null.");
		
		this.inputFormat = inputFormat;
	}

	public InputFormat<OUT, ?> getInputFormat() {
		return this.inputFormat;
	}
	
	// --------------------------------------------------------------------------------------------
	
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
	
	protected GenericDataSource<?> translateToDataFlow() {
		String name = this.name != null ? this.name : this.inputFormat.toString();
		if (name.length() > 100) {
			name = name.substring(0, 100);
		}
		
		PlanDataSource<OUT> source = new PlanDataSource<OUT>(this.inputFormat, name, getType());
		// set dop
		source.setDegreeOfParallelism(dop);
		
		return source;
	}
}
