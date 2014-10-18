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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Nothing;
import org.apache.flink.api.java.DataSet;


public class DataSink<T> {
	
	private final OutputFormat<T> format;
	
	private final TypeInformation<T> type;
	
	private final DataSet<T> data;
	
	private String name;
	
	private int dop = -1;
	
	public DataSink(DataSet<T> data, OutputFormat<T> format, TypeInformation<T> type) {
		if (format == null) {
			throw new IllegalArgumentException("The output format must not be null.");
		}
		if (type == null) {
			throw new IllegalArgumentException("The input type information must not be null.");
		}
		if (data == null) {
			throw new IllegalArgumentException("The data set must not be null.");
		}
		
		
		this.format = format;
		this.data = data;
		this.type = type;
	}

	
	public OutputFormat<T> getFormat() {
		return format;
	}
	
	public TypeInformation<T> getType() {
		return type;
	}
	
	public DataSet<T> getDataSet() {
		return data;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public DataSink<T> name(String name) {
		this.name = name;
		return this;
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected GenericDataSinkBase<T> translateToDataFlow(Operator<T> input) {
		// select the name (or create a default one)
		String name = this.name != null ? this.name : this.format.toString();
		GenericDataSinkBase<T> sink = new GenericDataSinkBase<T>(this.format, new UnaryOperatorInformation<T, Nothing>(this.type, new NothingTypeInfo()), name);
		// set input
		sink.setInput(input);
		// set dop
		if(this.dop > 0) {
			// use specified dop
			sink.setDegreeOfParallelism(this.dop);
		} else {
			// if no dop has been specified, use dop of input operator to enable chaining
			sink.setDegreeOfParallelism(input.getDegreeOfParallelism());
		}
		
		return sink;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "DataSink '" + (this.name == null ? "<unnamed>" : this.name) + "' (" + this.format.toString() + ")";
	}
	
	/**
	 * Returns the degree of parallelism of this data sink.
	 * 
	 * @return The degree of parallelism of this data sink.
	 */
	public int getParallelism() {
		return this.dop;
	}
	
	/**
	 * Sets the degree of parallelism for this data sink.
	 * The degree must be 1 or more.
	 * 
	 * @param dop The degree of parallelism for this data sink.
	 * @return This data sink with set degree of parallelism.
	 */
	public DataSink<T> setParallelism(int dop) {
		
		if(dop < 1) {
			throw new IllegalArgumentException("The parallelism of an operator must be at least 1.");
		}
		this.dop = dop;
		
		return this;
	}
}
