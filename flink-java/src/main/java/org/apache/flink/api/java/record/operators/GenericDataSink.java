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


package org.apache.flink.api.java.record.operators;

import com.google.common.base.Preconditions;

import java.util.List;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.api.java.typeutils.RecordTypeInfo;
import org.apache.flink.types.Nothing;
import org.apache.flink.types.Record;

/**
 * 
 * <b>NOTE: The Record API is marked as deprecated. It is not being developed anymore and will be removed from
 * the code at some point.
 * See <a href="https://issues.apache.org/jira/browse/FLINK-1106">FLINK-1106</a> for more details.</b>
 * 
 * Operator for nodes that act as data sinks, storing the data they receive.
 * The way the data is stored is handled by the {@link OutputFormat}.
 */

@Deprecated
public class GenericDataSink extends GenericDataSinkBase<Record> {

	private static String DEFAULT_NAME = "<Unnamed Generic Record Data Sink>";

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation 
	 * and the given name. 
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(OutputFormat<Record> f, String name) {
		super(f, new UnaryOperatorInformation<Record, Nothing>(new RecordTypeInfo(), new NothingTypeInfo()), name);
	}


	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation
	 * and a default name.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 */
	public GenericDataSink(OutputFormat<Record> f) {
		this(f, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given operator as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The operator to use as the input.
	 */
	public GenericDataSink(OutputFormat<Record> f, Operator<Record> input) {
		this(f, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contracts to use as the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public GenericDataSink(OutputFormat<Record> f, List<Operator<Record>> input) {
		this(f, input, DEFAULT_NAME);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given operator as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The operator to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(OutputFormat<Record> f, Operator<Record> input, String name) {
		this(f, name);
		setInput(input);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contracts to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public GenericDataSink(OutputFormat<Record> f, List<Operator<Record>> input, String name) {
		this(f, name);
		setInputs(input);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation 
	 * and the given name. 
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(Class<? extends OutputFormat<Record>> f, String name) {
		super(new UserCodeClassWrapper<OutputFormat<Record>>(f),
				new UnaryOperatorInformation<Record, Nothing>(new RecordTypeInfo(), new NothingTypeInfo()), name);
	}

	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation
	 * and a default name.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 */
	public GenericDataSink(Class<? extends OutputFormat<Record>> f) {
		this(f, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given operator as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The operator to use as the input.
	 */
	public GenericDataSink(Class<? extends OutputFormat<Record>> f, Operator<Record> input) {
		this(f, input, DEFAULT_NAME);
	}
	
	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation the default name.
	 * It uses the given contracts as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contracts to use as the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public GenericDataSink(Class<? extends OutputFormat<Record>> f, List<Operator<Record>> input) {
		this(f, input, DEFAULT_NAME);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given operator as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The operator to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 */
	public GenericDataSink(Class<? extends OutputFormat<Record>> f, Operator<Record> input, String name) {
		this(f, name);
		setInput(input);
	}

	/**
	 * Creates a GenericDataSink with the provided {@link OutputFormat} implementation and the given name.
	 * It uses the given contracts as its input.
	 * 
	 * @param f The {@link OutputFormat} implementation used to sink the data.
	 * @param input The contracts to use as the input.
	 * @param name The given name for the sink, used in plans, logs and progress messages.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public GenericDataSink(Class<? extends OutputFormat<Record>> f, List<Operator<Record>> input, String name) {
		this(f, name);
		setInputs(input);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the input to the union of the given operators.
	 * 
	 * @param inputs The operator(s) that form the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public void setInputs(Operator<Record>... inputs) {
		Preconditions.checkNotNull(inputs, "The inputs may not be null.");
		this.input = Operator.createUnionCascade(inputs);
	}
	
	/**
	 * Sets the input to the union of the given operators.
	 * 
	 * @param inputs The operator(s) that form the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public void setInputs(List<Operator<Record>> inputs) {
		Preconditions.checkNotNull(inputs, "The inputs may not be null.");
		this.input = Operator.createUnionCascade(inputs);
	}
	
	/**
	 * Adds to the input the union of the given operators.
	 * 
	 * @param inputs The operator(s) to be unioned with the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@Deprecated
	public void addInput(Operator<Record>... inputs) {
		Preconditions.checkNotNull(inputs, "The input may not be null.");
		this.input = Operator.createUnionCascade(this.input, inputs);
	}

	/**
	 * Adds to the input the union of the given operators.
	 * 
	 * @param inputs The operator(s) to be unioned with the input.
	 * @deprecated This method will be removed in future versions. Use the {@link org.apache.flink.api.common.operators.Union} operator instead.
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	public void addInputs(List<? extends Operator<Record>> inputs) {
		Preconditions.checkNotNull(inputs, "The inputs may not be null.");
		this.input = createUnionCascade(this.input, (Operator<Record>[]) inputs.toArray(new Operator[inputs.size()]));
	}
}
