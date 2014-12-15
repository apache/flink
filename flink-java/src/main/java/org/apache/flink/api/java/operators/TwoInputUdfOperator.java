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

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.DataSet;

/**
 * The <tt>TwoInputUdfOperator</tt> is the base class of all binary operators that execute
 * user-defined functions (UDFs). The UDFs encapsulated by this operator are naturally UDFs that
 * have two inputs (such as {@link org.apache.flink.api.common.functions.RichJoinFunction} or
 * {@link org.apache.flink.api.common.functions.RichCoGroupFunction}).
 * <p>
 * This class encapsulates utilities for the UDFs, such as broadcast variables, parameterization
 * through configuration objects, and semantic properties.
 *
 * @param <IN1> The data type of the first input data set.
 * @param <IN2> The data type of the second input data set.
 * @param <OUT> The data type of the returned data set.
 */
public abstract class TwoInputUdfOperator<IN1, IN2, OUT, O extends TwoInputUdfOperator<IN1, IN2, OUT, O>>
	extends TwoInputOperator<IN1, IN2, OUT, O> implements UdfOperator<O>
{
	private Configuration parameters;

	private Map<String, DataSet<?>> broadcastVariables;

	private DualInputSemanticProperties udfSemantics;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new operators with the two given data sets as inputs. The given result type
	 * describes the data type of the elements in the data set produced by the operator.
	 *
	 * @param input1 The data set for the first input.
	 * @param input2 The data set for the second input.
	 * @param resultType The type of the elements in the resulting data set.
	 */
	protected TwoInputUdfOperator(DataSet<IN1> input1, DataSet<IN2> input2, TypeInformation<OUT> resultType) {
		super(input1, input2, resultType);
	}
	
	protected void extractSemanticAnnotationsFromUdf(Class<?> udfClass) {
		Set<Annotation> annotations = FunctionAnnotation.readDualConstantAnnotations(udfClass);
		
		DualInputSemanticProperties dsp = SemanticPropUtil.getSemanticPropsDual(annotations,
					getInput1Type(), getInput2Type(), getResultType());

		setSemanticProperties(dsp);
	}
	
	protected void updateTypeDependentProperties() {
		// can be overwritten in subclasses for actions that depend on the return type
		// which may not be available immediately (e.g. if type is missing at instantiation)
	}

	// --------------------------------------------------------------------------------------------
	// Fluent API methods
	// --------------------------------------------------------------------------------------------

	@Override
	public O withParameters(Configuration parameters) {
		this.parameters = parameters;

		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}

	@Override
	public O withBroadcastSet(DataSet<?> data, String name) {
		if (data == null) {
			throw new IllegalArgumentException("Broadcast variable data must not be null.");
		}
		if (name == null) {
			throw new IllegalArgumentException("Broadcast variable name must not be null.");
		}
		
		if (this.broadcastVariables == null) {
			this.broadcastVariables = new HashMap<String, DataSet<?>>();
		}

		this.broadcastVariables.put(name, data);

		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}

	/**
	 * Adds a constant-set annotation for the first input of the UDF.
	 * 
	 * <p>
	 * Constant set annotations are used by the optimizer to infer the existence of data properties (sorted, partitioned, grouped).
	 * In certain cases, these annotations allow the optimizer to generate a more efficient execution plan which can lead to improved performance.
	 * Constant set annotations can only be specified if the first input and the output type of the UDF are of
	 * {@link org.apache.flink.api.java.tuple.Tuple} data types.
	 * 
	 * <p>
	 * A constant-set annotation is a set of constant field specifications. The constant field specification String "4->3" specifies, that this UDF copies the fourth field of 
	 * an input tuple to the third field of the output tuple. Field references are zero-indexed.
	 * 
	 * <p>
	 * <b>NOTICE: Constant set annotations are optional, but if given need to be correct. Otherwise, the program might produce wrong results!</b>
	 * 
	 * @param constantSetFirst A list of constant field specification Strings for the first input.
	 * @return This operator with an annotated constant field set for the first input.
	 */
	@SuppressWarnings("unchecked")
	public O withConstantSetFirst(String... constantSetFirst) {
		if (this.udfSemantics == null) {
			this.udfSemantics = new DualInputSemanticProperties();
		}
		
		SemanticPropUtil.getSemanticPropsDualFromString(this.udfSemantics, constantSetFirst, null,
				null, null, null, null, this.getInput1Type(), this.getInput2Type(), this.getResultType());

		O returnType = (O) this;
		return returnType;
	}
	
	/**
	 * Adds a constant-set annotation for the second input of the UDF.
	 * 
	 * <p>
	 * Constant set annotations are used by the optimizer to infer the existence of data properties (sorted, partitioned, grouped).
	 * In certain cases, these annotations allow the optimizer to generate a more efficient execution plan which can lead to improved performance.
	 * Constant set annotations can only be specified if the second input and the output type of the UDF are of
	 * {@link org.apache.flink.api.java.tuple.Tuple} data types.
	 * 
	 * <p>
	 * A constant-set annotation is a set of constant field specifications. The constant field specification String "4->3" specifies, that this UDF copies the fourth field of 
	 * an input tuple to the third field of the output tuple. Field references are zero-indexed.
	 * 
	 * <p>
	 * <b>NOTICE: Constant set annotations are optional, but if given need to be correct. Otherwise, the program might produce wrong results!</b>
	 * 
	 * @param constantSetSecond A list of constant field specification Strings for the second input.
	 * @return This operator with an annotated constant field set for the second input.
	 */
	@SuppressWarnings("unchecked")
	public O withConstantSetSecond(String... constantSetSecond) {
		if (this.udfSemantics == null) {
			this.udfSemantics = new DualInputSemanticProperties();
		}
		
		SemanticPropUtil.getSemanticPropsDualFromString(this.udfSemantics, null, constantSetSecond,
				null, null, null, null, this.getInput1Type(), this.getInput2Type(), this.getResultType());

		O returnType = (O) this;
		return returnType;
	}
	
	/**
	 * Adds a type information hint about the return type of this operator. 
	 * 
	 * <p>
	 * Type hints are important in cases where the Java compiler
	 * throws away generic type information necessary for efficient execution.
	 * 
	 * <p>
	 * This method takes a type information string that will be parsed. A type information string can contain the following
	 * types:
	 *
	 * <ul>
	 * <li>Basic types such as <code>Integer</code>, <code>String</code>, etc.
	 * <li>Basic type arrays such as <code>Integer[]</code>,
	 * <code>String[]</code>, etc.
	 * <li>Tuple types such as <code>Tuple1&lt;TYPE0&gt;</code>,
	 * <code>Tuple2&lt;TYPE0, TYPE1&gt;</code>, etc.</li>
	 * <li>Pojo types such as <code>org.my.MyPojo&lt;myFieldName=TYPE0,myFieldName2=TYPE1&gt;</code>, etc.</li>
	 * <li>Generic types such as <code>java.lang.Class</code>, etc.
	 * <li>Custom type arrays such as <code>org.my.CustomClass[]</code>,
	 * <code>org.my.CustomClass$StaticInnerClass[]</code>, etc.
	 * <li>Value types such as <code>DoubleValue</code>,
	 * <code>StringValue</code>, <code>IntegerValue</code>, etc.</li>
	 * <li>Tuple array types such as <code>Tuple2&lt;TYPE0,TYPE1&gt;[], etc.</code></li>
	 * <li>Writable types such as <code>Writable&lt;org.my.CustomWritable&gt;</code></li>
	 * <li>Enum types such as <code>Enum&lt;org.my.CustomEnum&gt;</code></li>
	 * </ul>
	 *
	 * Example:
	 * <code>"Tuple2&lt;String,Tuple2&lt;Integer,org.my.MyJob$Pojo&lt;word=String&gt;&gt;&gt;"</code>
	 *
	 * @param typeInfoString
	 *            type information string to be parsed
	 * @return This operator with a given return type hint.
	 */
	public O returns(String typeInfoString) {
		if (typeInfoString == null) {
			throw new IllegalArgumentException("Type information string must not be null.");
		}
		return returns(TypeInfoParser.<OUT>parse(typeInfoString));
	}
	
	/**
	 * Adds a type information hint about the return type of this operator. 
	 * 
	 * <p>
	 * Type hints are important in cases where the Java compiler
	 * throws away generic type information necessary for efficient execution.
	 * 
	 * <p>
	 * This method takes an instance of {@link org.apache.flink.api.common.typeinfo.TypeInformation} such as:
	 * 
	 * <ul>
	 * <li>{@link org.apache.flink.api.common.typeinfo.BasicTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.java.typeutils.TupleTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.java.typeutils.PojoTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.java.typeutils.WritableTypeInfo}</li>
	 * <li>{@link org.apache.flink.api.java.typeutils.ValueTypeInfo}</li>
	 * <li>etc.</li>
	 * </ul>
	 *
	 * @param typeInfo
	 *            type information as a return type hint
	 * @return This operator with a given return type hint.
	 */
	public O returns(TypeInformation<OUT> typeInfo) {
		if (typeInfo == null) {
			throw new IllegalArgumentException("Type information must not be null.");
		}
		fillInType(typeInfo);
		updateTypeDependentProperties();
		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}
	
	/**
	 * Adds a type information hint about the return type of this operator. 
	 * 
	 * <p>
	 * Type hints are important in cases where the Java compiler
	 * throws away generic type information necessary for efficient execution.
	 * 
	 * <p>
	 * This method takes a class that will be analyzed by Flink's type extraction capabilities.
	 * 
	 * <p>
	 * Examples for classes are:
	 * <ul>
	 * <li>Basic types such as <code>Integer.class</code>, <code>String.class</code>, etc.</li>
	 * <li>POJOs such as <code>MyPojo.class</code></li>
	 * <li>Classes that <b>extend</b> tuples. Classes like <code>Tuple1.class</code>,<code>Tuple2.class</code>, etc. are <b>not</b> sufficient.</li>
	 * <li>Arrays such as <code>String[].class</code>, etc.</li>
	 * </ul>
	 *
	 * @param typeClass
	 *            class as a return type hint
	 * @return This operator with a given return type hint.
	 */
	@SuppressWarnings("unchecked")
	public O returns(Class<OUT> typeClass) {
		if (typeClass == null) {
			throw new IllegalArgumentException("Type class must not be null.");
		}
		
		try {
			TypeInformation<OUT> ti = (TypeInformation<OUT>) TypeExtractor.createTypeInfo(typeClass);
			return returns(ti);
		}
		catch (InvalidTypesException e) {
			throw new InvalidTypesException("The given class is not suited for providing necessary type information.", e);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Accessors
	// --------------------------------------------------------------------------------------------

	@Override
	public Map<String, DataSet<?>> getBroadcastSets() {
		return this.broadcastVariables == null ?
				Collections.<String, DataSet<?>>emptyMap() :
				Collections.unmodifiableMap(this.broadcastVariables);
	}

	@Override
	public Configuration getParameters() {
		return this.parameters;
	}

	@Override
	public DualInputSemanticProperties getSemanticProperties() {
		return this.udfSemantics;
	}

	/**
	 * Sets the semantic properties for the user-defined function (UDF). The semantic properties
	 * define how fields of tuples and other objects are modified or preserved through this UDF.
	 * The configured properties can be retrieved via {@link UdfOperator#getSemanticProperties()}.
	 *
	 * @param properties The semantic properties for the UDF.
	 * @see UdfOperator#getSemanticProperties()
	 */
	public void setSemanticProperties(DualInputSemanticProperties properties) {
		this.udfSemantics = properties;
	}
}
