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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;

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

	// NOTE: only set this variable via setSemanticProperties()
	private DualInputSemanticProperties udfSemantics;

	private boolean analyzedUdfSemantics;

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
	
	protected abstract Function getFunction();

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
	 * <p>
	 * Adds semantic information about forwarded fields of the first input of the user-defined function.
	 * The forwarded fields information declares fields which are never modified by the function and
	 * which are forwarded at the same position to the output or unchanged copied to another position in the output.
	 * </p>
	 *
	 * <p>
	 * Fields that are forwarded at the same position are specified by their position.
	 * The specified position must be valid for the input and output data type and have the same type.
	 * For example <code>withForwardedFieldsFirst("f2")</code> declares that the third field of a Java input tuple
	 * from the first input is copied to the third field of an output tuple.
	 * </p>
	 *
	 * <p>
	 * Fields which are unchanged copied from the first input to another position in the output are declared
	 * by specifying the source field reference in the first input and the target field reference in the output.
	 * {@code withForwardedFieldsFirst("f0->f2")} denotes that the first field of the first input Java tuple is
	 * unchanged copied to the third field of the Java output tuple. When using a wildcard ("*") ensure that
	 * the number of declared fields and their types in first input and output type match.
	 * </p>
	 *
	 * <p>
	 * Multiple forwarded fields can be annotated in one ({@code withForwardedFieldsFirst("f2; f3->f0; f4")})
	 * or separate Strings ({@code withForwardedFieldsFirst("f2", "f3->f0", "f4")}).
	 * Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field references such as nested fields and wildcard.
	 * </p>
	 *
	 * <p>
	 * It is not possible to override existing semantic information about forwarded fields of the first input which was
	 * for example added by a {@link org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst} class annotation.
	 * </p>
	 *
	 * <p>
	 * <b>NOTE: Adding semantic information for functions is optional!
	 * If used correctly, semantic information can help the Flink optimizer to generate more efficient execution plans.
	 * However, incorrect semantic information can cause the optimizer to generate incorrect execution plans which compute wrong results!
	 * So be careful when adding semantic information.
	 * </b>
	 * </p>
	 *
	 * @param forwardedFieldsFirst A list of forwarded field expressions for the first input of the function.
	 * @return This operator with annotated forwarded field information.
	 *
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst
	 */
	@SuppressWarnings("unchecked")
	public O withForwardedFieldsFirst(String... forwardedFieldsFirst) {
		if (this.udfSemantics == null || this.analyzedUdfSemantics) {
			// extract semantic properties from function annotations
			setSemanticProperties(extractSemanticAnnotationsFromUdf(getFunction().getClass()));
		}

		if(this.udfSemantics == null || this.analyzedUdfSemantics) {
			setSemanticProperties(new DualInputSemanticProperties());
			SemanticPropUtil.getSemanticPropsDualFromString(this.udfSemantics, forwardedFieldsFirst, null,
					null, null, null, null, getInput1Type(), getInput2Type(), getResultType());
		} else {
			if(this.udfWithForwardedFieldsFirstAnnotation(getFunction().getClass())) {
				// refuse semantic information as it would override the function annotation
				throw new SemanticProperties.InvalidSemanticAnnotationException("Forwarded field information " +
						"has already been added by a function annotation for the first input of this operator. " +
						"Cannot overwrite function annotations.");
			} else {
				SemanticPropUtil.getSemanticPropsDualFromString(this.udfSemantics, forwardedFieldsFirst, null,
						null, null, null, null, getInput1Type(), getInput2Type(), getResultType());
			}
		}

		O returnType = (O) this;
		return returnType;
	}

	/**
	 * <p>
	 * Adds semantic information about forwarded fields of the second input of the user-defined function.
	 * The forwarded fields information declares fields which are never modified by the function and
	 * which are forwarded at the same position to the output or unchanged copied to another position in the output.
	 * </p>
	 *
	 * <p>
	 * Fields that are forwarded at the same position are specified by their position.
	 * The specified position must be valid for the input and output data type and have the same type.
	 * For example <code>withForwardedFieldsSecond("f2")</code> declares that the third field of a Java input tuple
	 * from the second input is copied to the third field of an output tuple.
	 * </p>
	 *
	 * <p>
	 * Fields which are unchanged copied from the second input to another position in the output are declared
	 * by specifying the source field reference in the second input and the target field reference in the output.
	 * {@code withForwardedFieldsSecond("f0->f2")} denotes that the first field of the second input Java tuple is
	 * unchanged copied to the third field of the Java output tuple. When using a wildcard ("*") ensure that
	 * the number of declared fields and their types in second input and output type match.
	 * </p>
	 *
	 * <p>
	 * Multiple forwarded fields can be annotated in one ({@code withForwardedFieldsSecond("f2; f3->f0; f4")})
	 * or separate Strings ({@code withForwardedFieldsSecond("f2", "f3->f0", "f4")}).
	 * Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field references such as nested fields and wildcard.
	 * </p>
	 *
	 * <p>
	 * It is not possible to override existing semantic information about forwarded fields of the second input which was
	 * for example added by a {@link org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond} class annotation.
	 * </p>
	 *
	 * <p>
	 * <b>NOTE: Adding semantic information for functions is optional!
	 * If used correctly, semantic information can help the Flink optimizer to generate more efficient execution plans.
	 * However, incorrect semantic information can cause the optimizer to generate incorrect execution plans which compute wrong results!
	 * So be careful when adding semantic information.
	 * </b>
	 * </p>
	 *
	 * @param forwardedFieldsSecond A list of forwarded field expressions for the second input of the function.
	 * @return This operator with annotated forwarded field information.
	 *
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond
	 */
	@SuppressWarnings("unchecked")
	public O withForwardedFieldsSecond(String... forwardedFieldsSecond) {
		if (this.udfSemantics == null || this.analyzedUdfSemantics) {
			// extract semantic properties from function annotations
			setSemanticProperties(extractSemanticAnnotationsFromUdf(getFunction().getClass()));
		}

		if(this.udfSemantics == null || this.analyzedUdfSemantics) {
			setSemanticProperties(new DualInputSemanticProperties());
			SemanticPropUtil.getSemanticPropsDualFromString(this.udfSemantics, null, forwardedFieldsSecond,
					null, null, null, null, getInput1Type(), getInput2Type(), getResultType());
		} else {
			if(udfWithForwardedFieldsSecondAnnotation(getFunction().getClass())) {
				// refuse semantic information as it would override the function annotation
				throw new SemanticProperties.InvalidSemanticAnnotationException("Forwarded field information " +
						"has already been added by a function annotation for the second input of this operator. " +
						"Cannot overwrite function annotations.");
			} else {
				SemanticPropUtil.getSemanticPropsDualFromString(this.udfSemantics, null, forwardedFieldsSecond,
						null, null, null, null, getInput1Type(), getInput2Type(), getResultType());
			}
		}

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
		if (this.udfSemantics == null || analyzedUdfSemantics) {
			DualInputSemanticProperties props = extractSemanticAnnotationsFromUdf(getFunction().getClass());
			if (props != null) {
				setSemanticProperties(props);
			}
		}
		if (this.udfSemantics == null) {
			setSemanticProperties(new DualInputSemanticProperties());
		}
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
		this.analyzedUdfSemantics = false;
	}

	protected boolean getAnalyzedUdfSemanticsFlag() {
		return this.analyzedUdfSemantics;
	}

	protected void setAnalyzedUdfSemanticsFlag() {
		this.analyzedUdfSemantics = true;
	}

	protected DualInputSemanticProperties extractSemanticAnnotationsFromUdf(Class<?> udfClass) {
		Set<Annotation> annotations = FunctionAnnotation.readDualForwardAnnotations(udfClass);
		return SemanticPropUtil.getSemanticPropsDual(annotations, getInput1Type(), getInput2Type(), getResultType());
	}

	protected boolean udfWithForwardedFieldsFirstAnnotation(Class<?> udfClass) {

		if (udfClass.getAnnotation(FunctionAnnotation.ForwardedFieldsFirst.class) != null ||
				udfClass.getAnnotation(FunctionAnnotation.NonForwardedFieldsFirst.class) != null) {
			return true;
		} else {
			return false;
		}
	}

	protected boolean udfWithForwardedFieldsSecondAnnotation(Class<?> udfClass) {

		if (udfClass.getAnnotation(FunctionAnnotation.ForwardedFieldsSecond.class) != null ||
				udfClass.getAnnotation(FunctionAnnotation.NonForwardedFieldsSecond.class) != null) {
			return true;
		} else {
			return false;
		}
	}

}
