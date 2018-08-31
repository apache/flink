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
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.configuration.Configuration;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The <tt>SingleInputUdfOperator</tt> is the base class of all unary operators that execute
 * user-defined functions (UDFs). The UDFs encapsulated by this operator are naturally UDFs that
 * have one input (such as {@link org.apache.flink.api.common.functions.RichMapFunction} or
 * {@link org.apache.flink.api.common.functions.RichReduceFunction}).
 *
 * <p>This class encapsulates utilities for the UDFs, such as broadcast variables, parameterization
 * through configuration objects, and semantic properties.
 * @param <IN> The data type of the input data set.
 * @param <OUT> The data type of the returned data set.
 */
@Public
public abstract class SingleInputUdfOperator<IN, OUT, O extends SingleInputUdfOperator<IN, OUT, O>>
	extends SingleInputOperator<IN, OUT, O> implements UdfOperator<O> {
	private Configuration parameters;

	private Map<String, DataSet<?>> broadcastVariables;

	// NOTE: only set this variable via setSemanticProperties()
	private SingleInputSemanticProperties udfSemantics;

	private boolean analyzedUdfSemantics;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new operators with the given data set as input. The given result type
	 * describes the data type of the elements in the data set produced by the operator.
	 *
	 * @param input The data set that is the input to the operator.
	 * @param resultType The type of the elements in the resulting data set.
	 */
	protected SingleInputUdfOperator(DataSet<IN> input, TypeInformation<OUT> resultType) {
		super(input, resultType);
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
	 * Adds semantic information about forwarded fields of the user-defined function.
	 * The forwarded fields information declares fields which are never modified by the function and
	 * which are forwarded at the same position to the output or unchanged copied to another position in the output.
	 *
	 * <p>Fields that are forwarded at the same position are specified by their position.
	 * The specified position must be valid for the input and output data type and have the same type.
	 * For example <code>withForwardedFields("f2")</code> declares that the third field of a Java input tuple is
	 * copied to the third field of an output tuple.
	 *
	 * <p>Fields which are unchanged copied to another position in the output are declared by specifying the
	 * source field reference in the input and the target field reference in the output.
	 * {@code withForwardedFields("f0->f2")} denotes that the first field of the Java input tuple is
	 * unchanged copied to the third field of the Java output tuple. When using a wildcard ("*") ensure that
	 * the number of declared fields and their types in input and output type match.
	 *
	 * <p>Multiple forwarded fields can be annotated in one ({@code withForwardedFields("f2; f3->f0; f4")})
	 * or separate Strings ({@code withForwardedFields("f2", "f3->f0", "f4")}).
	 * Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field references such as nested fields and wildcard.
	 *
	 * <p>It is not possible to override existing semantic information about forwarded fields which was
	 * for example added by a {@link org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields} class annotation.
	 *
	 * <p><b>NOTE: Adding semantic information for functions is optional!
	 * If used correctly, semantic information can help the Flink optimizer to generate more efficient execution plans.
	 * However, incorrect semantic information can cause the optimizer to generate incorrect execution plans which compute wrong results!
	 * So be careful when adding semantic information.
	 * </b>
	 *
	 * @param forwardedFields A list of field forward expressions.
	 * @return This operator with annotated forwarded field information.
	 *
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
	 */
	public O withForwardedFields(String... forwardedFields) {

		if (this.udfSemantics == null) {
			// extract semantic properties from function annotations
			setSemanticProperties(extractSemanticAnnotations(getFunction().getClass()));
		}

		if (this.udfSemantics == null
				|| this.analyzedUdfSemantics) { // discard analyzed semantic properties
			setSemanticProperties(new SingleInputSemanticProperties());
			SemanticPropUtil.getSemanticPropsSingleFromString(this.udfSemantics, forwardedFields, null, null, this.getInputType(), this.getResultType());
		} else {
			if (udfWithForwardedFieldsAnnotation(getFunction().getClass())) {
				// refuse semantic information as it would override the function annotation
				throw new SemanticProperties.InvalidSemanticAnnotationException("Forwarded field information " +
						"has already been added by a function annotation for this operator. " +
						"Cannot overwrite function annotations.");
			} else {
				SemanticPropUtil.getSemanticPropsSingleFromString(this.udfSemantics, forwardedFields, null, null, this.getInputType(), this.getResultType());
			}
		}

		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}

	// ------------------------------------------------------------------------
	//  type hinting
	// ------------------------------------------------------------------------

	/**
	 * Adds a type information hint about the return type of this operator. This method
	 * can be used in cases where Flink cannot determine automatically what the produced
	 * type of a function is. That can be the case if the function uses generic type variables
	 * in the return type that cannot be inferred from the input type.
	 *
	 * <p>Classes can be used as type hints for non-generic types (classes without generic parameters),
	 * but not for generic types like for example Tuples. For those generic types, please
	 * use the {@link #returns(TypeHint)} method.
	 *
	 * <p>Use this method the following way:
	 * <pre>{@code
	 *     DataSet<String[]> result =
	 *         data.flatMap(new FunctionWithNonInferrableReturnType())
	 *             .returns(String[].class);
	 * }</pre>
	 *
	 * @param typeClass The class of the returned data type.
	 * @return This operator with the type information corresponding to the given type class.
	 */
	public O returns(Class<OUT> typeClass) {
		requireNonNull(typeClass, "type class must not be null");

		try {
			return returns(TypeInformation.of(typeClass));
		}
		catch (InvalidTypesException e) {
			throw new InvalidTypesException("Cannot infer the type information from the class alone." +
					"This is most likely because the class represents a generic type. In that case," +
					"please use the 'returns(TypeHint)' method instead.", e);
		}
	}

	/**
	 * Adds a type information hint about the return type of this operator. This method
	 * can be used in cases where Flink cannot determine automatically what the produced
	 * type of a function is. That can be the case if the function uses generic type variables
	 * in the return type that cannot be inferred from the input type.
	 *
	 * <p>Use this method the following way:
	 * <pre>{@code
	 *     DataSet<Tuple2<String, Double>> result =
	 *         data.flatMap(new FunctionWithNonInferrableReturnType())
	 *             .returns(new TypeHint<Tuple2<String, Double>>(){});
	 * }</pre>
	 *
	 * @param typeHint The type hint for the returned data type.
	 * @return This operator with the type information corresponding to the given type hint.
	 */
	public O returns(TypeHint<OUT> typeHint) {
		requireNonNull(typeHint, "TypeHint must not be null");

		try {
			return returns(TypeInformation.of(typeHint));
		}
		catch (InvalidTypesException e) {
			throw new InvalidTypesException("Cannot infer the type information from the type hint. " +
					"Make sure that the TypeHint does not use any generic type variables.");
		}
	}

	/**
	 * Adds a type information hint about the return type of this operator. This method
	 * can be used in cases where Flink cannot determine automatically what the produced
	 * type of a function is. That can be the case if the function uses generic type variables
	 * in the return type that cannot be inferred from the input type.
	 *
	 * <p>In most cases, the methods {@link #returns(Class)} and {@link #returns(TypeHint)}
	 * are preferable.
	 *
	 * @param typeInfo The type information for the returned data type.
	 * @return This operator using the given type information for the return type.
	 */
	public O returns(TypeInformation<OUT> typeInfo) {
		requireNonNull(typeInfo, "TypeInformation must not be null");

		fillInType(typeInfo);
		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}

	// --------------------------------------------------------------------------------------------
	// Accessors
	// --------------------------------------------------------------------------------------------

	@Override
	@Internal
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
	@Internal
	public SingleInputSemanticProperties getSemanticProperties() {
		if (this.udfSemantics == null || analyzedUdfSemantics) {
			SingleInputSemanticProperties props = extractSemanticAnnotations(getFunction().getClass());
			if (props != null) {
				setSemanticProperties(props);
			}
		}
		if (this.udfSemantics == null) {
			setSemanticProperties(new SingleInputSemanticProperties());
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
	@Internal
	public void setSemanticProperties(SingleInputSemanticProperties properties) {
		this.udfSemantics = properties;
		this.analyzedUdfSemantics = false;
	}

	protected boolean getAnalyzedUdfSemanticsFlag() {
		return this.analyzedUdfSemantics;
	}

	protected void setAnalyzedUdfSemanticsFlag() {
		this.analyzedUdfSemantics = true;
	}

	protected SingleInputSemanticProperties extractSemanticAnnotations(Class<?> udfClass) {
		Set<Annotation> annotations = FunctionAnnotation.readSingleForwardAnnotations(udfClass);
		return SemanticPropUtil.getSemanticPropsSingle(annotations, getInputType(), getResultType());
	}

	protected boolean udfWithForwardedFieldsAnnotation(Class<?> udfClass) {
		return udfClass.getAnnotation(FunctionAnnotation.ForwardedFields.class) != null ||
				udfClass.getAnnotation(FunctionAnnotation.NonForwardedFields.class) != null;
	}
}
