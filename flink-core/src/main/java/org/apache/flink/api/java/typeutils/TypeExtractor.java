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

package org.apache.flink.api.java.typeutils;

import org.apache.commons.lang3.ClassUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils.LambdaExecutable;
import org.apache.flink.types.Row;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.hasSuperclass;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.checkAndExtractLambda;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getAllDeclaredMethods;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.sameTypeVars;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * A utility for reflection analysis on classes, to determine the return type of implementations of transformation
 * functions.
 */
@Public
public class TypeExtractor {

	/*
	 * NOTE: Most methods of the TypeExtractor work with a so-called "typeHierarchy".
	 * The type hierarchy describes all types (Classes, ParameterizedTypes, TypeVariables etc. ) and intermediate
	 * types from a given type of a function or type (e.g. MyMapper, Tuple2) until a current type
	 * (depends on the method, e.g. MyPojoFieldType).
	 *
	 * Thus, it fully qualifies types until tuple/POJO field level.
	 *
	 * A typical typeHierarchy could look like:
	 *
	 * UDF: MyMapFunction.class
	 * top-level UDF: MyMapFunctionBase.class
	 * RichMapFunction: RichMapFunction.class
	 * MapFunction: MapFunction.class
	 * Function's OUT: Tuple1<MyPojo>
	 * user-defined POJO: MyPojo.class
	 * user-defined top-level POJO: MyPojoBase.class
	 * POJO field: Tuple1<String>
	 * Field type: String.class
	 *
	 */

	/** The name of the class representing Hadoop's writable */
	private static final String HADOOP_WRITABLE_CLASS = "org.apache.hadoop.io.Writable";

	private static final String HADOOP_WRITABLE_TYPEINFO_CLASS = "org.apache.flink.api.java.typeutils.WritableTypeInfo";

	private static final String AVRO_SPECIFIC_RECORD_BASE_CLASS = "org.apache.avro.specific.SpecificRecordBase";

	private static final Logger LOG = LoggerFactory.getLogger(TypeExtractor.class);

	public static final int[] NO_INDEX = new int[] {};

	protected TypeExtractor() {
		// only create instances for special use cases
	}

	// --------------------------------------------------------------------------------------------
	//  TypeInfoFactory registry
	// --------------------------------------------------------------------------------------------

	private static Map<Type, Class<? extends TypeInfoFactory>> registeredTypeInfoFactories = new HashMap<>();

	/**
	 * Registers a type information factory globally for a certain type. Every following type extraction
	 * operation will use the provided factory for this type. The factory will have highest precedence
	 * for this type. In a hierarchy of types the registered factory has higher precedence than annotations
	 * at the same level but lower precedence than factories defined down the hierarchy.
	 *
	 * @param t type for which a new factory is registered
	 * @param factory type information factory that will produce {@link TypeInformation}
	 */
	private static void registerFactory(Type t, Class<? extends TypeInfoFactory> factory) {
		Preconditions.checkNotNull(t, "Type parameter must not be null.");
		Preconditions.checkNotNull(factory, "Factory parameter must not be null.");

		if (!TypeInfoFactory.class.isAssignableFrom(factory)) {
			throw new IllegalArgumentException("Class is not a TypeInfoFactory.");
		}
		if (registeredTypeInfoFactories.containsKey(t)) {
			throw new InvalidTypesException("A TypeInfoFactory for type '" + t + "' is already registered.");
		}
		registeredTypeInfoFactories.put(t, factory);
	}

	// --------------------------------------------------------------------------------------------
	//  Function specific methods
	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapInterface, TypeInformation<IN> inType) {
		return getMapReturnTypes(mapInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) mapInterface,
			MapFunction.class,
			0,
			1,
			new int[]{0},
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}


	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapInterface, TypeInformation<IN> inType) {
		return getFlatMapReturnTypes(flatMapInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) flatMapInterface,
			FlatMapFunction.class,
			0,
			1,
			new int[]{0},
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	/**
	 * @deprecated will be removed in a future version
	 */
	@PublicEvolving
	@Deprecated
	public static <IN, OUT> TypeInformation<OUT> getFoldReturnTypes(FoldFunction<IN, OUT> foldInterface, TypeInformation<IN> inType)
	{
		return getFoldReturnTypes(foldInterface, inType, null, false);
	}

	/**
	 * @deprecated will be removed in a future version
	 */
	@PublicEvolving
	@Deprecated
	public static <IN, OUT> TypeInformation<OUT> getFoldReturnTypes(FoldFunction<IN, OUT> foldInterface, TypeInformation<IN> inType, String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) foldInterface,
			FoldFunction.class,
			0,
			1,
			new int[]{1},
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, ACC> TypeInformation<ACC> getAggregateFunctionAccumulatorType(
			AggregateFunction<IN, ACC, ?> function,
			TypeInformation<IN> inType,
			String functionName,
			boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			function,
			AggregateFunction.class,
			0,
			1,
			new int[]{0},
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getAggregateFunctionReturnType(
			AggregateFunction<IN, ?, OUT> function,
			TypeInformation<IN> inType,
			String functionName,
			boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			function,
			AggregateFunction.class,
			0,
			2,
			NO_INDEX,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapPartitionReturnTypes(MapPartitionFunction<IN, OUT> mapPartitionInterface, TypeInformation<IN> inType) {
		return getMapPartitionReturnTypes(mapPartitionInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapPartitionReturnTypes(MapPartitionFunction<IN, OUT> mapPartitionInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) mapPartitionInterface,
			MapPartitionFunction.class,
			0,
			1,
			new int[]{0, 0},
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(GroupReduceFunction<IN, OUT> groupReduceInterface, TypeInformation<IN> inType) {
		return getGroupReduceReturnTypes(groupReduceInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(GroupReduceFunction<IN, OUT> groupReduceInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) groupReduceInterface,
			GroupReduceFunction.class,
			0,
			1,
			new int[]{0, 0},
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupCombineReturnTypes(GroupCombineFunction<IN, OUT> combineInterface, TypeInformation<IN> inType) {
		return getGroupCombineReturnTypes(combineInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupCombineReturnTypes(GroupCombineFunction<IN, OUT> combineInterface, TypeInformation<IN> inType,
																			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) combineInterface,
			GroupCombineFunction.class,
			0,
			1,
			new int[]{0, 0},
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getFlatJoinReturnTypes(FlatJoinFunction<IN1, IN2, OUT> joinInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type)
	{
		return getFlatJoinReturnTypes(joinInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getFlatJoinReturnTypes(FlatJoinFunction<IN1, IN2, OUT> joinInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, String functionName, boolean allowMissing)
	{
		return getBinaryOperatorReturnType(
			(Function) joinInterface,
			FlatJoinFunction.class,
			0,
			1,
			2,
			new int[]{0},
			new int[]{1},
			new int[]{2, 0},
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(JoinFunction<IN1, IN2, OUT> joinInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type)
	{
		return getJoinReturnTypes(joinInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(JoinFunction<IN1, IN2, OUT> joinInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, String functionName, boolean allowMissing)
	{
		return getBinaryOperatorReturnType(
			(Function) joinInterface,
			JoinFunction.class,
			0,
			1,
			2,
			new int[]{0},
			new int[]{1},
			NO_INDEX,
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(CoGroupFunction<IN1, IN2, OUT> coGroupInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type)
	{
		return getCoGroupReturnTypes(coGroupInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(CoGroupFunction<IN1, IN2, OUT> coGroupInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, String functionName, boolean allowMissing)
	{
		return getBinaryOperatorReturnType(
			(Function) coGroupInterface,
			CoGroupFunction.class,
			0,
			1,
			2,
			new int[]{0, 0},
			new int[]{1, 0},
			new int[]{2, 0},
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(CrossFunction<IN1, IN2, OUT> crossInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type)
	{
		return getCrossReturnTypes(crossInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(CrossFunction<IN1, IN2, OUT> crossInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, String functionName, boolean allowMissing)
	{
		return getBinaryOperatorReturnType(
			(Function) crossInterface,
			CrossFunction.class,
			0,
			1,
			2,
			new int[]{0},
			new int[]{1},
			NO_INDEX,
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(KeySelector<IN, OUT> selectorInterface, TypeInformation<IN> inType) {
		return getKeySelectorTypes(selectorInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(KeySelector<IN, OUT> selectorInterface,
			TypeInformation<IN> inType, String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) selectorInterface,
			KeySelector.class,
			0,
			1,
			new int[]{0},
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <T> TypeInformation<T> getPartitionerTypes(Partitioner<T> partitioner) {
		return getPartitionerTypes(partitioner, null, false);
	}

	@PublicEvolving
	public static <T> TypeInformation<T> getPartitionerTypes(
		Partitioner<T> partitioner,
		String functionName,
		boolean allowMissing) {
		try {
			final LambdaExecutable exec;
			try {
				exec = checkAndExtractLambda(partitioner);
			} catch (TypeExtractionException e) {
				throw new InvalidTypesException("Internal error occurred.", e);
			}
			if (exec != null) {
				// check for lambda type erasure
				validateLambdaGenericParameters(exec);

				// parameters must be accessed from behind, since JVM can add additional parameters e.g. when using local variables inside lambda function
				// paramLen is the total number of parameters of the provided lambda, it includes parameters added through closure
				final int paramLen = exec.getParameterTypes().length;

				final Method sam = TypeExtractionUtils.getSingleAbstractMethod(Partitioner.class);
				// number of parameters the SAM of implemented interface has; the parameter indexing applies to this range
				final int baseParametersLen = sam.getParameterTypes().length;

				final Type keyType = TypeExtractionUtils.extractTypeFromLambda(
					exec,
					new int[]{0},
					paramLen,
					baseParametersLen);
				return new TypeExtractor().privateCreateTypeInfo(keyType, null, null);
			} else {
				return new TypeExtractor().privateCreateTypeInfo(
					Partitioner.class,
					partitioner.getClass(),
					0,
					null,
					null);
			}
		} catch (InvalidTypesException e) {
			if (allowMissing) {
				return (TypeInformation<T>) new MissingTypeInfo(functionName != null ? functionName : partitioner.toString(), e);
			} else {
				throw e;
			}
		}
	}


	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <IN> TypeInformation<IN> getInputFormatTypes(InputFormat<IN, ?> inputFormatInterface) {
		if (inputFormatInterface instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<IN>) inputFormatInterface).getProducedType();
		}
		return new TypeExtractor().privateCreateTypeInfo(InputFormat.class, inputFormatInterface.getClass(), 0, null, null);
	}

	// --------------------------------------------------------------------------------------------
	//  Generic extraction methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the unary operator's return type.
	 *
	 * <p><b>NOTE:</b> lambda type indices allow extraction of Type from lambdas. To extract input type <b>IN</b>
	 * from the function given below one should pass {@code new int[] {0,1,0}} as lambdaInputTypeArgumentIndices.
	 *
	 * <pre>
	 * <code>
	 * OUT apply(Map<String, List<IN>> value)
	 * </code>
	 * </pre>
	 *
	 * @param function Function to extract the return type from
	 * @param baseClass Base class of the function
	 * @param inputTypeArgumentIndex Index of input type in the class specification
	 * @param outputTypeArgumentIndex Index of output type in the class specification
	 * @param lambdaInputTypeArgumentIndices Table of indices of the type argument specifying the input type. See example.
	 * @param lambdaOutputTypeArgumentIndices Table of indices of the type argument specifying the input type. See example.
	 * @param inType Type of the input elements (In case of an iterable, it is the element type)
	 * @param functionName Function name
	 * @param allowMissing Can the type information be missing
	 * @param <IN> Input type
	 * @param <OUT> Output type
	 * @return TypeInformation of the return type of the function
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getUnaryOperatorReturnType(
		Function function,
		Class<?> baseClass,
		int inputTypeArgumentIndex,
		int outputTypeArgumentIndex,
		int[] lambdaInputTypeArgumentIndices,
		int[] lambdaOutputTypeArgumentIndices,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {
		try {
			final LambdaExecutable exec;
			try {
				exec = checkAndExtractLambda(function);
			} catch (TypeExtractionException e) {
				throw new InvalidTypesException("Internal error occurred.", e);
			}
			if (exec != null) {
				Preconditions.checkArgument(
					lambdaInputTypeArgumentIndices != null && lambdaInputTypeArgumentIndices.length >= 1,
					"Indices for input type arguments within lambda not provided");
				Preconditions.checkArgument(
					lambdaOutputTypeArgumentIndices != null,
					"Indices for output type arguments within lambda not provided");
				// check for lambda type erasure
				validateLambdaGenericParameters(exec);

				// parameters must be accessed from behind, since JVM can add additional parameters e.g. when using local variables inside lambda function
				// paramLen is the total number of parameters of the provided lambda, it includes parameters added through closure
				final int paramLen = exec.getParameterTypes().length;

				final Method sam = TypeExtractionUtils.getSingleAbstractMethod(baseClass);

				// number of parameters the SAM of implemented interface has; the parameter indexing applies to this range
				final int baseParametersLen = sam.getParameterTypes().length;

				// executable references "this" implicitly
				if (paramLen <= 0) {
					// executable declaring class can also be a super class of the input type
					// we only validate if the executable exists in input type
					validateInputContainsExecutable(exec, inType);
				}
				else {
					final Type input = TypeExtractionUtils.extractTypeFromLambda(
						exec,
						lambdaInputTypeArgumentIndices,
						paramLen,
						baseParametersLen);
					validateInputType(input, inType);
				}

				if (function instanceof ResultTypeQueryable) {
					return ((ResultTypeQueryable<OUT>) function).getProducedType();
				}

				final Type output;
				if (lambdaOutputTypeArgumentIndices.length > 0) {
					output = TypeExtractionUtils.extractTypeFromLambda(
						exec,
						lambdaOutputTypeArgumentIndices,
						paramLen,
						baseParametersLen);
				} else {
					output = exec.getReturnType();
				}

				return new TypeExtractor().privateCreateTypeInfo(output, inType, null);
			} else {
				Preconditions.checkArgument(inputTypeArgumentIndex >= 0, "Input type argument index was not provided");
				Preconditions.checkArgument(outputTypeArgumentIndex >= 0, "Output type argument index was not provided");
				validateInputType(baseClass, function.getClass(), inputTypeArgumentIndex, inType);
				if(function instanceof ResultTypeQueryable) {
					return ((ResultTypeQueryable<OUT>) function).getProducedType();
				}
				return new TypeExtractor().privateCreateTypeInfo(baseClass, function.getClass(), outputTypeArgumentIndex, inType, null);
			}
		}
		catch (InvalidTypesException e) {
			if (allowMissing) {
				return (TypeInformation<OUT>) new MissingTypeInfo(functionName != null ? functionName : function.toString(), e);
			} else {
				throw e;
			}
		}
	}

	/**
	 * Returns the binary operator's return type.
	 *
	 * <p><b>NOTE:</b> lambda type indices allows extraction of Type from lambdas. To extract input type <b>IN1</b>
	 * from the function given below one should pass {@code new int[] {0,1,0}} as lambdaInput1TypeArgumentIndices.
	 *
	 * <pre>
	 * <code>
	 * OUT apply(Map<String, List<IN1>> value1, List<IN2> value2)
	 * </code>
	 * </pre>
	 *
	 * @param function Function to extract the return type from
	 * @param baseClass Base class of the function
	 * @param input1TypeArgumentIndex Index of first input type in the class specification
	 * @param input2TypeArgumentIndex Index of second input type in the class specification
	 * @param outputTypeArgumentIndex Index of output type in the class specification
	 * @param lambdaInput1TypeArgumentIndices Table of indices of the type argument specifying the first input type. See example.
	 * @param lambdaInput2TypeArgumentIndices Table of indices of the type argument specifying the second input type. See example.
	 * @param lambdaOutputTypeArgumentIndices Table of indices of the type argument specifying the output type. See example.
	 * @param in1Type Type of the left side input elements (In case of an iterable, it is the element type)
	 * @param in2Type Type of the right side input elements (In case of an iterable, it is the element type)
	 * @param functionName Function name
	 * @param allowMissing Can the type information be missing
	 * @param <IN1> Left side input type
	 * @param <IN2> Right side input type
	 * @param <OUT> Output type
	 * @return TypeInformation of the return type of the function
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getBinaryOperatorReturnType(
		Function function,
		Class<?> baseClass,
		int input1TypeArgumentIndex,
		int input2TypeArgumentIndex,
		int outputTypeArgumentIndex,
		int[] lambdaInput1TypeArgumentIndices,
		int[] lambdaInput2TypeArgumentIndices,
		int[] lambdaOutputTypeArgumentIndices,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {
		try {
			final LambdaExecutable exec;
			try {
				exec = checkAndExtractLambda(function);
			} catch (TypeExtractionException e) {
				throw new InvalidTypesException("Internal error occurred.", e);
			}
			if (exec != null) {
				Preconditions.checkArgument(
					lambdaInput1TypeArgumentIndices != null && lambdaInput1TypeArgumentIndices.length >= 1,
					"Indices for first input type arguments within lambda not provided");
				Preconditions.checkArgument(
					lambdaInput2TypeArgumentIndices != null && lambdaInput1TypeArgumentIndices.length >= 1,
					"Indices for second input type arguments within lambda not provided");
				Preconditions.checkArgument(
					lambdaOutputTypeArgumentIndices != null,
					"Indices for output type arguments within lambda not provided");
				// check for lambda type erasure
				validateLambdaGenericParameters(exec);

				final Method sam = TypeExtractionUtils.getSingleAbstractMethod(baseClass);
				final int baseParametersLen = sam.getParameterTypes().length;

				// parameters must be accessed from behind, since JVM can add additional parameters e.g. when using local variables inside lambda function
				final int paramLen = exec.getParameterTypes().length;

				final Type input1 = TypeExtractionUtils.extractTypeFromLambda(
					exec,
					lambdaInput1TypeArgumentIndices,
					paramLen,
					baseParametersLen);
				final Type input2 = TypeExtractionUtils.extractTypeFromLambda(
					exec,
					lambdaInput2TypeArgumentIndices,
					paramLen,
					baseParametersLen);

				validateInputType(input1, in1Type);
				validateInputType(input2, in2Type);
				if(function instanceof ResultTypeQueryable) {
					return ((ResultTypeQueryable<OUT>) function).getProducedType();
				}

				final Type output;
				if (lambdaOutputTypeArgumentIndices.length > 0) {
					output = TypeExtractionUtils.extractTypeFromLambda(
						exec,
						lambdaOutputTypeArgumentIndices,
						paramLen,
						baseParametersLen);
				} else {
					output = exec.getReturnType();
				}

				return new TypeExtractor().privateCreateTypeInfo(
					output,
					in1Type,
					in2Type);
			}
			else {
				Preconditions.checkArgument(input1TypeArgumentIndex >= 0, "Input 1 type argument index was not provided");
				Preconditions.checkArgument(input2TypeArgumentIndex >= 0, "Input 2 type argument index was not provided");
				Preconditions.checkArgument(outputTypeArgumentIndex >= 0, "Output type argument index was not provided");
				validateInputType(baseClass, function.getClass(), input1TypeArgumentIndex, in1Type);
				validateInputType(baseClass, function.getClass(), input2TypeArgumentIndex, in2Type);
				if(function instanceof ResultTypeQueryable) {
					return ((ResultTypeQueryable<OUT>) function).getProducedType();
				}
				return new TypeExtractor().privateCreateTypeInfo(baseClass, function.getClass(), outputTypeArgumentIndex, in1Type, in2Type);
			}
		}
		catch (InvalidTypesException e) {
			if (allowMissing) {
				return (TypeInformation<OUT>) new MissingTypeInfo(functionName != null ? functionName : function.toString(), e);
			} else {
				throw e;
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Create type information
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public static <T> TypeInformation<T> createTypeInfo(Class<T> type) {
		return (TypeInformation<T>) createTypeInfo((Type) type);
	}

	public static TypeInformation<?> createTypeInfo(Type t) {
		TypeInformation<?> ti = new TypeExtractor().privateCreateTypeInfo(t);
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}

	/**
	 * Creates a {@link TypeInformation} from the given parameters.
	 *
	 * If the given {@code instance} implements {@link ResultTypeQueryable}, its information
	 * is used to determine the type information. Otherwise, the type information is derived
	 * based on the given class information.
	 *
	 * @param instance			instance to determine type information for
	 * @param baseClass			base class of {@code instance}
	 * @param clazz				class of {@code instance}
	 * @param returnParamPos	index of the return type in the type arguments of {@code clazz}
	 * @param <OUT>				output type
	 * @return type information
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <OUT> TypeInformation<OUT> createTypeInfo(Object instance, Class<?> baseClass, Class<?> clazz, int returnParamPos) {
		if (instance instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) instance).getProducedType();
		} else {
			return createTypeInfo(baseClass, clazz, returnParamPos, null, null);
		}
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfo(Class<?> baseClass, Class<?> clazz, int returnParamPos,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		TypeInformation<OUT> ti =  new TypeExtractor().privateCreateTypeInfo(baseClass, clazz, returnParamPos, in1Type, in2Type);
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}

	// ----------------------------------- private methods ----------------------------------------

	private TypeInformation<?> privateCreateTypeInfo(Type t) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();
		typeHierarchy.add(t);
		return createTypeInfoWithTypeHierarchy(typeHierarchy, t, null, null);
	}

	// for (Rich)Functions
	@SuppressWarnings("unchecked")
	private <IN1, IN2, OUT> TypeInformation<OUT> privateCreateTypeInfo(Class<?> baseClass, Class<?> clazz, int returnParamPos,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();
		Type returnType = getParameterType(baseClass, typeHierarchy, clazz, returnParamPos);

		TypeInformation<OUT> typeInfo;

		// return type is a variable -> try to get the type info from the input directly
		if (returnType instanceof TypeVariable<?>) {
			typeInfo = (TypeInformation<OUT>) createTypeInfoFromInputs((TypeVariable<?>) returnType, typeHierarchy, in1Type, in2Type);

			if (typeInfo != null) {
				return typeInfo;
			}
		}

		// get info from hierarchy
		return (TypeInformation<OUT>) createTypeInfoWithTypeHierarchy(typeHierarchy, returnType, in1Type, in2Type);
	}

	// for LambdaFunctions
	@SuppressWarnings("unchecked")
	private <IN1, IN2, OUT> TypeInformation<OUT> privateCreateTypeInfo(Type returnType, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();

		// get info from hierarchy
		return (TypeInformation<OUT>) createTypeInfoWithTypeHierarchy(typeHierarchy, returnType, in1Type, in2Type);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfoWithTypeHierarchy(ArrayList<Type> typeHierarchy, Type t,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {

		// check if type information can be created using a type factory
		final TypeInformation<OUT> typeFromFactory = createTypeInfoFromFactory(t, typeHierarchy, in1Type, in2Type);
		if (typeFromFactory != null) {
			return typeFromFactory;
		}
		// check if type is a subclass of tuple
		else if (isClassType(t) && Tuple.class.isAssignableFrom(typeToClass(t))) {
			Type curT = t;

			// do not allow usage of Tuple as type
			if (typeToClass(t).equals(Tuple.class)) {
				throw new InvalidTypesException(
						"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
			}

			// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
			// collect the types while moving up for a later top-down
			while (!(isClassType(curT) && typeToClass(curT).getSuperclass().equals(Tuple.class))) {
				typeHierarchy.add(curT);
				curT = typeToClass(curT).getGenericSuperclass();
			}

			if(curT == Tuple0.class) {
				return new TupleTypeInfo(Tuple0.class);
			}

			// check if immediate child of Tuple has generics
			if (curT instanceof Class<?>) {
				throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
			}

			typeHierarchy.add(curT);

			// create the type information for the subtypes
			final TypeInformation<?>[] subTypesInfo = createSubTypesInfo(t, (ParameterizedType) curT, typeHierarchy, in1Type, in2Type, false);
			// type needs to be treated a pojo due to additional fields
			if (subTypesInfo == null) {
				if (t instanceof ParameterizedType) {
					return (TypeInformation<OUT>) analyzePojo(typeToClass(t), new ArrayList<Type>(typeHierarchy), (ParameterizedType) t, in1Type, in2Type);
				}
				else {
					return (TypeInformation<OUT>) analyzePojo(typeToClass(t), new ArrayList<Type>(typeHierarchy), null, in1Type, in2Type);
				}
			}
			// return tuple info
			return new TupleTypeInfo(typeToClass(t), subTypesInfo);

		}
		// type depends on another type
		// e.g. class MyMapper<E> extends MapFunction<String, E>
		else if (t instanceof TypeVariable) {
			Type typeVar = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) t);

			if (!(typeVar instanceof TypeVariable)) {
				return createTypeInfoWithTypeHierarchy(typeHierarchy, typeVar, in1Type, in2Type);
			}
			// try to derive the type info of the TypeVariable from the immediate base child input as a last attempt
			else {
				TypeInformation<OUT> typeInfo = (TypeInformation<OUT>) createTypeInfoFromInputs((TypeVariable<?>) t, typeHierarchy, in1Type, in2Type);
				if (typeInfo != null) {
					return typeInfo;
				} else {
					throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) t).getName() + "' in '"
							+ ((TypeVariable<?>) t).getGenericDeclaration() + "' could not be determined. This is most likely a type erasure problem. "
							+ "The type extraction currently supports types with generic variables only in cases where "
							+ "all variables in the return type can be deduced from the input type(s).");
				}
			}
		}
		// arrays with generics
		else if (t instanceof GenericArrayType) {
			GenericArrayType genericArray = (GenericArrayType) t;

			Type componentType = genericArray.getGenericComponentType();

			// due to a Java 6 bug, it is possible that the JVM classifies e.g. String[] or int[] as GenericArrayType instead of Class
			if (componentType instanceof Class) {
				Class<?> componentClass = (Class<?>) componentType;

				Class<OUT> classArray = (Class<OUT>) (java.lang.reflect.Array.newInstance(componentClass, 0).getClass());

				return getForClass(classArray);
			} else {
				TypeInformation<?> componentInfo = createTypeInfoWithTypeHierarchy(
					typeHierarchy,
					genericArray.getGenericComponentType(),
					in1Type,
					in2Type);

				Class<?> componentClass = componentInfo.getTypeClass();
				Class<OUT> classArray = (Class<OUT>) (java.lang.reflect.Array.newInstance(componentClass, 0).getClass());

				return ObjectArrayTypeInfo.getInfoFor(classArray, componentInfo);
			}
		}
		// objects with generics are treated as Class first
		else if (t instanceof ParameterizedType) {
			return (TypeInformation<OUT>) privateGetForClass(typeToClass(t), typeHierarchy, (ParameterizedType) t, in1Type, in2Type);
		}
		// no tuple, no TypeVariable, no generic type
		else if (t instanceof Class) {
			return privateGetForClass((Class<OUT>) t, typeHierarchy);
		}

		throw new InvalidTypesException("Type Information could not be created.");
	}

	private <IN1, IN2> TypeInformation<?> createTypeInfoFromInputs(TypeVariable<?> returnTypeVar, ArrayList<Type> returnTypeHierarchy,
			TypeInformation<IN1> in1TypeInfo, TypeInformation<IN2> in2TypeInfo) {

		Type matReturnTypeVar = materializeTypeVariable(returnTypeHierarchy, returnTypeVar);

		// variable could be resolved
		if (!(matReturnTypeVar instanceof TypeVariable)) {
			return createTypeInfoWithTypeHierarchy(returnTypeHierarchy, matReturnTypeVar, in1TypeInfo, in2TypeInfo);
		}
		else {
			returnTypeVar = (TypeVariable<?>) matReturnTypeVar;
		}

		// no input information exists
		if (in1TypeInfo == null && in2TypeInfo == null) {
			return null;
		}

		// create a new type hierarchy for the input
		ArrayList<Type> inputTypeHierarchy = new ArrayList<Type>();
		// copy the function part of the type hierarchy
		for (Type t : returnTypeHierarchy) {
			if (isClassType(t) && Function.class.isAssignableFrom(typeToClass(t)) && typeToClass(t) != Function.class) {
				inputTypeHierarchy.add(t);
			}
			else {
				break;
			}
		}
		ParameterizedType baseClass = (ParameterizedType) inputTypeHierarchy.get(inputTypeHierarchy.size() - 1);

		TypeInformation<?> info = null;
		if (in1TypeInfo != null) {
			// find the deepest type variable that describes the type of input 1
			Type in1Type = baseClass.getActualTypeArguments()[0];

			info = createTypeInfoFromInput(returnTypeVar, new ArrayList<Type>(inputTypeHierarchy), in1Type, in1TypeInfo);
		}

		if (info == null && in2TypeInfo != null) {
			// find the deepest type variable that describes the type of input 2
			Type in2Type = baseClass.getActualTypeArguments()[1];

			info = createTypeInfoFromInput(returnTypeVar, new ArrayList<Type>(inputTypeHierarchy), in2Type, in2TypeInfo);
		}

		if (info != null) {
			return info;
		}

		return null;
	}

	/**
	 * Finds the type information to a type variable.
	 *
	 * It solve the following:
	 *
	 * Return the type information for "returnTypeVar" given that "inType" has type information "inTypeInfo".
	 * Thus "inType" must contain "returnTypeVar" in a "inputTypeHierarchy", otherwise null is returned.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private <IN1> TypeInformation<?> createTypeInfoFromInput(TypeVariable<?> returnTypeVar, ArrayList<Type> inputTypeHierarchy, Type inType, TypeInformation<IN1> inTypeInfo) {
		TypeInformation<?> info = null;

		// use a factory to find corresponding type information to type variable
		final ArrayList<Type> factoryHierarchy = new ArrayList<>(inputTypeHierarchy);
		final TypeInfoFactory<?> factory = getClosestFactory(factoryHierarchy, inType);
		if (factory != null) {
			// the type that defines the factory is last in factory hierarchy
			final Type factoryDefiningType = factoryHierarchy.get(factoryHierarchy.size() - 1);
			// defining type has generics, the factory need to be asked for a mapping of subtypes to type information
			if (factoryDefiningType instanceof ParameterizedType) {
				final Type[] typeParams = typeToClass(factoryDefiningType).getTypeParameters();
				final Type[] actualParams = ((ParameterizedType) factoryDefiningType).getActualTypeArguments();
				// go thru all elements and search for type variables
				for (int i = 0; i < actualParams.length; i++) {
					final Map<String, TypeInformation<?>> componentInfo = inTypeInfo.getGenericParameters();
					final String typeParamName = typeParams[i].toString();
					if (!componentInfo.containsKey(typeParamName) || componentInfo.get(typeParamName) == null) {
						throw new InvalidTypesException("TypeInformation '" + inTypeInfo.getClass().getSimpleName() +
							"' does not supply a mapping of TypeVariable '" + typeParamName + "' to corresponding TypeInformation. " +
							"Input type inference can only produce a result with this information. " +
							"Please implement method 'TypeInformation.getGenericParameters()' for this.");
					}
					info = createTypeInfoFromInput(returnTypeVar, factoryHierarchy, actualParams[i], componentInfo.get(typeParamName));
					if (info != null) {
						break;
					}
				}
			}
		}
		// the input is a type variable
		else if (sameTypeVars(inType, returnTypeVar)) {
			return inTypeInfo;
		}
		else if (inType instanceof TypeVariable) {
			Type resolvedInType = materializeTypeVariable(inputTypeHierarchy, (TypeVariable<?>) inType);
			if (resolvedInType != inType) {
				info = createTypeInfoFromInput(returnTypeVar, inputTypeHierarchy, resolvedInType, inTypeInfo);
			}
		}
		// input is an array
		else if (inType instanceof GenericArrayType) {
			TypeInformation<?> componentInfo = null;
			if (inTypeInfo instanceof BasicArrayTypeInfo) {
				componentInfo = ((BasicArrayTypeInfo<?,?>) inTypeInfo).getComponentInfo();
			}
			else if (inTypeInfo instanceof PrimitiveArrayTypeInfo) {
				componentInfo = BasicTypeInfo.getInfoFor(inTypeInfo.getTypeClass().getComponentType());
			}
			else if (inTypeInfo instanceof ObjectArrayTypeInfo) {
				componentInfo = ((ObjectArrayTypeInfo<?,?>) inTypeInfo).getComponentInfo();
			}
			info = createTypeInfoFromInput(returnTypeVar, inputTypeHierarchy, ((GenericArrayType) inType).getGenericComponentType(), componentInfo);
		}
		// the input is a tuple
		else if (inTypeInfo instanceof TupleTypeInfo && isClassType(inType) && Tuple.class.isAssignableFrom(typeToClass(inType))) {
			ParameterizedType tupleBaseClass;

			// get tuple from possible tuple subclass
			while (!(isClassType(inType) && typeToClass(inType).getSuperclass().equals(Tuple.class))) {
				inputTypeHierarchy.add(inType);
				inType = typeToClass(inType).getGenericSuperclass();
			}
			inputTypeHierarchy.add(inType);

			// we can assume to be parameterized since we
			// already did input validation
			tupleBaseClass = (ParameterizedType) inType;

			Type[] tupleElements = tupleBaseClass.getActualTypeArguments();
			// go thru all tuple elements and search for type variables
			for (int i = 0; i < tupleElements.length; i++) {
				info = createTypeInfoFromInput(returnTypeVar, inputTypeHierarchy, tupleElements[i], ((TupleTypeInfo<?>) inTypeInfo).getTypeAt(i));
				if(info != null) {
					break;
				}
			}
		}
		// the input is a pojo
		else if (inTypeInfo instanceof PojoTypeInfo && isClassType(inType)) {
			// build the entire type hierarchy for the pojo
			getTypeHierarchy(inputTypeHierarchy, inType, Object.class);
			// determine a field containing the type variable
			List<Field> fields = getAllDeclaredFields(typeToClass(inType), false);
			for (Field field : fields) {
				Type fieldType = field.getGenericType();
				if (fieldType instanceof TypeVariable && sameTypeVars(returnTypeVar, materializeTypeVariable(inputTypeHierarchy, (TypeVariable<?>) fieldType))) {
					return getTypeOfPojoField(inTypeInfo, field);
				}
				else if (fieldType instanceof ParameterizedType || fieldType instanceof GenericArrayType) {
					ArrayList<Type> typeHierarchyWithFieldType = new ArrayList<>(inputTypeHierarchy);
					typeHierarchyWithFieldType.add(fieldType);
					TypeInformation<?> foundInfo = createTypeInfoFromInput(returnTypeVar, typeHierarchyWithFieldType, fieldType, getTypeOfPojoField(inTypeInfo, field));
					if (foundInfo != null) {
						return foundInfo;
					}
				}
			}
		}
		return info;
	}

	/**
	 * Creates the TypeInformation for all elements of a type that expects a certain number of
	 * subtypes (e.g. TupleXX).
	 *
	 * @param originalType most concrete subclass
	 * @param definingType type that defines the number of subtypes (e.g. Tuple2 -> 2 subtypes)
	 * @param typeHierarchy necessary for type inference
	 * @param in1Type necessary for type inference
	 * @param in2Type necessary for type inference
	 * @param lenient decides whether exceptions should be thrown if a subtype can not be determined
	 * @return array containing TypeInformation of sub types or null if definingType contains
	 *     more subtypes (fields) that defined
	 */
	private <IN1, IN2> TypeInformation<?>[] createSubTypesInfo(Type originalType, ParameterizedType definingType,
			ArrayList<Type> typeHierarchy, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, boolean lenient) {
		Type[] subtypes = new Type[definingType.getActualTypeArguments().length];

		// materialize possible type variables
		for (int i = 0; i < subtypes.length; i++) {
			final Type actualTypeArg = definingType.getActualTypeArguments()[i];
			// materialize immediate TypeVariables
			if (actualTypeArg instanceof TypeVariable<?>) {
				subtypes[i] = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) actualTypeArg);
			}
			// class or parameterized type
			else {
				subtypes[i] = actualTypeArg;
			}
		}

		TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[subtypes.length];
		for (int i = 0; i < subtypes.length; i++) {
			final ArrayList<Type> subTypeHierarchy = new ArrayList<>(typeHierarchy);
			subTypeHierarchy.add(subtypes[i]);
			// sub type could not be determined with materializing
			// try to derive the type info of the TypeVariable from the immediate base child input as a last attempt
			if (subtypes[i] instanceof TypeVariable<?>) {
				subTypesInfo[i] = createTypeInfoFromInputs((TypeVariable<?>) subtypes[i], subTypeHierarchy, in1Type, in2Type);

				// variable could not be determined
				if (subTypesInfo[i] == null && !lenient) {
					throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) subtypes[i]).getName() + "' in '"
							+ ((TypeVariable<?>) subtypes[i]).getGenericDeclaration()
							+ "' could not be determined. This is most likely a type erasure problem. "
							+ "The type extraction currently supports types with generic variables only in cases where "
							+ "all variables in the return type can be deduced from the input type(s).");
				}
			} else {
				// create the type information of the subtype or null/exception
				try {
					subTypesInfo[i] = createTypeInfoWithTypeHierarchy(subTypeHierarchy, subtypes[i], in1Type, in2Type);
				} catch (InvalidTypesException e) {
					if (lenient) {
						subTypesInfo[i] = null;
					} else {
						throw e;
					}
				}
			}
		}

		// check that number of fields matches the number of subtypes
		if (!lenient) {
			Class<?> originalTypeAsClass = null;
			if (isClassType(originalType)) {
				originalTypeAsClass = typeToClass(originalType);
			}
			checkNotNull(originalTypeAsClass, "originalType has an unexpected type");
			// check if the class we assumed to conform to the defining type so far is actually a pojo because the
			// original type contains additional fields.
			// check for additional fields.
			int fieldCount = countFieldsInClass(originalTypeAsClass);
			if(fieldCount > subTypesInfo.length) {
				return null;
			}
		}

		return subTypesInfo;
	}

	/**
	 * Creates type information using a factory if for this type or super types. Returns null otherwise.
	 */
	@SuppressWarnings("unchecked")
	private <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfoFromFactory(
			Type t, ArrayList<Type> typeHierarchy, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {

		final ArrayList<Type> factoryHierarchy = new ArrayList<>(typeHierarchy);
		final TypeInfoFactory<? super OUT> factory = getClosestFactory(factoryHierarchy, t);
		if (factory == null) {
			return null;
		}
		final Type factoryDefiningType = factoryHierarchy.get(factoryHierarchy.size() - 1);

		// infer possible type parameters from input
		final Map<String, TypeInformation<?>> genericParams;
		if (factoryDefiningType instanceof ParameterizedType) {
			genericParams = new HashMap<>();
			final ParameterizedType paramDefiningType = (ParameterizedType) factoryDefiningType;
			final Type[] args = typeToClass(paramDefiningType).getTypeParameters();

			final TypeInformation<?>[] subtypeInfo = createSubTypesInfo(t, paramDefiningType, factoryHierarchy, in1Type, in2Type, true);
			assert subtypeInfo != null;
			for (int i = 0; i < subtypeInfo.length; i++) {
				genericParams.put(args[i].toString(), subtypeInfo[i]);
			}
		} else {
			genericParams = Collections.emptyMap();
		}

		final TypeInformation<OUT> createdTypeInfo = (TypeInformation<OUT>) factory.createTypeInfo(t, genericParams);
		if (createdTypeInfo == null) {
			throw new InvalidTypesException("TypeInfoFactory returned invalid TypeInformation 'null'");
		}
		return createdTypeInfo;
	}

	// --------------------------------------------------------------------------------------------
	//  Extract type parameters
	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	public static Type getParameterType(Class<?> baseClass, Class<?> clazz, int pos) {
		return getParameterType(baseClass, null, clazz, pos);
	}

	private static Type getParameterType(Class<?> baseClass, ArrayList<Type> typeHierarchy, Class<?> clazz, int pos) {
		if (typeHierarchy != null) {
			typeHierarchy.add(clazz);
		}
		Type[] interfaceTypes = clazz.getGenericInterfaces();

		// search in interfaces for base class
		for (Type t : interfaceTypes) {
			Type parameter = getParameterTypeFromGenericType(baseClass, typeHierarchy, t, pos);
			if (parameter != null) {
				return parameter;
			}
		}

		// search in superclass for base class
		Type t = clazz.getGenericSuperclass();
		Type parameter = getParameterTypeFromGenericType(baseClass, typeHierarchy, t, pos);
		if (parameter != null) {
			return parameter;
		}

		throw new InvalidTypesException("The types of the interface " + baseClass.getName() + " could not be inferred. " +
						"Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point");
	}

	private static Type getParameterTypeFromGenericType(Class<?> baseClass, ArrayList<Type> typeHierarchy, Type t, int pos) {
		// base class
		if (t instanceof ParameterizedType && baseClass.equals(((ParameterizedType) t).getRawType())) {
			if (typeHierarchy != null) {
				typeHierarchy.add(t);
			}
			ParameterizedType baseClassChild = (ParameterizedType) t;
			return baseClassChild.getActualTypeArguments()[pos];
		}
		// interface that extended base class as class or parameterized type
		else if (t instanceof ParameterizedType && baseClass.isAssignableFrom((Class<?>) ((ParameterizedType) t).getRawType())) {
			if (typeHierarchy != null) {
				typeHierarchy.add(t);
			}
			return getParameterType(baseClass, typeHierarchy, (Class<?>) ((ParameterizedType) t).getRawType(), pos);
		}
		else if (t instanceof Class<?> && baseClass.isAssignableFrom((Class<?>) t)) {
			if (typeHierarchy != null) {
				typeHierarchy.add(t);
			}
			return getParameterType(baseClass, typeHierarchy, (Class<?>) t, pos);
		}
		return null;
	}

	// --------------------------------------------------------------------------------------------
	//  Validate input
	// --------------------------------------------------------------------------------------------

	private static void validateInputType(Type t, TypeInformation<?> inType) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();
		try {
			validateInfo(typeHierarchy, t, inType);
		}
		catch(InvalidTypesException e) {
			throw new InvalidTypesException("Input mismatch: " + e.getMessage(), e);
		}
	}

	private static void validateInputType(Class<?> baseClass, Class<?> clazz, int inputParamPos, TypeInformation<?> inTypeInfo) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();

		// try to get generic parameter
		Type inType;
		try {
			inType = getParameterType(baseClass, typeHierarchy, clazz, inputParamPos);
		}
		catch (InvalidTypesException e) {
			return; // skip input validation e.g. for raw types
		}

		try {
			validateInfo(typeHierarchy, inType, inTypeInfo);
		}
		catch(InvalidTypesException e) {
			throw new InvalidTypesException("Input mismatch: " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private static void validateInfo(ArrayList<Type> typeHierarchy, Type type, TypeInformation<?> typeInfo) {
		if (type == null) {
			throw new InvalidTypesException("Unknown Error. Type is null.");
		}

		if (typeInfo == null) {
			throw new InvalidTypesException("Unknown Error. TypeInformation is null.");
		}

		if (!(type instanceof TypeVariable<?>)) {
			// check for Java Basic Types
			if (typeInfo instanceof BasicTypeInfo) {

				TypeInformation<?> actual;
				// check if basic type at all
				if (!(type instanceof Class<?>) || (actual = BasicTypeInfo.getInfoFor((Class<?>) type)) == null) {
					throw new InvalidTypesException("Basic type expected.");
				}
				// check if correct basic type
				if (!typeInfo.equals(actual)) {
					throw new InvalidTypesException("Basic type '" + typeInfo + "' expected but was '" + actual + "'.");
				}

			}
			// check for Java SQL time types
			else if (typeInfo instanceof SqlTimeTypeInfo) {

				TypeInformation<?> actual;
				// check if SQL time type at all
				if (!(type instanceof Class<?>) || (actual = SqlTimeTypeInfo.getInfoFor((Class<?>) type)) == null) {
					throw new InvalidTypesException("SQL time type expected.");
				}
				// check if correct SQL time type
				if (!typeInfo.equals(actual)) {
					throw new InvalidTypesException("SQL time type '" + typeInfo + "' expected but was '" + actual + "'.");
				}

			}
			// check for Java Tuples
			else if (typeInfo instanceof TupleTypeInfo) {
				// check if tuple at all
				if (!(isClassType(type) && Tuple.class.isAssignableFrom(typeToClass(type)))) {
					throw new InvalidTypesException("Tuple type expected.");
				}

				// do not allow usage of Tuple as type
				if (isClassType(type) && typeToClass(type).equals(Tuple.class)) {
					throw new InvalidTypesException("Concrete subclass of Tuple expected.");
				}

				// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
				while (!(isClassType(type) && typeToClass(type).getSuperclass().equals(Tuple.class))) {
					typeHierarchy.add(type);
					type = typeToClass(type).getGenericSuperclass();
				}

				if(type == Tuple0.class) {
					return;
				}

				// check if immediate child of Tuple has generics
				if (type instanceof Class<?>) {
					throw new InvalidTypesException("Parameterized Tuple type expected.");
				}

				TupleTypeInfo<?> tti = (TupleTypeInfo<?>) typeInfo;

				Type[] subTypes = ((ParameterizedType) type).getActualTypeArguments();

				if (subTypes.length != tti.getArity()) {
					throw new InvalidTypesException("Tuple arity '" + tti.getArity() + "' expected but was '"
							+ subTypes.length + "'.");
				}

				for (int i = 0; i < subTypes.length; i++) {
					validateInfo(new ArrayList<Type>(typeHierarchy), subTypes[i], tti.getTypeAt(i));
				}
			}
			// check for primitive array
			else if (typeInfo instanceof PrimitiveArrayTypeInfo) {
				Type component;
				// check if array at all
				if (!(type instanceof Class<?> && ((Class<?>) type).isArray() && (component = ((Class<?>) type).getComponentType()) != null)
						&& !(type instanceof GenericArrayType && (component = ((GenericArrayType) type).getGenericComponentType()) != null)) {
					throw new InvalidTypesException("Array type expected.");
				}
				if (component instanceof TypeVariable<?>) {
					component = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) component);
					if (component instanceof TypeVariable) {
						return;
					}
				}
				if (!(component instanceof Class<?> && ((Class<?>)component).isPrimitive())) {
					throw new InvalidTypesException("Primitive component expected.");
				}
			}
			// check for basic array
			else if (typeInfo instanceof BasicArrayTypeInfo<?, ?>) {
				Type component;
				// check if array at all
				if (!(type instanceof Class<?> && ((Class<?>) type).isArray() && (component = ((Class<?>) type).getComponentType()) != null)
						&& !(type instanceof GenericArrayType && (component = ((GenericArrayType) type).getGenericComponentType()) != null)) {
					throw new InvalidTypesException("Array type expected.");
				}

				if (component instanceof TypeVariable<?>) {
					component = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) component);
					if (component instanceof TypeVariable) {
						return;
					}
				}

				validateInfo(typeHierarchy, component, ((BasicArrayTypeInfo<?, ?>) typeInfo).getComponentInfo());

			}
			// check for object array
			else if (typeInfo instanceof ObjectArrayTypeInfo<?, ?>) {
				// check if array at all
				if (!(type instanceof Class<?> && ((Class<?>) type).isArray()) && !(type instanceof GenericArrayType)) {
					throw new InvalidTypesException("Object array type expected.");
				}

				// check component
				Type component;
				if (type instanceof Class<?>) {
					component = ((Class<?>) type).getComponentType();
				} else {
					component = ((GenericArrayType) type).getGenericComponentType();
				}

				if (component instanceof TypeVariable<?>) {
					component = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) component);
					if (component instanceof TypeVariable) {
						return;
					}
				}

				validateInfo(typeHierarchy, component, ((ObjectArrayTypeInfo<?, ?>) typeInfo).getComponentInfo());
			}
			// check for value
			else if (typeInfo instanceof ValueTypeInfo<?>) {
				// check if value at all
				if (!(type instanceof Class<?> && Value.class.isAssignableFrom((Class<?>) type))) {
					throw new InvalidTypesException("Value type expected.");
				}

				TypeInformation<?> actual;
				// check value type contents
				if (!((ValueTypeInfo<?>) typeInfo).equals(actual = ValueTypeInfo.getValueTypeInfo((Class<? extends Value>) type))) {
					throw new InvalidTypesException("Value type '" + typeInfo + "' expected but was '" + actual + "'.");
				}
			}
			// check for POJO
			else if (typeInfo instanceof PojoTypeInfo) {
				Class<?> clazz = null;
				if (!(isClassType(type) && ((PojoTypeInfo<?>) typeInfo).getTypeClass() == (clazz = typeToClass(type)))) {
					throw new InvalidTypesException("POJO type '"
							+ ((PojoTypeInfo<?>) typeInfo).getTypeClass().getCanonicalName() + "' expected but was '"
							+ clazz.getCanonicalName() + "'.");
				}
			}
			// check for Enum
			else if (typeInfo instanceof EnumTypeInfo) {
				if (!(type instanceof Class<?> && Enum.class.isAssignableFrom((Class<?>) type))) {
					throw new InvalidTypesException("Enum type expected.");
				}
				// check enum type contents
				if (!(typeInfo.getTypeClass() == type)) {
					throw new InvalidTypesException("Enum type '" + typeInfo.getTypeClass().getCanonicalName() + "' expected but was '"
							+ typeToClass(type).getCanonicalName() + "'.");
				}
			}
			// check for generic object
			else if (typeInfo instanceof GenericTypeInfo<?>) {
				Class<?> clazz = null;
				if (!(isClassType(type) && (clazz = typeToClass(type)).isAssignableFrom(((GenericTypeInfo<?>) typeInfo).getTypeClass()))) {
					throw new InvalidTypesException("Generic type '"
							+ ((GenericTypeInfo<?>) typeInfo).getTypeClass().getCanonicalName() + "' or a subclass of it expected but was '"
							+ clazz.getCanonicalName() + "'.");
				}
			}
			// check for Writable
			else {
				validateIfWritable(typeInfo, type);
			}
		} else {
			type = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) type);
			if (!(type instanceof TypeVariable)) {
				validateInfo(typeHierarchy, type, typeInfo);
			}
		}
	}

	private static void validateInputContainsExecutable(LambdaExecutable exec, TypeInformation<?> typeInfo) {
		List<Method> methods = getAllDeclaredMethods(typeInfo.getTypeClass());
		for (Method method : methods) {
			if (exec.executablesEquals(method)) {
				return;
			}
		}
		Constructor<?>[] constructors = typeInfo.getTypeClass().getDeclaredConstructors();
		for (Constructor<?> constructor : constructors) {
			if (exec.executablesEquals(constructor)) {
				return;
			}
		}
		throw new InvalidTypesException("Type contains no executable '" + exec.getName() + "'.");
	}

	// --------------------------------------------------------------------------------------------
	//  Utility methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the type information factory for a type using the factory registry or annotations.
	 */
	@Internal
	public static <OUT> TypeInfoFactory<OUT> getTypeInfoFactory(Type t) {
		final Class<?> factoryClass;
		if (registeredTypeInfoFactories.containsKey(t)) {
			factoryClass = registeredTypeInfoFactories.get(t);
		}
		else {
			if (!isClassType(t) || !typeToClass(t).isAnnotationPresent(TypeInfo.class)) {
				return null;
			}
			final TypeInfo typeInfoAnnotation = typeToClass(t).getAnnotation(TypeInfo.class);
			factoryClass = typeInfoAnnotation.value();
			// check for valid factory class
			if (!TypeInfoFactory.class.isAssignableFrom(factoryClass)) {
				throw new InvalidTypesException("TypeInfo annotation does not specify a valid TypeInfoFactory.");
			}
		}

		// instantiate
		return (TypeInfoFactory<OUT>) InstantiationUtil.instantiate(factoryClass);
	}

	/**
	 * @return number of items with equal type or same raw type
	 */
	private static int countTypeInHierarchy(ArrayList<Type> typeHierarchy, Type type) {
		int count = 0;
		for (Type t : typeHierarchy) {
			if (t == type || (isClassType(type) && t == typeToClass(type)) || (isClassType(t) && typeToClass(t) == type)) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Traverses the type hierarchy up until a type information factory can be found.
	 *
	 * @param typeHierarchy hierarchy to be filled while traversing up
	 * @param t type for which a factory needs to be found
	 * @return closest type information factory or null if there is no factory in the type hierarchy
	 */
	private static <OUT> TypeInfoFactory<? super OUT> getClosestFactory(ArrayList<Type> typeHierarchy, Type t) {
		TypeInfoFactory factory = null;
		while (factory == null && isClassType(t) && !(typeToClass(t).equals(Object.class))) {
			typeHierarchy.add(t);
			factory = getTypeInfoFactory(t);
			t = typeToClass(t).getGenericSuperclass();

			if (t == null) {
				break;
			}
		}
		return factory;
	}

	private int countFieldsInClass(Class<?> clazz) {
		int fieldCount = 0;
		for(Field field : clazz.getFields()) { // get all fields
			if(	!Modifier.isStatic(field.getModifiers()) &&
				!Modifier.isTransient(field.getModifiers())
				) {
				fieldCount++;
			}
		}
		return fieldCount;
	}

	private static void validateLambdaGenericParameters(LambdaExecutable exec) {
		// check the arguments
		for (Type t : exec.getParameterTypes()) {
			validateLambdaGenericParameter(t);
		}

		// check the return type
		validateLambdaGenericParameter(exec.getReturnType());
	}

	private static void validateLambdaGenericParameter(Type t) {
		if(!(t instanceof Class)) {
			return;
		}
		final Class<?> clazz = (Class<?>) t;

		if(clazz.getTypeParameters().length > 0) {
			throw new InvalidTypesException("The generic type parameters of '" + clazz.getSimpleName() + "' are missing. \n"
					+ "It seems that your compiler has not stored them into the .class file. \n"
					+ "Currently, only the Eclipse JDT compiler preserves the type information necessary to use the lambdas feature type-safely. \n"
					+ "See the documentation for more information about how to compile jobs containing lambda expressions.");
		}
	}

	/**
	 * Tries to find a concrete value (Class, ParameterizedType etc. ) for a TypeVariable by traversing the type hierarchy downwards.
	 * If a value could not be found it will return the most bottom type variable in the hierarchy.
	 */
	private static Type materializeTypeVariable(ArrayList<Type> typeHierarchy, TypeVariable<?> typeVar) {
		TypeVariable<?> inTypeTypeVar = typeVar;
		// iterate thru hierarchy from top to bottom until type variable gets a class assigned
		for (int i = typeHierarchy.size() - 1; i >= 0; i--) {
			Type curT = typeHierarchy.get(i);

			// parameterized type
			if (curT instanceof ParameterizedType) {
				Class<?> rawType = ((Class<?>) ((ParameterizedType) curT).getRawType());

				for (int paramIndex = 0; paramIndex < rawType.getTypeParameters().length; paramIndex++) {

					TypeVariable<?> curVarOfCurT = rawType.getTypeParameters()[paramIndex];

					// check if variable names match
					if (sameTypeVars(curVarOfCurT, inTypeTypeVar)) {
						Type curVarType = ((ParameterizedType) curT).getActualTypeArguments()[paramIndex];

						// another type variable level
						if (curVarType instanceof TypeVariable<?>) {
							inTypeTypeVar = (TypeVariable<?>) curVarType;
						}
						// class
						else {
							return curVarType;
						}
					}
				}
			}
		}
		// can not be materialized, most likely due to type erasure
		// return the type variable of the deepest level
		return inTypeTypeVar;
	}

	/**
	 * Creates type information from a given Class such as Integer, String[] or POJOs.
	 *
	 * This method does not support ParameterizedTypes such as Tuples or complex type hierarchies.
	 * In most cases {@link TypeExtractor#createTypeInfo(Type)} is the recommended method for type extraction
	 * (a Class is a child of Type).
	 *
	 * @param clazz a Class to create TypeInformation for
	 * @return TypeInformation that describes the passed Class
	 */
	public static <X> TypeInformation<X> getForClass(Class<X> clazz) {
		final ArrayList<Type> typeHierarchy = new ArrayList<>();
		typeHierarchy.add(clazz);
		return new TypeExtractor().privateGetForClass(clazz, typeHierarchy);
	}

	private <X> TypeInformation<X> privateGetForClass(Class<X> clazz, ArrayList<Type> typeHierarchy) {
		return privateGetForClass(clazz, typeHierarchy, null, null, null);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <OUT,IN1,IN2> TypeInformation<OUT> privateGetForClass(Class<OUT> clazz, ArrayList<Type> typeHierarchy,
			ParameterizedType parameterizedType, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		checkNotNull(clazz);

		// check if type information can be produced using a factory
		final TypeInformation<OUT> typeFromFactory = createTypeInfoFromFactory(clazz, typeHierarchy, in1Type, in2Type);
		if (typeFromFactory != null) {
			return typeFromFactory;
		}

		// Object is handled as generic type info
		if (clazz.equals(Object.class)) {
			return new GenericTypeInfo<>(clazz);
		}

		// Class is handled as generic type info
		if (clazz.equals(Class.class)) {
			return new GenericTypeInfo<OUT>(clazz);
		}

		// recursive types are handled as generic type info
		if (countTypeInHierarchy(typeHierarchy, clazz) > 1) {
			return new GenericTypeInfo<>(clazz);
		}

		// check for arrays
		if (clazz.isArray()) {

			// primitive arrays: int[], byte[], ...
			PrimitiveArrayTypeInfo<OUT> primitiveArrayInfo = PrimitiveArrayTypeInfo.getInfoFor(clazz);
			if (primitiveArrayInfo != null) {
				return primitiveArrayInfo;
			}

			// basic type arrays: String[], Integer[], Double[]
			BasicArrayTypeInfo<OUT, ?> basicArrayInfo = BasicArrayTypeInfo.getInfoFor(clazz);
			if (basicArrayInfo != null) {
				return basicArrayInfo;
			}

			// object arrays
			else {
				TypeInformation<?> componentTypeInfo = createTypeInfoWithTypeHierarchy(
					typeHierarchy,
					clazz.getComponentType(),
					in1Type,
					in2Type);

				return ObjectArrayTypeInfo.getInfoFor(clazz, componentTypeInfo);
			}
		}

		// check for writable types
		if (isHadoopWritable(clazz)) {
			return createHadoopWritableTypeInfo(clazz);
		}

		// check for basic types
		TypeInformation<OUT> basicTypeInfo = BasicTypeInfo.getInfoFor(clazz);
		if (basicTypeInfo != null) {
			return basicTypeInfo;
		}

		// check for SQL time types
		TypeInformation<OUT> timeTypeInfo = SqlTimeTypeInfo.getInfoFor(clazz);
		if (timeTypeInfo != null) {
			return timeTypeInfo;
		}

		// check for subclasses of Value
		if (Value.class.isAssignableFrom(clazz)) {
			Class<? extends Value> valueClass = clazz.asSubclass(Value.class);
			return (TypeInformation<OUT>) ValueTypeInfo.getValueTypeInfo(valueClass);
		}

		// check for subclasses of Tuple
		if (Tuple.class.isAssignableFrom(clazz)) {
			if(clazz == Tuple0.class) {
				return new TupleTypeInfo(Tuple0.class);
			}
			throw new InvalidTypesException("Type information extraction for tuples (except Tuple0) cannot be done based on the class.");
		}

		// check for Enums
		if(Enum.class.isAssignableFrom(clazz)) {
			return new EnumTypeInfo(clazz);
		}

		// special case for POJOs generated by Avro.
		if (hasSuperclass(clazz, AVRO_SPECIFIC_RECORD_BASE_CLASS)) {
			return AvroUtils.getAvroUtils().createAvroTypeInfo(clazz);
		}

		if (Modifier.isInterface(clazz.getModifiers())) {
			// Interface has no members and is therefore not handled as POJO
			return new GenericTypeInfo<OUT>(clazz);
		}

		try {
			TypeInformation<OUT> pojoType = analyzePojo(clazz, new ArrayList<Type>(typeHierarchy), parameterizedType, in1Type, in2Type);
			if (pojoType != null) {
				return pojoType;
			}
		} catch (InvalidTypesException e) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Unable to handle type "+clazz+" as POJO. Message: "+e.getMessage(), e);
			}
			// ignore and create generic type info
		}

		// return a generic type
		return new GenericTypeInfo<OUT>(clazz);
	}

	/**
	 * Checks if the given field is a valid pojo field:
	 * - it is public
	 * OR
	 *  - there are getter and setter methods for the field.
	 *
	 * @param f field to check
	 * @param clazz class of field
	 * @param typeHierarchy type hierarchy for materializing generic types
	 */
	private boolean isValidPojoField(Field f, Class<?> clazz, ArrayList<Type> typeHierarchy) {
		if(Modifier.isPublic(f.getModifiers())) {
			return true;
		} else {
			boolean hasGetter = false, hasSetter = false;
			final String fieldNameLow = f.getName().toLowerCase().replaceAll("_", "");

			Type fieldType = f.getGenericType();
			Class<?> fieldTypeWrapper = ClassUtils.primitiveToWrapper(f.getType());

			TypeVariable<?> fieldTypeGeneric = null;
			if(fieldType instanceof TypeVariable) {
				fieldTypeGeneric = (TypeVariable<?>) fieldType;
				fieldType = materializeTypeVariable(typeHierarchy, (TypeVariable<?>)fieldType);
			}
			for(Method m : clazz.getMethods()) {
				final String methodNameLow = m.getName().endsWith("_$eq") ?
						m.getName().toLowerCase().replaceAll("_", "").replaceFirst("\\$eq$", "_\\$eq") :
						m.getName().toLowerCase().replaceAll("_", "");

				// check for getter
				if(	// The name should be "get<FieldName>" or "<fieldName>" (for scala) or "is<fieldName>" for boolean fields.
					(methodNameLow.equals("get"+fieldNameLow) || methodNameLow.equals("is"+fieldNameLow) || methodNameLow.equals(fieldNameLow)) &&
					// no arguments for the getter
					m.getParameterTypes().length == 0 &&
					// return type is same as field type (or the generic variant of it)
					(m.getGenericReturnType().equals( fieldType ) || (fieldTypeWrapper != null && m.getReturnType().equals( fieldTypeWrapper )) || (fieldTypeGeneric != null && m.getGenericReturnType().equals(fieldTypeGeneric)) )
				) {
					hasGetter = true;
				}
				// check for setters (<FieldName>_$eq for scala)
				if((methodNameLow.equals("set"+fieldNameLow) || methodNameLow.equals(fieldNameLow+"_$eq")) &&
					m.getParameterTypes().length == 1 && // one parameter of the field's type
					(m.getGenericParameterTypes()[0].equals( fieldType ) || (fieldTypeWrapper != null && m.getParameterTypes()[0].equals( fieldTypeWrapper )) || (fieldTypeGeneric != null && m.getGenericParameterTypes()[0].equals(fieldTypeGeneric) ) )&&
					// return type is void.
					m.getReturnType().equals(Void.TYPE)
				) {
					hasSetter = true;
				}
			}
			if(hasGetter && hasSetter) {
				return true;
			} else {
				if(!hasGetter) {
					LOG.info(clazz+" does not contain a getter for field "+f.getName() );
				}
				if(!hasSetter) {
					LOG.info(clazz+" does not contain a setter for field "+f.getName() );
				}
				return false;
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected <OUT, IN1, IN2> TypeInformation<OUT> analyzePojo(Class<OUT> clazz, ArrayList<Type> typeHierarchy,
			ParameterizedType parameterizedType, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {

		if (!Modifier.isPublic(clazz.getModifiers())) {
			LOG.info("Class " + clazz.getName() + " is not public so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			return new GenericTypeInfo<OUT>(clazz);
		}

		// add the hierarchy of the POJO itself if it is generic
		if (parameterizedType != null) {
			getTypeHierarchy(typeHierarchy, parameterizedType, Object.class);
		}
		// create a type hierarchy, if the incoming only contains the most bottom one or none.
		else if (typeHierarchy.size() <= 1) {
			getTypeHierarchy(typeHierarchy, clazz, Object.class);
		}

		List<Field> fields = getAllDeclaredFields(clazz, false);
		if (fields.size() == 0) {
			LOG.info("No fields were detected for " + clazz + " so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			return new GenericTypeInfo<OUT>(clazz);
		}

		List<PojoField> pojoFields = new ArrayList<PojoField>();
		for (Field field : fields) {
			Type fieldType = field.getGenericType();
			if(!isValidPojoField(field, clazz, typeHierarchy)) {
				LOG.info("Class " + clazz + " cannot be used as a POJO type because not all fields are valid POJO fields, " +
					"and must be processed as GenericType. Please read the Flink documentation " +
					"on \"Data Types & Serialization\" for details of the effect on performance.");
				return null;
			}
			try {
				ArrayList<Type> fieldTypeHierarchy = new ArrayList<Type>(typeHierarchy);
				fieldTypeHierarchy.add(fieldType);
				TypeInformation<?> ti = createTypeInfoWithTypeHierarchy(fieldTypeHierarchy, fieldType, in1Type, in2Type);
				pojoFields.add(new PojoField(field, ti));
			} catch (InvalidTypesException e) {
				Class<?> genericClass = Object.class;
				if(isClassType(fieldType)) {
					genericClass = typeToClass(fieldType);
				}
				pojoFields.add(new PojoField(field, new GenericTypeInfo<OUT>((Class<OUT>) genericClass)));
			}
		}

		CompositeType<OUT> pojoType = new PojoTypeInfo<OUT>(clazz, pojoFields);

		//
		// Validate the correctness of the pojo.
		// returning "null" will result create a generic type information.
		//
		List<Method> methods = getAllDeclaredMethods(clazz);
		for (Method method : methods) {
			if (method.getName().equals("readObject") || method.getName().equals("writeObject")) {
				LOG.info("Class " + clazz + " contains custom serialization methods we do not call, so it cannot be used as a POJO type " +
					"and must be processed as GenericType. Please read the Flink documentation " +
					"on \"Data Types & Serialization\" for details of the effect on performance.");
				return null;
			}
		}

		// Try retrieving the default constructor, if it does not have one
		// we cannot use this because the serializer uses it.
		Constructor defaultConstructor = null;
		try {
			defaultConstructor = clazz.getDeclaredConstructor();
		} catch (NoSuchMethodException e) {
			if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
				LOG.info(clazz + " is abstract or an interface, having a concrete " +
						"type can increase performance.");
			} else {
				LOG.info(clazz + " is missing a default constructor so it cannot be used as a POJO type " +
					"and must be processed as GenericType. Please read the Flink documentation " +
					"on \"Data Types & Serialization\" for details of the effect on performance.");
				return null;
			}
		}
		if(defaultConstructor != null && !Modifier.isPublic(defaultConstructor.getModifiers())) {
			LOG.info("The default constructor of " + clazz + " is not Public so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			return null;
		}

		// everything is checked, we return the pojo
		return pojoType;
	}

	/**
	 * Recursively determine all declared fields
	 * This is required because class.getFields() is not returning fields defined
	 * in parent classes.
	 *
	 * @param clazz class to be analyzed
	 * @param ignoreDuplicates if true, in case of duplicate field names only the lowest one
	 *                            in a hierarchy will be returned; throws an exception otherwise
	 * @return list of fields
	 */
	@PublicEvolving
	public static List<Field> getAllDeclaredFields(Class<?> clazz, boolean ignoreDuplicates) {
		List<Field> result = new ArrayList<Field>();
		while (clazz != null) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				if(Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
					continue; // we have no use for transient or static fields
				}
				if(hasFieldWithSameName(field.getName(), result)) {
					if (ignoreDuplicates) {
						continue;
					} else {
						throw new InvalidTypesException("The field "+field+" is already contained in the hierarchy of the "+clazz+"."
							+ "Please use unique field names through your classes hierarchy");
					}
				}
				result.add(field);
			}
			clazz = clazz.getSuperclass();
		}
		return result;
	}

	@PublicEvolving
	public static Field getDeclaredField(Class<?> clazz, String name) {
		for (Field field : getAllDeclaredFields(clazz, true)) {
			if (field.getName().equals(name)) {
				return field;
			}
		}
		return null;
	}

	private static boolean hasFieldWithSameName(String name, List<Field> fields) {
		for(Field field : fields) {
			if(name.equals(field.getName())) {
				return true;
			}
		}
		return false;
	}

	private static TypeInformation<?> getTypeOfPojoField(TypeInformation<?> pojoInfo, Field field) {
		for (int j = 0; j < pojoInfo.getArity(); j++) {
			PojoField pf = ((PojoTypeInfo<?>) pojoInfo).getPojoFieldAt(j);
			if (pf.getField().getName().equals(field.getName())) {
				return pf.getTypeInformation();
			}
		}
		return null;
	}

	public static <X> TypeInformation<X> getForObject(X value) {
		return new TypeExtractor().privateGetForObject(value);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <X> TypeInformation<X> privateGetForObject(X value) {
		checkNotNull(value);

		// check if type information can be produced using a factory
		final ArrayList<Type> typeHierarchy = new ArrayList<>();
		typeHierarchy.add(value.getClass());
		final TypeInformation<X> typeFromFactory = createTypeInfoFromFactory(value.getClass(), typeHierarchy, null, null);
		if (typeFromFactory != null) {
			return typeFromFactory;
		}

		// check if we can extract the types from tuples, otherwise work with the class
		if (value instanceof Tuple) {
			Tuple t = (Tuple) value;
			int numFields = t.getArity();
			if(numFields != countFieldsInClass(value.getClass())) {
				// not a tuple since it has more fields.
				return analyzePojo((Class<X>) value.getClass(), new ArrayList<Type>(), null, null, null); // we immediately call analyze Pojo here, because
				// there is currently no other type that can handle such a class.
			}

			TypeInformation<?>[] infos = new TypeInformation[numFields];
			for (int i = 0; i < numFields; i++) {
				Object field = t.getField(i);

				if (field == null) {
					throw new InvalidTypesException("Automatic type extraction is not possible on candidates with null values. "
							+ "Please specify the types directly.");
				}

				infos[i] = privateGetForObject(field);
			}
			return new TupleTypeInfo(value.getClass(), infos);
		}
		else if (value instanceof Row) {
			Row row = (Row) value;
			int arity = row.getArity();
			for (int i = 0; i < arity; i++) {
				if (row.getField(i) == null) {
					LOG.warn("Cannot extract type of Row field, because of Row field[" + i + "] is null. " +
						"Should define RowTypeInfo explicitly.");
					return privateGetForClass((Class<X>) value.getClass(), new ArrayList<Type>());
				}
			}
			TypeInformation<?>[] typeArray = new TypeInformation<?>[arity];
			for (int i = 0; i < arity; i++) {
				typeArray[i] = TypeExtractor.getForObject(row.getField(i));
			}
			return (TypeInformation<X>) new RowTypeInfo(typeArray);
		}
		else {
			return privateGetForClass((Class<X>) value.getClass(), new ArrayList<Type>());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities to handle Hadoop's 'Writable' type via reflection
	// ------------------------------------------------------------------------

	// visible for testing
	static boolean isHadoopWritable(Class<?> typeClass) {
		// check if this is directly the writable interface
		if (typeClass.getName().equals(HADOOP_WRITABLE_CLASS)) {
			return false;
		}

		final HashSet<Class<?>> alreadySeen = new HashSet<>();
		alreadySeen.add(typeClass);
		return hasHadoopWritableInterface(typeClass, alreadySeen);
	}

	private static boolean hasHadoopWritableInterface(Class<?> clazz,  HashSet<Class<?>> alreadySeen) {
		Class<?>[] interfaces = clazz.getInterfaces();
		for (Class<?> c : interfaces) {
			if (c.getName().equals(HADOOP_WRITABLE_CLASS)) {
				return true;
			}
			else if (alreadySeen.add(c) && hasHadoopWritableInterface(c, alreadySeen)) {
				return true;
			}
		}

		Class<?> superclass = clazz.getSuperclass();
		return superclass != null && alreadySeen.add(superclass) && hasHadoopWritableInterface(superclass, alreadySeen);
	}

	// visible for testing
	public static <T> TypeInformation<T> createHadoopWritableTypeInfo(Class<T> clazz) {
		checkNotNull(clazz);

		Class<?> typeInfoClass;
		try {
			typeInfoClass = Class.forName(HADOOP_WRITABLE_TYPEINFO_CLASS, false, TypeExtractor.class.getClassLoader());
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load the TypeInformation for the class '"
					+ HADOOP_WRITABLE_CLASS + "'. You may be missing the 'flink-hadoop-compatibility' dependency.");
		}

		try {
			Constructor<?> constr = typeInfoClass.getConstructor(Class.class);

			@SuppressWarnings("unchecked")
			TypeInformation<T> typeInfo = (TypeInformation<T>) constr.newInstance(clazz);
			return typeInfo;
		}
		catch (NoSuchMethodException | IllegalAccessException | InstantiationException e) {
			throw new RuntimeException("Incompatible versions of the Hadoop Compatibility classes found.");
		}
		catch (InvocationTargetException e) {
			throw new RuntimeException("Cannot create Hadoop WritableTypeInfo.", e.getTargetException());
		}
	}

	// visible for testing
	static void validateIfWritable(TypeInformation<?> typeInfo, Type type) {
		try {
			// try to load the writable type info

			Class<?> writableTypeInfoClass = Class
					.forName(HADOOP_WRITABLE_TYPEINFO_CLASS, false, typeInfo.getClass().getClassLoader());

			if (writableTypeInfoClass.isAssignableFrom(typeInfo.getClass())) {
				// this is actually a writable type info
				// check if the type is a writable
				if (!(type instanceof Class && isHadoopWritable((Class<?>) type))) {
					throw new InvalidTypesException(HADOOP_WRITABLE_CLASS + " type expected.");
				}

				// check writable type contents
				Class<?> clazz = (Class<?>) type;
				if (typeInfo.getTypeClass() != clazz) {
					throw new InvalidTypesException("Writable type '"
							+ typeInfo.getTypeClass().getCanonicalName() + "' expected but was '"
							+ clazz.getCanonicalName() + "'.");
				}
			}
		}
		catch (ClassNotFoundException e) {
			// class not present at all, so cannot be that type info
			// ignore
		}
	}
}
