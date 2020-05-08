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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
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

import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.checkAndExtractLambda;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getAllDeclaredMethods;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.hasSuperclass;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.sameTypeVars;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility for reflection analysis on classes, to determine the return type of implementations of transformation
 * functions.
 *
 * <p>NOTES FOR USERS OF THIS CLASS:
 * Automatic type extraction is a hacky business that depends on a lot of variables such as generics,
 * compiler, interfaces, etc. The type extraction fails regularly with either {@link MissingTypeInfo} or
 * hard exceptions. Whenever you use methods of this class, make sure to provide a way to pass custom
 * type information as a fallback.
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

	/** The name of the class representing Hadoop's writable. */
	private static final String HADOOP_WRITABLE_CLASS = "org.apache.hadoop.io.Writable";

	private static final String HADOOP_WRITABLE_TYPEINFO_CLASS = "org.apache.flink.api.java.typeutils.WritableTypeInfo";

	private static final String AVRO_SPECIFIC_RECORD_BASE_CLASS = "org.apache.avro.specific.SpecificRecordBase";

	private static final Logger LOG = LoggerFactory.getLogger(TypeExtractor.class);

	public static final int[] NO_INDEX = new int[] {};

	// --------------------------------------------------------------------------------------------
	//  Function specific methods
	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(
		MapFunction<IN, OUT> mapInterface,
		TypeInformation<IN> inType) {

		return getMapReturnTypes(mapInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(
		MapFunction<IN, OUT> mapInterface,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			mapInterface,
			MapFunction.class,
			0,
			1,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(
		FlatMapFunction<IN, OUT> flatMapInterface,
		TypeInformation<IN> inType) {

		return getFlatMapReturnTypes(flatMapInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(
		FlatMapFunction<IN, OUT> flatMapInterface,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			flatMapInterface,
			FlatMapFunction.class,
			0,
			1,
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
	public static <IN, OUT> TypeInformation<OUT> getFoldReturnTypes(
		FoldFunction<IN, OUT> foldInterface,
		TypeInformation<IN> inType) {

		return getFoldReturnTypes(foldInterface, inType, null, false);
	}

	/**
	 * @deprecated will be removed in a future version
	 */
	@PublicEvolving
	@Deprecated
	public static <IN, OUT> TypeInformation<OUT> getFoldReturnTypes(
		FoldFunction<IN, OUT> foldInterface,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			foldInterface,
			FoldFunction.class,
			0,
			1,
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
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			function,
			AggregateFunction.class,
			0,
			1,
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
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			function,
			AggregateFunction.class,
			0,
			2,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapPartitionReturnTypes(
		MapPartitionFunction<IN, OUT> mapPartitionInterface,
		TypeInformation<IN> inType) {

		return getMapPartitionReturnTypes(mapPartitionInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapPartitionReturnTypes(
		MapPartitionFunction<IN, OUT> mapPartitionInterface, TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			mapPartitionInterface,
			MapPartitionFunction.class,
			0,
			1,
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(
		GroupReduceFunction<IN, OUT> groupReduceInterface,
		TypeInformation<IN> inType) {

		return getGroupReduceReturnTypes(groupReduceInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(
		GroupReduceFunction<IN, OUT> groupReduceInterface, TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			groupReduceInterface,
			GroupReduceFunction.class,
			0,
			1,
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupCombineReturnTypes(
		GroupCombineFunction<IN, OUT> combineInterface,
		TypeInformation<IN> inType) {

		return getGroupCombineReturnTypes(combineInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupCombineReturnTypes(
		GroupCombineFunction<IN, OUT> combineInterface, TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			combineInterface,
			GroupCombineFunction.class,
			0,
			1,
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getFlatJoinReturnTypes(
		FlatJoinFunction<IN1, IN2, OUT> joinInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		return getFlatJoinReturnTypes(joinInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getFlatJoinReturnTypes(
		FlatJoinFunction<IN1, IN2, OUT> joinInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		return getBinaryOperatorReturnType(
			joinInterface,
			FlatJoinFunction.class,
			0,
			1,
			2,
			new int[]{2, 0},
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(
		JoinFunction<IN1, IN2, OUT> joinInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		return getJoinReturnTypes(joinInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(
		JoinFunction<IN1, IN2, OUT> joinInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		return getBinaryOperatorReturnType(
			joinInterface,
			JoinFunction.class,
			0,
			1,
			2,
			NO_INDEX,
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(
		CoGroupFunction<IN1, IN2, OUT> coGroupInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		return getCoGroupReturnTypes(coGroupInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(
		CoGroupFunction<IN1, IN2, OUT> coGroupInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		return getBinaryOperatorReturnType(
			coGroupInterface,
			CoGroupFunction.class,
			0,
			1,
			2,
			new int[]{2, 0},
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(
		CrossFunction<IN1, IN2, OUT> crossInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		return getCrossReturnTypes(crossInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(
		CrossFunction<IN1, IN2, OUT> crossInterface,
		TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		return getBinaryOperatorReturnType(
			crossInterface,
			CrossFunction.class,
			0,
			1,
			2,
			NO_INDEX,
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(
		KeySelector<IN, OUT> selectorInterface,
		TypeInformation<IN> inType) {

		return getKeySelectorTypes(selectorInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(
		KeySelector<IN, OUT> selectorInterface,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			selectorInterface,
			KeySelector.class,
			0,
			1,
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

		return getUnaryOperatorReturnType(
			partitioner,
			Partitioner.class,
			-1,
			0,
			new int[]{0},
			null,
			functionName,
			allowMissing);
	}

	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <IN> TypeInformation<IN> getInputFormatTypes(InputFormat<IN, ?> inputFormatInterface) {
		if (inputFormatInterface instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<IN>) inputFormatInterface).getProducedType();
		}
		return privateCreateTypeInfo(
			InputFormat.class,
			inputFormatInterface.getClass(),
			0,
			null,
			null);
	}

	// --------------------------------------------------------------------------------------------
	//  Generic extraction methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the unary operator's return type.
	 *
	 * <p>This method can extract a type in 4 different ways:
	 *
	 * <p>1. By using the generics of the base class like {@code MyFunction<X, Y, Z, IN, OUT>}.
	 *    This is what outputTypeArgumentIndex (in this example "4") is good for.
	 *
	 * <p>2. By using input type inference {@code SubMyFunction<T, String, String, String, T>}.
	 *    This is what inputTypeArgumentIndex (in this example "0") and inType is good for.
	 *
	 * <p>3. By using the static method that a compiler generates for Java lambdas.
	 *    This is what lambdaOutputTypeArgumentIndices is good for. Given that MyFunction has
	 *    the following single abstract method:
	 *
	 * <pre>
	 * {@code void apply(IN value, Collector<OUT> value)}
	 * </pre>
	 *
	 * <p>Lambda type indices allow the extraction of a type from lambdas. To extract the
	 *    output type <b>OUT</b> from the function one should pass {@code new int[] {1, 0}}.
	 *    "1" for selecting the parameter and 0 for the first generic in this type.
	 *    Use {@code TypeExtractor.NO_INDEX} for selecting the return type of the lambda for
	 *    extraction or if the class cannot be a lambda because it is not a single abstract
	 *    method interface.
	 *
	 * <p>4. By using interfaces such as {@link TypeInfoFactory} or {@link ResultTypeQueryable}.
	 *
	 * <p>See also comments in the header of this class.
	 *
	 * @param function Function to extract the return type from
	 * @param baseClass Base class of the function
	 * @param inputTypeArgumentIndex Index of input generic type in the base class specification (ignored if inType is null)
	 * @param outputTypeArgumentIndex Index of output generic type in the base class specification
	 * @param lambdaOutputTypeArgumentIndices Table of indices of the type argument specifying the input type. See example.
	 * @param inType Type of the input elements (In case of an iterable, it is the element type) or null
	 * @param functionName Function name
	 * @param allowMissing Can the type information be missing (this generates a MissingTypeInfo for postponing an exception)
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
		int[] lambdaOutputTypeArgumentIndices,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		Preconditions.checkArgument(inType == null || inputTypeArgumentIndex >= 0, "Input type argument index was not provided");
		Preconditions.checkArgument(outputTypeArgumentIndex >= 0, "Output type argument index was not provided");
		Preconditions.checkArgument(
			lambdaOutputTypeArgumentIndices != null,
			"Indices for output type arguments within lambda not provided");

		// explicit result type has highest precedence
		if (function instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) function).getProducedType();
		}

		// perform extraction
		try {
			final LambdaExecutable exec;
			try {
				exec = checkAndExtractLambda(function);
			} catch (TypeExtractionException e) {
				throw new InvalidTypesException("Internal error occurred.", e);
			}
			if (exec != null) {

				// parameters must be accessed from behind, since JVM can add additional parameters e.g. when using local variables inside lambda function
				// paramLen is the total number of parameters of the provided lambda, it includes parameters added through closure
				final int paramLen = exec.getParameterTypes().length;

				final Method sam = TypeExtractionUtils.getSingleAbstractMethod(baseClass);

				// number of parameters the SAM of implemented interface has; the parameter indexing applies to this range
				final int baseParametersLen = sam.getParameterTypes().length;

				final Type output;
				if (lambdaOutputTypeArgumentIndices.length > 0) {
					output = TypeExtractionUtils.extractTypeFromLambda(
						baseClass,
						exec,
						lambdaOutputTypeArgumentIndices,
						paramLen,
						baseParametersLen);
				} else {
					output = exec.getReturnType();
					TypeExtractionUtils.validateLambdaType(baseClass, output);
				}

				return (TypeInformation<OUT>) createTypeInfo(output, Collections.emptyMap(), Collections.emptyList());
			} else {
				return privateCreateTypeInfo(baseClass, function.getClass(), outputTypeArgumentIndex, inType, null);
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
	 * <p>This method can extract a type in 4 different ways:
	 *
	 * <p>1. By using the generics of the base class like {@code MyFunction<X, Y, Z, IN, OUT>}.
	 *    This is what outputTypeArgumentIndex (in this example "4") is good for.
	 *
	 * <p>2. By using input type inference {@code SubMyFunction<T, String, String, String, T>}.
	 *    This is what inputTypeArgumentIndex (in this example "0") and inType is good for.
	 *
	 * <p>3. By using the static method that a compiler generates for Java lambdas.
	 *    This is what lambdaOutputTypeArgumentIndices is good for. Given that MyFunction has
	 *    the following single abstract method:
	 *
	 * <pre>
	 * {@code void apply(IN value, Collector<OUT> value) }
	 * </pre>
	 *
	 * <p>Lambda type indices allow the extraction of a type from lambdas. To extract the
	 *    output type <b>OUT</b> from the function one should pass {@code new int[] {1, 0}}.
	 *    "1" for selecting the parameter and 0 for the first generic in this type.
	 *    Use {@code TypeExtractor.NO_INDEX} for selecting the return type of the lambda for
	 *    extraction or if the class cannot be a lambda because it is not a single abstract
	 *    method interface.
	 *
	 * <p>4. By using interfaces such as {@link TypeInfoFactory} or {@link ResultTypeQueryable}.
	 *
	 * <p>See also comments in the header of this class.
	 *
	 * @param function Function to extract the return type from
	 * @param baseClass Base class of the function
	 * @param input1TypeArgumentIndex Index of first input generic type in the class specification (ignored if in1Type is null)
	 * @param input2TypeArgumentIndex Index of second input generic type in the class specification (ignored if in2Type is null)
	 * @param outputTypeArgumentIndex Index of output generic type in the class specification
	 * @param lambdaOutputTypeArgumentIndices Table of indices of the type argument specifying the output type. See example.
	 * @param in1Type Type of the left side input elements (In case of an iterable, it is the element type)
	 * @param in2Type Type of the right side input elements (In case of an iterable, it is the element type)
	 * @param functionName Function name
	 * @param allowMissing Can the type information be missing (this generates a MissingTypeInfo for postponing an exception)
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
		int[] lambdaOutputTypeArgumentIndices,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		Preconditions.checkArgument(in1Type == null || input1TypeArgumentIndex >= 0, "Input 1 type argument index was not provided");
		Preconditions.checkArgument(in2Type == null || input2TypeArgumentIndex >= 0, "Input 2 type argument index was not provided");
		Preconditions.checkArgument(outputTypeArgumentIndex >= 0, "Output type argument index was not provided");
		Preconditions.checkArgument(
			lambdaOutputTypeArgumentIndices != null,
			"Indices for output type arguments within lambda not provided");

		// explicit result type has highest precedence
		if (function instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) function).getProducedType();
		}

		// perform extraction
		try {
			final LambdaExecutable exec;
			try {
				exec = checkAndExtractLambda(function);
			} catch (TypeExtractionException e) {
				throw new InvalidTypesException("Internal error occurred.", e);
			}
			if (exec != null) {

				final Method sam = TypeExtractionUtils.getSingleAbstractMethod(baseClass);
				final int baseParametersLen = sam.getParameterTypes().length;

				// parameters must be accessed from behind, since JVM can add additional parameters e.g.
				// when using local variables inside lambda function
				final int paramLen = exec.getParameterTypes().length;

				final Type output;
				if (lambdaOutputTypeArgumentIndices.length > 0) {
					output = TypeExtractionUtils.extractTypeFromLambda(
						baseClass,
						exec,
						lambdaOutputTypeArgumentIndices,
						paramLen,
						baseParametersLen);
				} else {
					output = exec.getReturnType();
					TypeExtractionUtils.validateLambdaType(baseClass, output);
				}

				return (TypeInformation<OUT>) createTypeInfo(output, Collections.emptyMap(), Collections.emptyList());
			}
			else {
				return privateCreateTypeInfo(baseClass, function.getClass(), outputTypeArgumentIndex, in1Type, in2Type);
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
		TypeInformation<?> ti = createTypeInfo(t, Collections.emptyMap(), Collections.emptyList());
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}

	/**
	 * Creates a {@link TypeInformation} from the given parameters.
	 *
	 * <p>If the given {@code instance} implements {@link ResultTypeQueryable}, its information
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
	public static <OUT> TypeInformation<OUT> createTypeInfo(
		Object instance,
		Class<?> baseClass,
		Class<?> clazz,
		int returnParamPos) {

		if (instance instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) instance).getProducedType();
		} else {
			return createTypeInfo(baseClass, clazz, returnParamPos, null, null);
		}
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfo(
		Class<?> baseClass,
		Class<?> clazz,
		int returnParamPos,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		TypeInformation<OUT> ti =  privateCreateTypeInfo(baseClass, clazz, returnParamPos, in1Type, in2Type);
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}

	// ----------------------------------- private methods ----------------------------------------

	// for (Rich)Functions
	@SuppressWarnings("unchecked")
	private static <IN1, IN2, OUT> TypeInformation<OUT> privateCreateTypeInfo(
		Class<?> baseClass,
		Class<?> clazz,
		int returnParamPos,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		final Type returnType = getParameterType(baseClass, clazz, returnParamPos);

		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings =
			bindTypeVariablesWithTypeInformationFromInputs(clazz, Function.class, in1Type, 0, in2Type, 1);

		// return type is a variable -> try to get the type info from the input directly
		if (returnType instanceof TypeVariable<?>) {
			final TypeInformation<OUT> typeInfo = (TypeInformation<OUT>) typeVariableBindings.get(returnType);
			if (typeInfo != null) {
				return typeInfo;
			}
		}

		// get info from hierarchy
		return (TypeInformation<OUT>) createTypeInfo(returnType, typeVariableBindings, Collections.emptyList());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <OUT> TypeInformation<OUT> createTypeInfo(
		final Type t,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		final List<Class<?>> currentExtractingClasses;
		if (isClassType(t)) {
			currentExtractingClasses = new ArrayList(extractingClasses);
			currentExtractingClasses.add(typeToClass(t));
		} else {
			currentExtractingClasses = extractingClasses;
		}

		// check if type information can be created using a type factory
		final TypeInformation<OUT> typeFromFactory =
			createTypeInfoFromFactory(t, typeVariableBindings, currentExtractingClasses);
		if (typeFromFactory != null) {
			return typeFromFactory;
		}
		// check if type is a subclass of tuple
		else if (isClassType(t) && Tuple.class.isAssignableFrom(typeToClass(t))) {
			final List<ParameterizedType> typeHierarchy = new ArrayList<>();

			Type curT = t;

			// do not allow usage of Tuple as type
			if (typeToClass(t).equals(Tuple.class)) {
				throw new InvalidTypesException(
						"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
			}

			// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
			// collect the types while moving up for a later top-down
			while (!(isClassType(curT) && typeToClass(curT).getSuperclass().equals(Tuple.class))) {
				if (curT instanceof ParameterizedType) {
					typeHierarchy.add((ParameterizedType) curT);
				}
				curT = typeToClass(curT).getGenericSuperclass();
			}

			if (curT == Tuple0.class) {
				return new TupleTypeInfo(Tuple0.class);
			}

			// check if immediate child of Tuple has generics
			if (curT instanceof Class<?>) {
				throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
			}

			if (curT instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) curT);
			}

			curT = resolveTypeFromTypeHierarchy(curT, typeHierarchy, true);

			// create the type information for the subtypes
			final TypeInformation<?>[] subTypesInfo =
				createSubTypesInfo(t, (ParameterizedType) curT, typeVariableBindings, extractingClasses, false);
			// type needs to be treated a pojo due to additional fields
			if (subTypesInfo == null) {
				if (t instanceof ParameterizedType) {
					return (TypeInformation<OUT>) analyzePojo(typeToClass(t), (ParameterizedType) t, typeVariableBindings, extractingClasses);
				}
				else {
					return (TypeInformation<OUT>) analyzePojo(typeToClass(t), null, typeVariableBindings, extractingClasses);
				}
			}
			// return tuple info
			return new TupleTypeInfo(typeToClass(t), subTypesInfo);

		}
		// type depends on another type
		// e.g. class MyMapper<E> extends MapFunction<String, E>
		else if (t instanceof TypeVariable) {
			final TypeInformation<OUT> typeInfo = (TypeInformation<OUT>) typeVariableBindings.get(t);
			if (typeInfo != null) {
				return typeInfo;
			} else {
				throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) t).getName() + "' in '"
					+ ((TypeVariable<?>) t).getGenericDeclaration() + "' could not be determined. This is most likely a type erasure problem. "
					+ "The type extraction currently supports types with generic variables only in cases where "
					+ "all variables in the return type can be deduced from the input type(s). "
					+ "Otherwise the type has to be specified explicitly using type information.");
			}
		}
		// arrays with generics
		else if (t instanceof GenericArrayType) {
			GenericArrayType genericArray = (GenericArrayType) t;

			Type componentType = genericArray.getGenericComponentType();

			// due to a Java 6 bug, it is possible that the JVM classifies e.g. String[] or int[] as GenericArrayType instead of Class
			if (componentType instanceof Class) {
				Class<?> componentClass = (Class<?>) componentType;

				Class<OUT> classArray =
					(Class<OUT>) (java.lang.reflect.Array.newInstance(componentClass, 0).getClass());

				return getForClass(classArray);
			} else {
				TypeInformation<?> componentInfo = createTypeInfo(
					genericArray.getGenericComponentType(),
					typeVariableBindings,
					extractingClasses);

				Class<?> componentClass = componentInfo.getTypeClass();
				Class<OUT> classArray =
					(Class<OUT>) (java.lang.reflect.Array.newInstance(componentClass, 0).getClass());

				return ObjectArrayTypeInfo.getInfoFor(classArray, componentInfo);
			}
		}
		// objects with generics are treated as Class first
		else if (t instanceof ParameterizedType) {
			return (TypeInformation<OUT>)
				privateGetForClass(typeToClass(t), (ParameterizedType) t, typeVariableBindings, currentExtractingClasses);
		}
		// no tuple, no TypeVariable, no generic type
		else if (t instanceof Class) {
			return privateGetForClass((Class<OUT>) t, typeVariableBindings, currentExtractingClasses);
		}

		throw new InvalidTypesException("Type Information could not be created.");
	}

	/**
	 * Creates the TypeInformation for all elements of a type that expects a certain number of
	 * subtypes (e.g. TupleXX).
	 *
	 * @param originalType most concrete subclass
	 * @param definingType type that defines the number of subtypes (e.g. Tuple2 -> 2 subtypes)
	 * @param typeVariableBindings the mapping relation between the type variable and the typein formation
	 * @param extractingClasses the classes that are extrating the type information
	 * @param lenient decides whether exceptions should be thrown if a subtype can not be determined
	 * @return array containing TypeInformation of sub types or null if definingType contains
	 *     more subtypes (fields) that defined
	 */
	private static TypeInformation<?>[] createSubTypesInfo(
		Type originalType,
		ParameterizedType definingType,
		Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		List<Class<?>> extractingClasses, boolean lenient) {

		final int typeArgumentsLength = definingType.getActualTypeArguments().length;
		final TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[typeArgumentsLength];
		for (int i = 0; i < typeArgumentsLength; i++) {
			final Type acutalTypeArgument = definingType.getActualTypeArguments()[i];
			// sub type could not be determined with materializing
			// try to derive the type info of the TypeVariable from the immediate base child input as a last attempt
			if (acutalTypeArgument instanceof TypeVariable<?>) {
				subTypesInfo[i] = typeVariableBindings.get(acutalTypeArgument);

				// variable could not be determined
				if (subTypesInfo[i] == null && !lenient) {
					throw new InvalidTypesException("Type of TypeVariable '"
						+ ((TypeVariable<?>) acutalTypeArgument).getName() + "' in '"
						+ ((TypeVariable<?>) acutalTypeArgument).getGenericDeclaration()
						+ "' could not be determined. This is most likely a type erasure problem. "
						+ "The type extraction currently supports types with generic variables only in cases where "
						+ "all variables in the return type can be deduced from the input type(s). "
						+ "Otherwise the type has to be specified explicitly using type information.");
				}
			} else {
				// create the type information of the subtype or null/exception
				try {
					subTypesInfo[i] = createTypeInfo(acutalTypeArgument, typeVariableBindings, extractingClasses);
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
			if (fieldCount > subTypesInfo.length) {
				return null;
			}
		}

		return subTypesInfo;
	}

	/**
	 * Creates type information using a factory if for this type or super types. Returns null otherwise.
	 */
	@SuppressWarnings("unchecked")
	private static <OUT> TypeInformation<OUT> createTypeInfoFromFactory(
		Type t,
		Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		List<Class<?>> extractingClasses) {

		final List<ParameterizedType> factoryHierarchy = new ArrayList<>();
		final TypeInfoFactory<? super OUT> factory = getClosestFactory(factoryHierarchy, t);
		if (factory == null) {
			return null;
		}
		final Type factoryDefiningType = factoryHierarchy.size() < 1 ? t :
			resolveTypeFromTypeHierarchy(factoryHierarchy.get(factoryHierarchy.size() - 1), factoryHierarchy, true);

		// infer possible type parameters from input
		final Map<String, TypeInformation<?>> genericParams;
		if (factoryDefiningType instanceof ParameterizedType) {
			genericParams = new HashMap<>();
			final ParameterizedType paramDefiningType = (ParameterizedType) factoryDefiningType;
			final Type[] args = typeToClass(paramDefiningType).getTypeParameters();

			final TypeInformation<?>[] subtypeInfo =
				createSubTypesInfo(t, paramDefiningType, typeVariableBindings, extractingClasses, true);
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
	//  Utility methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the type information factory for a type using the factory registry or annotations.
	 */
	@Internal
	public static <OUT> TypeInfoFactory<OUT> getTypeInfoFactory(Type t) {
		final Class<?> factoryClass;

		if (!isClassType(t) || !typeToClass(t).isAnnotationPresent(TypeInfo.class)) {
			return null;
		}
		final TypeInfo typeInfoAnnotation = typeToClass(t).getAnnotation(TypeInfo.class);
		factoryClass = typeInfoAnnotation.value();
		// check for valid factory class
		if (!TypeInfoFactory.class.isAssignableFrom(factoryClass)) {
			throw new InvalidTypesException("TypeInfo annotation does not specify a valid TypeInfoFactory.");
		}
		// instantiate
		return (TypeInfoFactory<OUT>) InstantiationUtil.instantiate(factoryClass);
	}

	/**
	 * @return number of items with equal type or same raw type
	 */
	private static int countTypeInHierarchy(List<Class<?>> typeHierarchy, Type type) {
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
	private static <OUT> TypeInfoFactory<? super OUT> getClosestFactory(List<ParameterizedType> typeHierarchy, Type t) {
		TypeInfoFactory factory = null;
		while (factory == null && isClassType(t) && !(typeToClass(t).equals(Object.class))) {
			if (t instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) t);
			}
			factory = getTypeInfoFactory(t);
			t = typeToClass(t).getGenericSuperclass();

			if (t == null) {
				break;
			}
		}
		return factory;
	}

	private static int countFieldsInClass(Class<?> clazz) {
		int fieldCount = 0;
		for (Field field : clazz.getFields()) { // get all fields
			if (!Modifier.isStatic(field.getModifiers()) &&
				!Modifier.isTransient(field.getModifiers())
				) {
				fieldCount++;
			}
		}
		return fieldCount;
	}

	/**
	 * Creates type information from a given Class such as Integer, String[] or POJOs.
	 *
	 * <p>This method does not support ParameterizedTypes such as Tuples or complex type hierarchies.
	 * In most cases {@link TypeExtractor#createTypeInfo(Type)} is the recommended method for type extraction
	 * (a Class is a child of Type).
	 *
	 * @param clazz a Class to create TypeInformation for
	 * @return TypeInformation that describes the passed Class
	 */
	public static <X> TypeInformation<X> getForClass(Class<X> clazz) {
		return privateGetForClass(clazz, Collections.emptyMap(), Collections.emptyList());
	}

	private static <X> TypeInformation<X> privateGetForClass(
		Class<X> clazz,
		Map<TypeVariable<?>, TypeInformation<?>>  typeVariableBindings,
		List<Class<?>> extractingClasses) {

		return privateGetForClass(clazz, null, typeVariableBindings, extractingClasses);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <OUT> TypeInformation<OUT> privateGetForClass(
		Class<OUT> clazz,
		ParameterizedType parameterizedType,
		Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		List<Class<?>> extractingClasses) {
		checkNotNull(clazz);

		// check if type information can be produced using a factory
		final TypeInformation<OUT> typeFromFactory =
			createTypeInfoFromFactory(clazz, typeVariableBindings, extractingClasses);
		if (typeFromFactory != null) {
			return typeFromFactory;
		}

		// Object is handled as generic type info
		if (clazz.equals(Object.class)) {
			return new GenericTypeInfo<>(clazz);
		}

		// Class is handled as generic type info
		if (clazz.equals(Class.class)) {
			return new GenericTypeInfo<>(clazz);
		}

		// recursive types are handled as generic type info
		if (countTypeInHierarchy(extractingClasses, clazz) > 1) {
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
				TypeInformation<?> componentTypeInfo = createTypeInfo(
					clazz.getComponentType(),
					typeVariableBindings,
					extractingClasses);

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
			if (clazz == Tuple0.class) {
				return new TupleTypeInfo(Tuple0.class);
			}
			throw new InvalidTypesException("Type information extraction for tuples (except Tuple0) cannot be done based on the class.");
		}

		// check for Enums
		if (Enum.class.isAssignableFrom(clazz)) {
			return new EnumTypeInfo(clazz);
		}

		// special case for POJOs generated by Avro.
		if (hasSuperclass(clazz, AVRO_SPECIFIC_RECORD_BASE_CLASS)) {
			return AvroUtils.getAvroUtils().createAvroTypeInfo(clazz);
		}

		if (Modifier.isInterface(clazz.getModifiers())) {
			// Interface has no members and is therefore not handled as POJO
			return new GenericTypeInfo<>(clazz);
		}

		try {
			TypeInformation<OUT> pojoType = analyzePojo(clazz, parameterizedType, typeVariableBindings, extractingClasses);
			if (pojoType != null) {
				return pojoType;
			}
		} catch (InvalidTypesException e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Unable to handle type " + clazz + " as POJO. Message: " + e.getMessage(), e);
			}
			// ignore and create generic type info
		}

		// return a generic type
		return new GenericTypeInfo<>(clazz);
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
	private static boolean isValidPojoField(Field f, Class<?> clazz, List<ParameterizedType> typeHierarchy) {
		if (Modifier.isPublic(f.getModifiers())) {
			return true;
		} else {
			boolean hasGetter = false, hasSetter = false;
			final String fieldNameLow = f.getName().toLowerCase().replaceAll("_", "");

			Type fieldType = f.getGenericType();
			Class<?> fieldTypeWrapper = ClassUtils.primitiveToWrapper(f.getType());

			TypeVariable<?> fieldTypeGeneric = null;
			if (fieldType instanceof TypeVariable) {
				fieldTypeGeneric = (TypeVariable<?>) fieldType;
				fieldType = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) fieldType);
			}
			for (Method m : clazz.getMethods()) {
				final String methodNameLow = m.getName().endsWith("_$eq") ?
						m.getName().toLowerCase().replaceAll("_", "").replaceFirst("\\$eq$", "_\\$eq") :
						m.getName().toLowerCase().replaceAll("_", "");

				// check for getter
				if (// The name should be "get<FieldName>" or "<fieldName>" (for scala) or "is<fieldName>" for boolean fields.
					(methodNameLow.equals("get" + fieldNameLow)
						|| methodNameLow.equals("is" + fieldNameLow)
						|| methodNameLow.equals(fieldNameLow))
					&& m.getParameterTypes().length == 0 // no arguments for the getter
					// return type is same as field type (or the generic variant of it)
					&& (m.getGenericReturnType().equals(fieldType)
						|| (m.getReturnType().equals(fieldTypeWrapper))
						|| (fieldTypeGeneric != null && m.getGenericReturnType().equals(fieldTypeGeneric)))) {
					hasGetter = true;
				}
				// check for setters (<FieldName>_$eq for scala)
				if ((methodNameLow.equals("set" + fieldNameLow) || methodNameLow.equals(fieldNameLow + "_$eq"))
					&& m.getParameterTypes().length == 1 // one parameter of the field's type
					&& (m.getGenericParameterTypes()[0].equals(fieldType)
						|| (m.getParameterTypes()[0].equals(fieldTypeWrapper))
						|| (fieldTypeGeneric != null && m.getGenericParameterTypes()[0].equals(fieldTypeGeneric)))
					// return type is void (or the class self).
					&& (m.getReturnType().equals(Void.TYPE) || m.getReturnType().equals(clazz))) {
					hasSetter = true;
				}
			}
			if (hasGetter && hasSetter) {
				return true;
			} else {
				if (!hasGetter) {
					LOG.info(clazz + " does not contain a getter for field " + f.getName());
				}
				if (!hasSetter) {
					LOG.info(clazz + " does not contain a setter for field " + f.getName());
				}
				return false;
			}
		}
	}

	/* make this method public only for AvroTypeInfo */
	@SuppressWarnings("unchecked")
	public static <OUT> TypeInformation<OUT> analyzePojo(
		Class<OUT> clazz,
		ParameterizedType parameterizedType,
		Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		List<Class<?>> extractingClasses) {

		final List<ParameterizedType> typeHierarchy;
		if (!Modifier.isPublic(clazz.getModifiers())) {
			LOG.info("Class " + clazz.getName() + " is not public so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			return new GenericTypeInfo<>(clazz);
		}

		// add the hierarchy of the POJO itself if it is generic
		if (parameterizedType != null) {
			typeHierarchy = buildParameterizedTypeHierarchy(parameterizedType, Object.class);
		} else { // create a type hierarchy, if the incoming only contains the most bottom one or none.
			typeHierarchy = buildParameterizedTypeHierarchy(clazz, Object.class);
		}

		List<Field> fields = getAllDeclaredFields(clazz, false);
		if (fields.size() == 0) {
			LOG.info("No fields were detected for " + clazz + " so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			return new GenericTypeInfo<>(clazz);
		}

		List<PojoField> pojoFields = new ArrayList<>();
		for (Field field : fields) {
			Type fieldType = field.getGenericType();
			if (!isValidPojoField(field, clazz, typeHierarchy)) {
				LOG.info("Class " + clazz + " cannot be used as a POJO type because not all fields are valid POJO fields, " +
					"and must be processed as GenericType. Please read the Flink documentation " +
					"on \"Data Types & Serialization\" for details of the effect on performance.");
				return null;
			}
			try {
				List<ParameterizedType> fieldTypeHierarchy = new ArrayList<>(typeHierarchy);
				Type resolveFieldType = resolveTypeFromTypeHierarchy(fieldType, fieldTypeHierarchy, true);

				TypeInformation<?> ti = createTypeInfo(resolveFieldType, typeVariableBindings, extractingClasses);
				pojoFields.add(new PojoField(field, ti));
			} catch (InvalidTypesException e) {
				Class<?> genericClass = Object.class;
				if (isClassType(fieldType)) {
					genericClass = typeToClass(fieldType);
				}
				pojoFields.add(new PojoField(field, new GenericTypeInfo<>((Class<OUT>) genericClass)));
			}
		}

		CompositeType<OUT> pojoType = new PojoTypeInfo<>(clazz, pojoFields);

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
				LOG.info(clazz + " is abstract or an interface, having a concrete "
					+ "type can increase performance.");
			} else {
				LOG.info(clazz + " is missing a default constructor so it cannot be used as a POJO type "
					+ "and must be processed as GenericType. Please read the Flink documentation "
					+ "on \"Data Types & Serialization\" for details of the effect on performance.");
				return null;
			}
		}
		if (defaultConstructor != null && !Modifier.isPublic(defaultConstructor.getModifiers())) {
			LOG.info("The default constructor of " + clazz + " is not Public so it cannot be used as a POJO type "
				+ "and must be processed as GenericType. Please read the Flink documentation "
				+ "on \"Data Types & Serialization\" for details of the effect on performance.");
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
		List<Field> result = new ArrayList<>();
		while (clazz != null) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				if (Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
					continue; // we have no use for transient or static fields
				}
				if (hasFieldWithSameName(field.getName(), result)) {
					if (ignoreDuplicates) {
						continue;
					} else {
						throw new InvalidTypesException("The field " + field + " is already contained in the hierarchy of the " +
							clazz + ".Please use unique field names through your classes hierarchy");
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
		for (Field field : fields) {
			if (name.equals(field.getName())) {
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

	// --------------------------------------------------------------------------------------------
	//  Create type information for object.
	// --------------------------------------------------------------------------------------------

	public static <X> TypeInformation<X> getForObject(X value) {
		return privateGetForObject(value);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <X> TypeInformation<X> privateGetForObject(X value) {
		checkNotNull(value);

		final List<Class<?>> currentExtractingClasses = Collections.singletonList(value.getClass());
		final TypeInformation<X> typeFromFactory =
			createTypeInfoFromFactory(value.getClass(), Collections.emptyMap(), currentExtractingClasses);
		if (typeFromFactory != null) {
			return typeFromFactory;
		}

		// check if we can extract the types from tuples, otherwise work with the class
		if (value instanceof Tuple) {
			Tuple t = (Tuple) value;
			int numFields = t.getArity();
			if (numFields != countFieldsInClass(value.getClass())) {
				// not a tuple since it has more fields.
				// we immediately call analyze Pojo here, because there is currently no other type that can handle such a class.
				return analyzePojo(
					(Class<X>) value.getClass(),
					null,
					Collections.emptyMap(),
					currentExtractingClasses);
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
					return privateGetForClass((Class<X>) value.getClass(), Collections.emptyMap(), currentExtractingClasses);
				}
			}
			TypeInformation<?>[] typeArray = new TypeInformation<?>[arity];
			for (int i = 0; i < arity; i++) {
				typeArray[i] = TypeExtractor.getForObject(row.getField(i));
			}
			return (TypeInformation<X>) new RowTypeInfo(typeArray);
		}
		else {
			return privateGetForClass((Class<X>) value.getClass(), Collections.emptyMap(), currentExtractingClasses);
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
			typeInfoClass =
				Class.forName(HADOOP_WRITABLE_TYPEINFO_CLASS, false, Thread.currentThread().getContextClassLoader());
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

	// --------------------------------------------------------------------------------------------
	//  Resolve type parameters
	// --------------------------------------------------------------------------------------------

	/**
	 * Resolve the type of the {@code pos}-th generic parameter of the {@code baseClass} from a parameterized type hierarchy
	 * that is built from {@code clazz} to {@code baseClass}. If the {@code pos}-th generic parameter is a generic class the
	 * type of generic parameters of it would also be resolved. This is a recursive resolving process.
	 *
	 * @param baseClass a generic class/interface
	 * @param clazz a sub class of the {@code baseClass}
	 * @param pos the position of generic parameter in the {@code baseClass}
	 * @return the type of the {@code pos}-th generic parameter
	 */
	@PublicEvolving
	public static Type getParameterType(Class<?> baseClass, Class<?> clazz, int pos) {
		final List<ParameterizedType> typeHierarchy = buildParameterizedTypeHierarchy(clazz, baseClass, true);

		if (typeHierarchy.size() < 1) {
			throw new InvalidTypesException("The types of the interface " + baseClass.getName() + " could not be inferred. " +
				"Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point");
		}

		final Type baseClassType =
			resolveTypeFromTypeHierarchy(typeHierarchy.get(typeHierarchy.size() - 1), typeHierarchy, false);

		return ((ParameterizedType) baseClassType).getActualTypeArguments()[pos];
	}

	/**
	 * Build the parameterized type hierarchy from {@code subClass} to {@code baseClass}.
	 * @param subClass the begin class of the type hierarchy
	 * @param baseClass the end class of the type hierarchy
	 * @param traverseInterface whether to traverse the interface type
	 * @return the parameterized type hierarchy.
	 */
	@VisibleForTesting
	static List<ParameterizedType> buildParameterizedTypeHierarchy(
		final Class<?> subClass,
		final Class<?> baseClass,
		final boolean traverseInterface) {

		final List<ParameterizedType> typeHierarchy = new ArrayList<>();

		if (baseClass.equals(subClass) || !baseClass.isAssignableFrom(subClass)) {
			return Collections.emptyList();
		}

		if (traverseInterface) {
			final Type[] interfaceTypes = subClass.getGenericInterfaces();

			for (Type type : interfaceTypes) {
				if (baseClass.isAssignableFrom(typeToClass(type))) {
					final List<ParameterizedType> subTypeHierarchy = buildParameterizedTypeHierarchy(typeToClass(type), baseClass, traverseInterface);
					if (type instanceof ParameterizedType) {
						typeHierarchy.add((ParameterizedType) type);
					}
					typeHierarchy.addAll(subTypeHierarchy);
					return typeHierarchy;
				}
			}
		}

		if (baseClass.isAssignableFrom(subClass)) {
			final Type type = subClass.getGenericSuperclass();
			final List<ParameterizedType> subTypeHierarchy = buildParameterizedTypeHierarchy(typeToClass(type), baseClass, traverseInterface);
			if (type instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) type);
			}
			typeHierarchy.addAll(subTypeHierarchy);

			return typeHierarchy;
		}
		return typeHierarchy.size() == 0 ? Collections.emptyList() : typeHierarchy;
	}

	/**
	 * Build the parameterized type hierarchy from {@code type} to the {@code baseClass}.
	 * @param type the begin type of the type hierarchy
	 * @param baseClass the end type of the type hierarchy
	 * @return the parameterized type hierarchy.
	 */
	private static List<ParameterizedType> buildParameterizedTypeHierarchy(final Type type, final Class<?> baseClass) {
		if (isClassType(type)) {
			final List<ParameterizedType> typeHierarchy = new ArrayList<>();
			if (type instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) type);
			}
			typeHierarchy.addAll(buildParameterizedTypeHierarchy(typeToClass(type), baseClass, false));
			return typeHierarchy.size() == 0 ? Collections.emptyList() : typeHierarchy;
		}

		return Collections.emptyList();
	}

	/**
	 * Resolve all {@link TypeVariable}s of the type from the type hierarchy.
	 * @param type the type needed to be resolved
	 * @param typeHierarchy the set of types which the {@link TypeVariable} could be resovled from.
	 * @param resolveGenericArray whether to resolve the {@code GenericArrayType} or not. This is for compatible.
	 *                               (Some code path resolves the component type of a GenericArrayType. Some code path
	 *                               does not resolve the component type of a GenericArray. A example case is
	 *                               {@code testParameterizedArrays()})
	 * @return resolved type
	 */
	@VisibleForTesting
	static Type resolveTypeFromTypeHierarchy(
		final Type type,
		final List<ParameterizedType> typeHierarchy,
		final boolean resolveGenericArray) {

		Type resolvedType = type;

		if (type instanceof TypeVariable) {
			resolvedType = materializeTypeVariable(typeHierarchy, (TypeVariable) type);
		}

		if (resolvedType instanceof ParameterizedType) {
			return resolveParameterizedType((ParameterizedType) resolvedType, typeHierarchy, resolveGenericArray);
		} else if (resolveGenericArray && resolvedType instanceof GenericArrayType) {
			return resolveGenericArrayType((GenericArrayType) resolvedType, typeHierarchy);
		}

		return resolvedType;
	}

	/**
	 * Resolve all {@link TypeVariable}s of a {@link ParameterizedType}.
	 * @param parameterizedType the {@link ParameterizedType} needed to be resovled.
	 * @param typeHierarchy the set of types which the {@link TypeVariable}s could be resolved from.
	 * @param resolveGenericArray whether to resolve the {@code GenericArrayType} or not. This is for compatible.
	 * @return resolved {@link ParameterizedType}
	 */
	private static Type resolveParameterizedType(
		final ParameterizedType parameterizedType,
		final List<ParameterizedType> typeHierarchy,
		final boolean resolveGenericArray) {

		final Type[] actualTypeArguments = new Type[parameterizedType.getActualTypeArguments().length];

		int i = 0;
		for (Type type : parameterizedType.getActualTypeArguments()) {
			actualTypeArguments[i] = resolveTypeFromTypeHierarchy(type, typeHierarchy, resolveGenericArray);
			i++;
		}

		return new ResolvedParameterizedType(parameterizedType.getRawType(),
			parameterizedType.getOwnerType(),
			actualTypeArguments,
			parameterizedType.getTypeName());
	}

	/**
	 * Tries to find a concrete value (Class, ParameterizedType etc. ) for a TypeVariable by traversing the type
	 * hierarchy downwards.
	 *
	 * @param typeHierarchy the type hierarchy
	 * @param typeVar the type variable needed to be concreted
	 * @return the concrete value or
	 * 		   the most bottom type variable in the hierarchy
	 */
	@VisibleForTesting
	static Type materializeTypeVariable(List<ParameterizedType> typeHierarchy, TypeVariable<?> typeVar) {
		TypeVariable<?> inTypeTypeVar = typeVar;
		// iterate thru hierarchy from top to bottom until type variable gets a class assigned
		for (int i = typeHierarchy.size() - 1; i >= 0; i--) {
			ParameterizedType curT = typeHierarchy.get(i);
			Class<?> rawType = (Class<?>) curT.getRawType();

			for (int paramIndex = 0; paramIndex < rawType.getTypeParameters().length; paramIndex++) {

				TypeVariable<?> curVarOfCurT = rawType.getTypeParameters()[paramIndex];

				// check if variable names match
				if (sameTypeVars(curVarOfCurT, inTypeTypeVar)) {
					Type curVarType = curT.getActualTypeArguments()[paramIndex];

					// another type variable level
					if (curVarType instanceof TypeVariable<?>) {
						inTypeTypeVar = (TypeVariable<?>) curVarType;
					} else {
						return curVarType;
					}
				}
			}
		}
		// can not be materialized, most likely due to type erasure
		// return the type variable of the deepest level
		return inTypeTypeVar;
	}

	/**
	 * Resolve the component type of {@link GenericArrayType}.
	 * @param genericArrayType the {@link GenericArrayType} needed to be resolved.
	 * @param typeHierarchy the set of types which the {@link TypeVariable}s could be resolved from.
	 * @return resolved {@link GenericArrayType}
	 */
	private static Type resolveGenericArrayType(final GenericArrayType genericArrayType, final List<ParameterizedType> typeHierarchy) {

		final Type resolvedComponentType =
			resolveTypeFromTypeHierarchy(genericArrayType.getGenericComponentType(), typeHierarchy, true);

		return new ResolvedGenericArrayType(genericArrayType.getTypeName(), resolvedComponentType);
	}

	private static class ResolvedGenericArrayType implements GenericArrayType {

		private final Type componentType;

		private final String typeName;

		ResolvedGenericArrayType(String typeName, Type componentType) {
			this.componentType = componentType;
			this.typeName = typeName;
		}

		@Override
		public Type getGenericComponentType() {
			return componentType;
		}

		public String getTypeName() {
			return typeName;
		}
	}

	private static class ResolvedParameterizedType implements ParameterizedType {

		private final Type rawType;

		private final Type ownerType;

		private final Type[] actualTypeArguments;

		private final String typeName;

		ResolvedParameterizedType(Type rawType, Type ownerType, Type[] actualTypeArguments, String typeName) {
			this.rawType = rawType;
			this.ownerType = ownerType;
			this.actualTypeArguments = actualTypeArguments;
			this.typeName = typeName;
		}

		@Override
		public Type[] getActualTypeArguments() {
			return actualTypeArguments;
		}

		@Override
		public Type getRawType() {
			return rawType;
		}

		@Override
		public Type getOwnerType() {
			return ownerType;
		}

		public String getTypeName() {
			return typeName;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Bind type variable with type information.
	// --------------------------------------------------------------------------------------------

	/**
	 * Bind the {@link TypeVariable} with {@link TypeInformation} from the inputs' {@link TypeInformation}.
	 * @param clazz the sub class
	 * @param baseClazz the base class
	 * @param in1TypeInfo the {@link TypeInformation} of the first input
	 * @param in1Pos the position of type parameter of the first input in a {@link Function} sub class
	 * @param in2TypeInfo the {@link TypeInformation} of the second input
	 * @param in2Pos the position of type parameter of the second input in a {@link Function} sub class
	 * @param <IN1> the type of the first input
	 * @param <IN2> the type of the second input
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 */
	@VisibleForTesting
	static <IN1, IN2> Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariablesWithTypeInformationFromInputs(
		final Class<?> clazz,
		final Class<?> baseClazz,
		@Nullable final TypeInformation<IN1> in1TypeInfo,
		final int in1Pos,
		@Nullable final TypeInformation<IN2> in2TypeInfo,
		final int in2Pos) {

		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();

		if (in1TypeInfo == null && in2TypeInfo == null) {
			return Collections.emptyMap();
		}

		final List<ParameterizedType> functionTypeHierarchy = buildParameterizedTypeHierarchy(clazz, baseClazz, true);

		if (functionTypeHierarchy.size() < 1) {
			return Collections.emptyMap();
		}

		final ParameterizedType baseClass = functionTypeHierarchy.get(functionTypeHierarchy.size() - 1);

		if (in1TypeInfo != null) {
			final Type in1Type = baseClass.getActualTypeArguments()[in1Pos];
			final Type resolvedIn1Type = resolveTypeFromTypeHierarchy(in1Type, functionTypeHierarchy, false);
			typeVariableBindings.putAll(bindTypeVariablesWithTypeInformationFromInput(resolvedIn1Type, in1TypeInfo));
		}

		if (in2TypeInfo != null) {
			final Type in2Type = baseClass.getActualTypeArguments()[in2Pos];
			final Type resolvedIn2Type = resolveTypeFromTypeHierarchy(in2Type, functionTypeHierarchy, false);
			typeVariableBindings.putAll(bindTypeVariablesWithTypeInformationFromInput(resolvedIn2Type, in2TypeInfo));
		}
		return typeVariableBindings.isEmpty() ? Collections.emptyMap() : typeVariableBindings;
	}

	/**
	 * Bind the {@link TypeVariable} with {@link TypeInformation} from one input's {@link TypeInformation}.
	 * @param inType the input type
	 * @param inTypeInfo the input's {@link TypeInformation}
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 */
	private static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariablesWithTypeInformationFromInput(
		final Type inType,
		final TypeInformation<?> inTypeInfo) {

		final List<ParameterizedType> factoryHierarchy = new ArrayList<>();
		final TypeInfoFactory<?> factory = getClosestFactory(factoryHierarchy, inType);

		if (factory != null) {
			final Type factoryDefiningType = factoryHierarchy.size() < 1 ? inType :
				resolveTypeFromTypeHierarchy(factoryHierarchy.get(factoryHierarchy.size() - 1), factoryHierarchy, true);
			return bindTypeVariableFromGenericParameters(factoryDefiningType, inTypeInfo);
		} else if (inType instanceof GenericArrayType) {
			return bindTypeVariableFromGenericArray(inType, inTypeInfo);
		} else if (inTypeInfo instanceof TupleTypeInfo && isClassType(inType) && Tuple.class.isAssignableFrom(typeToClass(inType))) {
			final List<ParameterizedType> typeHierarchy = new ArrayList<>();
			Type curType = inType;
			// get tuple from possible tuple subclass
			while (!(isClassType(curType) && typeToClass(curType).getSuperclass().equals(Tuple.class))) {
				if (curType instanceof ParameterizedType) {
					typeHierarchy.add((ParameterizedType) curType);
				}
				curType = typeToClass(curType).getGenericSuperclass();
			}
			if (curType instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) curType);
			}
			final Type tupleBaseClass = resolveTypeFromTypeHierarchy(curType, typeHierarchy, true);
			return bindTypeVariableFromGenericParameters(tupleBaseClass, inTypeInfo);
		} else if (inTypeInfo instanceof PojoTypeInfo && isClassType(inType)) {
			return bindTypeVariableFromFields(inType, inTypeInfo);
		} else if (inType instanceof TypeVariable) {
			final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();

			typeVariableBindings.put((TypeVariable<?>) inType, inTypeInfo);
			return typeVariableBindings;
		}
		return Collections.emptyMap();
	}

	/**
	 * Bind the {@link TypeVariable} with {@link TypeInformation} from mapping relations between the generic paramters
	 * and {@link TypeInformation}.
	 *
	 * @param type the type that has {@link TypeVariable}
	 * @param typeInformation the {@link TypeInformation} that stores the mapping relations between the generic parameters
	 *                        and {@link TypeInformation}.
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 */
	private static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariableFromGenericParameters(
		final Type type,
		final TypeInformation<?> typeInformation) {

		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();
		if (type instanceof ParameterizedType) {
			final Type[] typeParams = typeToClass(type).getTypeParameters();
			final Type[] actualParams = ((ParameterizedType) type).getActualTypeArguments();
			for (int i = 0; i < actualParams.length; i++) {
				final Map<String, TypeInformation<?>> componentInfo = typeInformation.getGenericParameters();
				final String typeParamName = typeParams[i].toString();
				if (!componentInfo.containsKey(typeParamName) || componentInfo.get(typeParamName) == null) {
					throw new InvalidTypesException("TypeInformation '" + typeInformation.getClass().getSimpleName() +
						"' does not supply a mapping of TypeVariable '" + typeParamName + "' to corresponding TypeInformation. " +
						"Input type inference can only produce a result with this information. " +
						"Please implement method 'TypeInformation.getGenericParameters()' for this.");
				}
				final Map<TypeVariable<?>, TypeInformation<?>> sub =
					bindTypeVariablesWithTypeInformationFromInput(actualParams[i], componentInfo.get(typeParamName));
				typeVariableBindings.putAll(sub);
			}
		}
		return typeVariableBindings;
	}

	private static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariableFromGenericArray(
		final Type type,
		final TypeInformation<?> typeInformation) {

		TypeInformation<?> componentInfo = null;
		if (typeInformation instanceof BasicArrayTypeInfo) {
			componentInfo = ((BasicArrayTypeInfo<?, ?>) typeInformation).getComponentInfo();
		} else if (typeInformation instanceof PrimitiveArrayTypeInfo) {
			componentInfo = BasicTypeInfo.getInfoFor(typeInformation.getTypeClass().getComponentType());
		} else if (typeInformation instanceof ObjectArrayTypeInfo) {
			componentInfo = ((ObjectArrayTypeInfo<?, ?>) typeInformation).getComponentInfo();
		}
		return bindTypeVariablesWithTypeInformationFromInput(((GenericArrayType) type).getGenericComponentType(), componentInfo);
	}

	/**
	 * Bind the {@link TypeVariable} with {@link TypeInformation} from the mapping relation between the fields
	 * and {@link TypeInformation}.
	 * TODO:: we could make this method generic later
	 * @param type pojo type
	 * @param typeInformation {@link TypeInformation} that could provide the mapping relation between the fields
	 *                                                and {@link TypeInformation}
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 */
	private static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariableFromFields(
		final Type type,
		final TypeInformation<?> typeInformation) {

		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();
		// build the entire type hierarchy for the pojo
		final List<ParameterizedType> pojoHierarchy = buildParameterizedTypeHierarchy(type, Object.class);
		// build the entire type hierarchy for the pojo
		final List<Field> fields = getAllDeclaredFields(typeToClass(type), false);
		for (Field field : fields) {
			final Type fieldType = field.getGenericType();
			final Type resolvedFieldType =  resolveTypeFromTypeHierarchy(fieldType, pojoHierarchy, true);
			final Map<TypeVariable<?>, TypeInformation<?>> sub =
				bindTypeVariablesWithTypeInformationFromInput(resolvedFieldType, getTypeOfPojoField(typeInformation, field));
			typeVariableBindings.putAll(sub);
		}

		return typeVariableBindings;
	}
}
