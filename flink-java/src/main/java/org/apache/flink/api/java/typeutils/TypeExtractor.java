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

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.specific.SpecificRecordBase;
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
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A utility for reflection analysis on classes, to determine the return type of implementations of transformation
 * functions.
 */
public class TypeExtractor {
	
	private static final Logger LOG = LoggerFactory.getLogger(TypeExtractor.class);

	// We need this to detect recursive types and not get caught
	// in an endless recursion
	private Set<Class<?>> alreadySeen;

	protected TypeExtractor() {
		alreadySeen = new HashSet<Class<?>>();
	}

	// --------------------------------------------------------------------------------------------
	//  Function specific methods
	// --------------------------------------------------------------------------------------------
	
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapInterface, TypeInformation<IN> inType) {
		return getMapReturnTypes(mapInterface, inType, null, false);
	}
	
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType((Function) mapInterface, MapFunction.class, false, false, inType, functionName, allowMissing);
	}
	

	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapInterface, TypeInformation<IN> inType) {
		return getFlatMapReturnTypes(flatMapInterface, inType, null, false);
	}
	
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType((Function) flatMapInterface, FlatMapFunction.class, false, true, inType, functionName, allowMissing);
	}

	public static <IN, OUT> TypeInformation<OUT> getFoldReturnTypes(FoldFunction<IN, OUT> foldInterface, TypeInformation<IN> inType)
	{
		return getFoldReturnTypes(foldInterface, inType, null, false);
	}

	public static <IN, OUT> TypeInformation<OUT> getFoldReturnTypes(FoldFunction<IN, OUT> foldInterface, TypeInformation<IN> inType, String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType((Function) foldInterface, FoldFunction.class, false, false, inType, functionName, allowMissing);
	}
	
	
	public static <IN, OUT> TypeInformation<OUT> getMapPartitionReturnTypes(MapPartitionFunction<IN, OUT> mapPartitionInterface, TypeInformation<IN> inType) {
		return getMapPartitionReturnTypes(mapPartitionInterface, inType, null, false);
	}
	
	public static <IN, OUT> TypeInformation<OUT> getMapPartitionReturnTypes(MapPartitionFunction<IN, OUT> mapPartitionInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType((Function) mapPartitionInterface, MapPartitionFunction.class, true, true, inType, functionName, allowMissing);
	}
	
	
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(GroupReduceFunction<IN, OUT> groupReduceInterface, TypeInformation<IN> inType) {
		return getGroupReduceReturnTypes(groupReduceInterface, inType, null, false);
	}
	
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(GroupReduceFunction<IN, OUT> groupReduceInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType((Function) groupReduceInterface, GroupReduceFunction.class, true, true, inType, functionName, allowMissing);
	}

	public static <IN, OUT> TypeInformation<OUT> getGroupCombineReturnTypes(GroupCombineFunction<IN, OUT> combineInterface, TypeInformation<IN> inType) {
		return getGroupCombineReturnTypes(combineInterface, inType, null, false);
	}

	public static <IN, OUT> TypeInformation<OUT> getGroupCombineReturnTypes(GroupCombineFunction<IN, OUT> combineInterface, TypeInformation<IN> inType,
																			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType((Function) combineInterface, GroupCombineFunction.class, true, true, inType, functionName, allowMissing);
	}
	
	
	public static <IN1, IN2, OUT> TypeInformation<OUT> getFlatJoinReturnTypes(FlatJoinFunction<IN1, IN2, OUT> joinInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type)
	{
		return getFlatJoinReturnTypes(joinInterface, in1Type, in2Type, null, false);
	}
	
	public static <IN1, IN2, OUT> TypeInformation<OUT> getFlatJoinReturnTypes(FlatJoinFunction<IN1, IN2, OUT> joinInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, String functionName, boolean allowMissing)
	{
		return getBinaryOperatorReturnType((Function) joinInterface, FlatJoinFunction.class, false, true,
				in1Type, in2Type, functionName, allowMissing);
	}
	
	
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(JoinFunction<IN1, IN2, OUT> joinInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type)
	{
		return getJoinReturnTypes(joinInterface, in1Type, in2Type, null, false);
	}
	
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(JoinFunction<IN1, IN2, OUT> joinInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, String functionName, boolean allowMissing)
	{
		return getBinaryOperatorReturnType((Function) joinInterface, JoinFunction.class, false, false,
				in1Type, in2Type, functionName, allowMissing);
	}
	
	
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(CoGroupFunction<IN1, IN2, OUT> coGroupInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type)
	{
		return getCoGroupReturnTypes(coGroupInterface, in1Type, in2Type, null, false);
	}
	
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(CoGroupFunction<IN1, IN2, OUT> coGroupInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, String functionName, boolean allowMissing)
	{
		return getBinaryOperatorReturnType((Function) coGroupInterface, CoGroupFunction.class, true, true,
				in1Type, in2Type, functionName, allowMissing);
	}
	
	
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(CrossFunction<IN1, IN2, OUT> crossInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type)
	{
		return getCrossReturnTypes(crossInterface, in1Type, in2Type, null, false);
	}
	
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(CrossFunction<IN1, IN2, OUT> crossInterface,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, String functionName, boolean allowMissing)
	{
		return getBinaryOperatorReturnType((Function) crossInterface, CrossFunction.class, false, false,
				in1Type, in2Type, functionName, allowMissing);
	}
	
	
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(KeySelector<IN, OUT> selectorInterface, TypeInformation<IN> inType) {
		return getKeySelectorTypes(selectorInterface, inType, null, false);
	}
	
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(KeySelector<IN, OUT> selectorInterface,
			TypeInformation<IN> inType, String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType((Function) selectorInterface, KeySelector.class, false, false, inType, functionName, allowMissing);
	}
	
	
	public static <T> TypeInformation<T> getPartitionerTypes(Partitioner<T> partitioner) {
		return getPartitionerTypes(partitioner, null, false);
	}
	
	public static <T> TypeInformation<T> getPartitionerTypes(Partitioner<T> partitioner, String functionName, boolean allowMissing) {
		return new TypeExtractor().privateCreateTypeInfo(Partitioner.class, partitioner.getClass(), 0, null, null);
	}
	
	
	@SuppressWarnings("unchecked")
	public static <IN> TypeInformation<IN> getInputFormatTypes(InputFormat<IN, ?> inputFormatInterface) {
		if (inputFormatInterface instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<IN>) inputFormatInterface).getProducedType();
		}
		return new TypeExtractor().privateCreateTypeInfo(InputFormat.class, inputFormatInterface.getClass(), 0, null, null);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Generic extraction methods
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public static <IN, OUT> TypeInformation<OUT> getUnaryOperatorReturnType(Function function, Class<?> baseClass, 
			boolean hasIterable, boolean hasCollector, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		try {
			final Method m = FunctionUtils.checkAndExtractLambdaMethod(function);
			if (m != null) {
				// check for lambda type erasure
				validateLambdaGenericParameters(m);
				
				// parameters must be accessed from behind, since JVM can add additional parameters e.g. when using local variables inside lambda function
				final int paramLen = m.getGenericParameterTypes().length - 1;
				final Type input = (hasCollector)? m.getGenericParameterTypes()[paramLen - 1] : m.getGenericParameterTypes()[paramLen];
				validateInputType((hasIterable)?removeGenericWrapper(input) : input, inType);
				if(function instanceof ResultTypeQueryable) {
					return ((ResultTypeQueryable<OUT>) function).getProducedType();
				}
				return new TypeExtractor().privateCreateTypeInfo((hasCollector)? removeGenericWrapper(m.getGenericParameterTypes()[paramLen]) : m.getGenericReturnType(), inType, null);
			}
			else {
				validateInputType(baseClass, function.getClass(), 0, inType);
				if(function instanceof ResultTypeQueryable) {
					return ((ResultTypeQueryable<OUT>) function).getProducedType();
				}
				return new TypeExtractor().privateCreateTypeInfo(baseClass, function.getClass(), 1, inType, null);
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
	
	@SuppressWarnings("unchecked")
	public static <IN1, IN2, OUT> TypeInformation<OUT> getBinaryOperatorReturnType(Function function, Class<?> baseClass,
			boolean hasIterables, boolean hasCollector, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type,
			String functionName, boolean allowMissing)
	{
		try {
			final Method m = FunctionUtils.checkAndExtractLambdaMethod(function);
			if (m != null) {
				// check for lambda type erasure
				validateLambdaGenericParameters(m);
				
				// parameters must be accessed from behind, since JVM can add additional parameters e.g. when using local variables inside lambda function
				final int paramLen = m.getGenericParameterTypes().length - 1;
				final Type input1 = (hasCollector)? m.getGenericParameterTypes()[paramLen - 2] : m.getGenericParameterTypes()[paramLen - 1];
				final Type input2 = (hasCollector)? m.getGenericParameterTypes()[paramLen - 1] : m.getGenericParameterTypes()[paramLen];
				validateInputType((hasIterables)? removeGenericWrapper(input1) : input1, in1Type);
				validateInputType((hasIterables)? removeGenericWrapper(input2) : input2, in2Type);
				if(function instanceof ResultTypeQueryable) {
					return ((ResultTypeQueryable<OUT>) function).getProducedType();
				}
				return new TypeExtractor().privateCreateTypeInfo((hasCollector)? removeGenericWrapper(m.getGenericParameterTypes()[paramLen]) : m.getGenericReturnType(), in1Type, in2Type);
			}
			else {
				validateInputType(baseClass, function.getClass(), 0, in1Type);
				validateInputType(baseClass, function.getClass(), 1, in2Type);
				if(function instanceof ResultTypeQueryable) {
					return ((ResultTypeQueryable<OUT>) function).getProducedType();
				}
				return new TypeExtractor().privateCreateTypeInfo(baseClass, function.getClass(), 2, in1Type, in2Type);
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
	
	public static TypeInformation<?> createTypeInfo(Type t) {
		TypeInformation<?> ti = new TypeExtractor().privateCreateTypeInfo(t);
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}
	
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
		
		// check if type is a subclass of tuple
		if (isClassType(t) && Tuple.class.isAssignableFrom(typeToClass(t))) {
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
			
			// check if immediate child of Tuple has generics
			if (curT instanceof Class<?>) {
				throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
			}
			
			typeHierarchy.add(curT);
			
			ParameterizedType tupleChild = (ParameterizedType) curT;
			
			Type[] subtypes = new Type[tupleChild.getActualTypeArguments().length];
			
			// materialize possible type variables
			for (int i = 0; i < subtypes.length; i++) {
				// materialize immediate TypeVariables
				if (tupleChild.getActualTypeArguments()[i] instanceof TypeVariable<?>) {
					subtypes[i] = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) tupleChild.getActualTypeArguments()[i]);
				}
				// class or parameterized type
				else {
					subtypes[i] = tupleChild.getActualTypeArguments()[i];
				}
			}
			
			TypeInformation<?>[] tupleSubTypes = new TypeInformation<?>[subtypes.length];
			for (int i = 0; i < subtypes.length; i++) {
				// sub type could not be determined with materializing
				// try to derive the type info of the TypeVariable from the immediate base child input as a last attempt
				if (subtypes[i] instanceof TypeVariable<?>) {
					tupleSubTypes[i] = createTypeInfoFromInputs((TypeVariable<?>) subtypes[i], typeHierarchy, in1Type, in2Type);
					
					// variable could not be determined
					if (tupleSubTypes[i] == null) {
						throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) subtypes[i]).getName() + "' in '"
								+ ((TypeVariable<?>) subtypes[i]).getGenericDeclaration()
								+ "' could not be determined. This is most likely a type erasure problem. "
								+ "The type extraction currently supports types with generic variables only in cases where "
								+ "all variables in the return type can be deduced from the input type(s).");
					}
				} else {
					tupleSubTypes[i] = createTypeInfoWithTypeHierarchy(new ArrayList<Type>(typeHierarchy), subtypes[i], in1Type, in2Type);
				}
			}
			
			Class<?> tAsClass = null;
			if (isClassType(t)) {
				tAsClass = typeToClass(t);
			}
			Preconditions.checkNotNull(tAsClass, "t has a unexpected type");
			// check if the class we assumed to be a Tuple so far is actually a pojo because it contains additional fields.
			// check for additional fields.
			int fieldCount = countFieldsInClass(tAsClass);
			if(fieldCount != tupleSubTypes.length) {
				// the class is not a real tuple because it contains additional fields. treat as a pojo
				if (t instanceof ParameterizedType) {
					return (TypeInformation<OUT>) analyzePojo(tAsClass, new ArrayList<Type>(typeHierarchy), (ParameterizedType) t, in1Type, in2Type);
				}
				else {
					return (TypeInformation<OUT>) analyzePojo(tAsClass, new ArrayList<Type>(typeHierarchy), null, in1Type, in2Type);
				}
			}
			
			return new TupleTypeInfo(tAsClass, tupleSubTypes);
			
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
				String className;
				// for int[], double[] etc.
				if(componentClass.isPrimitive()) {
					className = encodePrimitiveClass(componentClass);
				}
				// for String[], Integer[] etc.
				else {
					className = "L" + componentClass.getName() + ";";
				}
				
				Class<OUT> classArray;
				try {
					classArray = (Class<OUT>) Class.forName("[" + className);
				} catch (ClassNotFoundException e) {
					throw new InvalidTypesException("Could not convert GenericArrayType to Class.");
				}
				return getForClass(classArray);
			}
			
			TypeInformation<?> componentInfo = createTypeInfoWithTypeHierarchy(typeHierarchy, genericArray.getGenericComponentType(),
					in1Type, in2Type);
			return ObjectArrayTypeInfo.getInfoFor(t, componentInfo);
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
	
	private <IN1> TypeInformation<?> createTypeInfoFromInput(TypeVariable<?> returnTypeVar, ArrayList<Type> inputTypeHierarchy, Type inType, TypeInformation<IN1> inTypeInfo) {
		TypeInformation<?> info = null;
		
		// the input is a type variable
		if (inType instanceof TypeVariable) {
			inType = materializeTypeVariable(inputTypeHierarchy, (TypeVariable<?>) inType);
			info = findCorrespondingInfo(returnTypeVar, inType, inTypeInfo, inputTypeHierarchy);
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
		else if (inTypeInfo instanceof TupleTypeInfo && isClassType(inType) 
				&& Tuple.class.isAssignableFrom(typeToClass(inType))) {
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
		else if (inTypeInfo instanceof PojoTypeInfo) {
			// build the entire type hierarchy for the pojo
			getTypeHierarchy(inputTypeHierarchy, inType, Object.class);
			info = findCorrespondingInfo(returnTypeVar, inType, inTypeInfo, inputTypeHierarchy);
		}
		return info;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Extract type parameters
	// --------------------------------------------------------------------------------------------
	
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
		
		throw new IllegalArgumentException("The types of the interface " + baseClass.getName() + " could not be inferred. " + 
						"Support for synthetic interfaces, lambdas, and generic types is limited at this point.");
	}
	
	private static Type getParameterTypeFromGenericType(Class<?> baseClass, ArrayList<Type> typeHierarchy, Type t, int pos) {
		// base class
		if (t instanceof ParameterizedType && baseClass.equals((Class<?>) ((ParameterizedType) t).getRawType())) {
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
			throw new InvalidTypesException("Input mismatch: " + e.getMessage());
		}
	}
	
	private static void validateInputType(Class<?> baseClass, Class<?> clazz, int inputParamPos, TypeInformation<?> inTypeInfo) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();

		// try to get generic parameter
		Type inType;
		try {
			inType = getParameterType(baseClass, typeHierarchy, clazz, inputParamPos);
		}
		catch (IllegalArgumentException e) {
			return; // skip input validation e.g. for raw types
		}

		try {
			validateInfo(typeHierarchy, inType, inTypeInfo);
		}
		catch(InvalidTypesException e) {
			throw new InvalidTypesException("Input mismatch: " + e.getMessage());
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
			// check for basic type
			if (typeInfo.isBasicType()) {
				
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
			// check for tuple
			else if (typeInfo.isTupleType()) {
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
					validateInfo(new ArrayList<Type>(typeHierarchy), subTypes[i], ((TupleTypeInfo<?>) typeInfo).getTypeAt(i));
				}
			}
			// check for Writable
			else if (typeInfo instanceof WritableTypeInfo<?>) {
				// check if writable at all
				if (!(type instanceof Class<?> && Writable.class.isAssignableFrom((Class<?>) type))) {
					throw new InvalidTypesException("Writable type expected.");
				}
				
				// check writable type contents
				Class<?> clazz;
				if (((WritableTypeInfo<?>) typeInfo).getTypeClass() != (clazz = (Class<?>) type)) {
					throw new InvalidTypesException("Writable type '"
							+ ((WritableTypeInfo<?>) typeInfo).getTypeClass().getCanonicalName() + "' expected but was '"
							+ clazz.getCanonicalName() + "'.");
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
				if (!(isClassType(type) && ((GenericTypeInfo<?>) typeInfo).getTypeClass() == (clazz = typeToClass(type)))) {
					throw new InvalidTypesException("Generic object type '"
							+ ((GenericTypeInfo<?>) typeInfo).getTypeClass().getCanonicalName() + "' expected but was '"
							+ clazz.getCanonicalName() + "'.");
				}
			}
		} else {
			type = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) type);
			if (!(type instanceof TypeVariable)) {
				validateInfo(typeHierarchy, type, typeInfo);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Utility methods
	// --------------------------------------------------------------------------------------------
	
	/**
	 * @param curT : start type
	 * @return Type The immediate child of the top class
	 */
	private static Type getTypeHierarchy(ArrayList<Type> typeHierarchy, Type curT, Class<?> stopAtClass) {
		// skip first one
		if (typeHierarchy.size() > 0 && typeHierarchy.get(0) == curT && isClassType(curT)) {
			curT = typeToClass(curT).getGenericSuperclass();
		}
		while (!(isClassType(curT) && typeToClass(curT).equals(stopAtClass))) {
			typeHierarchy.add(curT);
			curT = typeToClass(curT).getGenericSuperclass();

			if (curT == null) {
				break;
			}
		}
		return curT;
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
	
	private static Type removeGenericWrapper(Type t) {
		if(t instanceof ParameterizedType 	&& 
				(Collector.class.isAssignableFrom(typeToClass(t))
						|| Iterable.class.isAssignableFrom(typeToClass(t)))) {
			return ((ParameterizedType) t).getActualTypeArguments()[0];
		}
		return t;
	}
	
	private static void validateLambdaGenericParameters(Method m) {
		// check the arguments
		for (Type t : m.getGenericParameterTypes()) {
			validateLambdaGenericParameter(t);
		}

		// check the return type
		validateLambdaGenericParameter(m.getGenericReturnType());
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
	
	private static String encodePrimitiveClass(Class<?> primitiveClass) {
		if (primitiveClass == boolean.class) {
			return "Z";
		}
		else if (primitiveClass == byte.class) {
			return "B";
		}
		else if (primitiveClass == char.class) {
			return "C";
		}
		else if (primitiveClass == double.class) {
			return "D";
		}
		else if (primitiveClass == float.class) {
			return "F";
		}
		else if (primitiveClass == int.class) {
			return "I";
		}
		else if (primitiveClass == long.class) {
			return "J";
		}
		else if (primitiveClass == short.class) {
			return "S";
		}
		throw new InvalidTypesException();
	}
	
	private static TypeInformation<?> findCorrespondingInfo(TypeVariable<?> typeVar, Type type, TypeInformation<?> corrInfo, ArrayList<Type> typeHierarchy) {
		if (sameTypeVars(type, typeVar)) {
			return corrInfo;
		}
		else if (type instanceof TypeVariable && sameTypeVars(materializeTypeVariable(typeHierarchy, (TypeVariable<?>) type), typeVar)) {
			return corrInfo;
		}
		else if (type instanceof GenericArrayType) {
			TypeInformation<?> componentInfo = null;
			if (corrInfo instanceof BasicArrayTypeInfo) {
				componentInfo = ((BasicArrayTypeInfo<?,?>) corrInfo).getComponentInfo();
			}
			else if (corrInfo instanceof PrimitiveArrayTypeInfo) {
				componentInfo = BasicTypeInfo.getInfoFor(corrInfo.getTypeClass().getComponentType());
			}
			else if (corrInfo instanceof ObjectArrayTypeInfo) {
				componentInfo = ((ObjectArrayTypeInfo<?,?>) corrInfo).getComponentInfo();
			}
			TypeInformation<?> info = findCorrespondingInfo(typeVar, ((GenericArrayType) type).getGenericComponentType(), componentInfo, typeHierarchy);
			if (info != null) {
				return info;
			}
		}
		else if (corrInfo instanceof TupleTypeInfo
				&& type instanceof ParameterizedType
				&& Tuple.class.isAssignableFrom((Class<?>) ((ParameterizedType) type).getRawType())) {
			ParameterizedType tuple = (ParameterizedType) type;
			Type[] args = tuple.getActualTypeArguments();
			
			for (int i = 0; i < args.length; i++) {
				TypeInformation<?> info = findCorrespondingInfo(typeVar, args[i], ((TupleTypeInfo<?>) corrInfo).getTypeAt(i), typeHierarchy);
				if (info != null) {
					return info;
				}
			}
		}
		else if (corrInfo instanceof PojoTypeInfo && isClassType(type)) {
			// determine a field containing the type variable
			List<Field> fields = getAllDeclaredFields(typeToClass(type));
			for (Field field : fields) {
				Type fieldType = field.getGenericType();
				if (fieldType instanceof TypeVariable 
						&& sameTypeVars(typeVar, materializeTypeVariable(typeHierarchy, (TypeVariable<?>) fieldType))) {
					return getTypeOfPojoField(corrInfo, field);
				}
				else if (fieldType instanceof ParameterizedType
						|| fieldType instanceof GenericArrayType) {
					ArrayList<Type> typeHierarchyWithFieldType = new ArrayList<Type>(typeHierarchy);
					typeHierarchyWithFieldType.add(fieldType);
					TypeInformation<?> info = findCorrespondingInfo(typeVar, fieldType, getTypeOfPojoField(corrInfo, field), typeHierarchyWithFieldType);
					if (info != null) {
						return info;
					}
				}
			}
		}
		return null;
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
		return new TypeExtractor().privateGetForClass(clazz, new ArrayList<Type>());
	}
	
	private <X> TypeInformation<X> privateGetForClass(Class<X> clazz, ArrayList<Type> typeHierarchy) {
		return privateGetForClass(clazz, typeHierarchy, null, null, null);
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <OUT,IN1,IN2> TypeInformation<OUT> privateGetForClass(Class<OUT> clazz, ArrayList<Type> typeHierarchy,
			ParameterizedType parameterizedType, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		Preconditions.checkNotNull(clazz);
		
		if (clazz.equals(Object.class)) {
			return new GenericTypeInfo<OUT>(clazz);
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
				return ObjectArrayTypeInfo.getInfoFor(clazz);
			}
		}
		
		// check for writable types
		if(Writable.class.isAssignableFrom(clazz)) {
			return (TypeInformation<OUT>) WritableTypeInfo.getWritableTypeInfo((Class<? extends Writable>) clazz);
		}
		
		// check for basic types
		TypeInformation<OUT> basicTypeInfo = BasicTypeInfo.getInfoFor(clazz);
		if (basicTypeInfo != null) {
			return basicTypeInfo;
		}
		
		// check for subclasses of Value
		if (Value.class.isAssignableFrom(clazz)) {
			Class<? extends Value> valueClass = clazz.asSubclass(Value.class);
			return (TypeInformation<OUT>) ValueTypeInfo.getValueTypeInfo(valueClass);
		}
		
		// check for subclasses of Tuple
		if (Tuple.class.isAssignableFrom(clazz)) {
			throw new InvalidTypesException("Type information extraction for tuples cannot be done based on the class.");
		}

		// check for Enums
		if(Enum.class.isAssignableFrom(clazz)) {
			return (TypeInformation<OUT>) new EnumTypeInfo(clazz);
		}

		// special case for POJOs generated by Avro.
		if(SpecificRecordBase.class.isAssignableFrom(clazz)) {
			return (TypeInformation<OUT>) new AvroTypeInfo(clazz);
		}

		if (alreadySeen.contains(clazz)) {
			return new GenericTypeInfo<OUT>(clazz);
		}

		alreadySeen.add(clazz);

		if (Modifier.isInterface(clazz.getModifiers())) {
			// Interface has no members and is therefore not handled as POJO
			return new GenericTypeInfo<OUT>(clazz);
		}

		if (clazz.equals(Class.class)) {
			// special case handling for Class, this should not be handled by the POJO logic
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
			final String fieldNameLow = f.getName().toLowerCase();
			
			Type fieldType = f.getGenericType();
			TypeVariable<?> fieldTypeGeneric = null;
			if(fieldType instanceof TypeVariable) {
				fieldTypeGeneric = (TypeVariable<?>) fieldType;
				fieldType = materializeTypeVariable(typeHierarchy, (TypeVariable<?>)fieldType);
			}
			for(Method m : clazz.getMethods()) {
				// check for getter
				if(	// The name should be "get<FieldName>" or "<fieldName>" (for scala) or "is<fieldName>" for boolean fields.
					(m.getName().toLowerCase().equals("get"+fieldNameLow) || m.getName().toLowerCase().equals("is"+fieldNameLow) || m.getName().toLowerCase().equals(fieldNameLow)) &&
					// no arguments for the getter
					m.getParameterTypes().length == 0 &&
					// return type is same as field type (or the generic variant of it)
					(m.getGenericReturnType().equals( fieldType ) || (fieldTypeGeneric != null && m.getGenericReturnType().equals(fieldTypeGeneric)) )
				) {
					if(hasGetter) {
						throw new IllegalStateException("Detected more than one getter");
					}
					hasGetter = true;
				}
				// check for setters (<FieldName>_$eq for scala)
				if((m.getName().toLowerCase().equals("set"+fieldNameLow) || m.getName().toLowerCase().equals(fieldNameLow+"_$eq")) &&
					m.getParameterTypes().length == 1 && // one parameter of the field's type
					( m.getGenericParameterTypes()[0].equals( fieldType ) || (fieldTypeGeneric != null && m.getGenericParameterTypes()[0].equals(fieldTypeGeneric) ) )&&
					// return type is void.
					m.getReturnType().equals(Void.TYPE)
				) {
					if(hasSetter) {
						throw new IllegalStateException("Detected more than one setter");
					}
					hasSetter = true;
				}
			}
			if(hasGetter && hasSetter) {
				return true;
			} else {
				if(!hasGetter) {
					LOG.debug("Class "+clazz+" does not contain a getter for field "+f.getName() );
				}
				if(!hasSetter) {
					LOG.debug("Class "+clazz+" does not contain a setter for field "+f.getName() );
				}
				return false;
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected <OUT, IN1, IN2> TypeInformation<OUT> analyzePojo(Class<OUT> clazz, ArrayList<Type> typeHierarchy,
			ParameterizedType parameterizedType, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {

		// add the hierarchy of the POJO itself if it is generic
		if (parameterizedType != null) {
			getTypeHierarchy(typeHierarchy, parameterizedType, Object.class);
		}
		// create a type hierarchy, if the incoming only contains the most bottom one or none.
		else if(typeHierarchy.size() <= 1) {
			getTypeHierarchy(typeHierarchy, clazz, Object.class);
		}
		
		List<Field> fields = getAllDeclaredFields(clazz);
		if(fields.size() == 0) {
			LOG.info("No fields detected for class " + clazz + ". Cannot be used as a PojoType. Will be handled as GenericType");
			return new GenericTypeInfo<OUT>(clazz);
		}

		List<PojoField> pojoFields = new ArrayList<PojoField>();
		for (Field field : fields) {
			Type fieldType = field.getGenericType();
			if(!isValidPojoField(field, clazz, typeHierarchy)) {
				LOG.info("Class " + clazz + " is not a valid POJO type");
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
				LOG.info("Class "+clazz+" contains custom serialization methods we do not call.");
				return null;
			}
		}

		// Try retrieving the default constructor, if it does not have one
		// we cannot use this because the serializer uses it.
		try {
			clazz.getDeclaredConstructor();
		} catch (NoSuchMethodException e) {
			if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
				LOG.info("Class " + clazz + " is abstract or an interface, having a concrete " +
						"type can increase performance.");
			} else {
				LOG.info("Class " + clazz + " must have a default constructor to be used as a POJO.");
				return null;
			}
		}
		
		// everything is checked, we return the pojo
		return pojoType;
	}

	/**
	 * recursively determine all declared fields
	 * This is required because class.getFields() is not returning fields defined
	 * in parent classes.
	 */
	public static List<Field> getAllDeclaredFields(Class<?> clazz) {
		List<Field> result = new ArrayList<Field>();
		while (clazz != null) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				if(Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
					continue; // we have no use for transient or static fields
				}
				if(hasFieldWithSameName(field.getName(), result)) {
					throw new RuntimeException("The field "+field+" is already contained in the hierarchy of the class "+clazz+"."
							+ "Please use unique field names through your classes hierarchy");
				}
				result.add(field);
			}
			clazz = clazz.getSuperclass();
		}
		return result;
	}
	
	public static Field getDeclaredField(Class<?> clazz, String name) {
		for (Field field : getAllDeclaredFields(clazz)) {
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

	
	// recursively determine all declared methods
	private static List<Method> getAllDeclaredMethods(Class<?> clazz) {
		List<Method> result = new ArrayList<Method>();
		while (clazz != null) {
			Method[] methods = clazz.getDeclaredMethods();
			for (Method method : methods) {
				result.add(method);
			}
			clazz = clazz.getSuperclass();
		}
		return result;
	}

	// not public to users
	public static Class<?> typeToClass(Type t) {
		if (t instanceof Class) {
			return (Class<?>)t;
		}
		else if (t instanceof ParameterizedType) {
			return ((Class<?>)((ParameterizedType) t).getRawType());
		}
		throw new IllegalArgumentException("Cannot convert type to class");
	}

	// not public to users
	public static boolean isClassType(Type t) {
		return t instanceof Class<?> || t instanceof ParameterizedType;
	}

	private static boolean sameTypeVars(Type t1, Type t2) {
		if (!(t1 instanceof TypeVariable) || !(t2 instanceof TypeVariable)) {
			return false;
		}
		return ((TypeVariable<?>) t1).getName().equals(((TypeVariable<?>) t2).getName())
				&& ((TypeVariable<?>) t1).getGenericDeclaration().equals(((TypeVariable<?>) t2).getGenericDeclaration());
	}
	
	private static TypeInformation<?> getTypeOfPojoField(TypeInformation<?> pojoInfo, Field field) {
		for (int j = 0; j < pojoInfo.getArity(); j++) {
			PojoField pf = ((PojoTypeInfo<?>) pojoInfo).getPojoFieldAt(j);
			if (pf.field.getName().equals(field.getName())) {
				return pf.type;
			}
		}
		return null;
	}


	public static <X> TypeInformation<X> getForObject(X value) {
		return new TypeExtractor().privateGetForObject(value);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <X> TypeInformation<X> privateGetForObject(X value) {
		Preconditions.checkNotNull(value);

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
			return (TypeInformation<X>) new TupleTypeInfo(value.getClass(), infos);
		} else {
			return privateGetForClass((Class<X>) value.getClass(), new ArrayList<Type>());
		}
	}
}
