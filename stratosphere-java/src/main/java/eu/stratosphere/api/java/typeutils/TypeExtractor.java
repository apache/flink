/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.typeutils;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;

import eu.stratosphere.types.TypeInformation;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.io.Writable;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.InvalidTypesException;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.types.Value;

public class TypeExtractor {
	
	@SuppressWarnings("unchecked")
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapFunction, TypeInformation<IN> inType) {
		validateInputType(MapFunction.class, mapFunction.getClass(), 0, inType);
		if(mapFunction instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) mapFunction).getProducedType();
		}
		return createTypeInfo(MapFunction.class, mapFunction.getClass(), 1, inType, null);
	}
	
	@SuppressWarnings("unchecked")
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapFunction, TypeInformation<IN> inType) {
		validateInputType(FlatMapFunction.class, flatMapFunction.getClass(), 0, inType);
		if(flatMapFunction instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) flatMapFunction).getProducedType();
		}
		return createTypeInfo(FlatMapFunction.class, flatMapFunction.getClass(), 1, inType, null);
	}
	
	@SuppressWarnings("unchecked")
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(GroupReduceFunction<IN, OUT> groupReduceFunction,
			TypeInformation<IN> inType) {
		validateInputType(GroupReduceFunction.class, groupReduceFunction.getClass(), 0, inType);
		if(groupReduceFunction instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) groupReduceFunction).getProducedType();
		}
		return createTypeInfo(GroupReduceFunction.class, groupReduceFunction.getClass(), 1, inType, null);
	}
	
	@SuppressWarnings("unchecked")
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(JoinFunction<IN1, IN2, OUT> joinFunction,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		validateInputType(JoinFunction.class, joinFunction.getClass(), 0, in1Type);
		validateInputType(JoinFunction.class, joinFunction.getClass(), 1, in2Type);
		if(joinFunction instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) joinFunction).getProducedType();
		}
		return createTypeInfo(JoinFunction.class, joinFunction.getClass(), 2, in1Type, in2Type);
	}
	
	@SuppressWarnings("unchecked")
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(CoGroupFunction<IN1, IN2, OUT> coGroupFunction,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		validateInputType(CoGroupFunction.class, coGroupFunction.getClass(), 0, in1Type);
		validateInputType(CoGroupFunction.class, coGroupFunction.getClass(), 1, in2Type);
		if(coGroupFunction instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) coGroupFunction).getProducedType();
		}
		return createTypeInfo(CoGroupFunction.class, coGroupFunction.getClass(), 2, in1Type, in2Type);
	}
	
	@SuppressWarnings("unchecked")
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(CrossFunction<IN1, IN2, OUT> crossFunction,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		validateInputType(CrossFunction.class, crossFunction.getClass(), 0, in1Type);
		validateInputType(CrossFunction.class, crossFunction.getClass(), 1, in2Type);
		if(crossFunction instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) crossFunction).getProducedType();
		}
		return createTypeInfo(CrossFunction.class, crossFunction.getClass(), 2, in1Type, in2Type);
	}
	
	@SuppressWarnings("unchecked")
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(KeySelector<IN, OUT> selector, TypeInformation<IN> inType) {
		validateInputType(KeySelector.class, selector.getClass(), 0, inType);
		if(selector instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) selector).getProducedType();
		}
		return createTypeInfo(KeySelector.class, selector.getClass(), 1, inType, null);
	}
	
	@SuppressWarnings("unchecked")
	public static <IN> TypeInformation<IN> getInputFormatTypes(InputFormat<IN, ?> inputFormat) {
		if(inputFormat instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<IN>) inputFormat).getProducedType();
		}
		return createTypeInfo(InputFormat.class, inputFormat.getClass(), 0, null, null);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Generic utility methods
	// --------------------------------------------------------------------------------------------
	
	public static TypeInformation<?> createTypeInfo(Type t) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();
		typeHierarchy.add(t);
		return createTypeInfoWithTypeHierarchy(typeHierarchy, t, null, null);
	}
	
	@SuppressWarnings("unchecked")
	public static <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfo(Class<?> baseClass, Class<?> clazz, int returnParamPos,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();
		Type returnType = getParameterType(baseClass, typeHierarchy, clazz, returnParamPos);
		
		TypeInformation<OUT> typeInfo = null;
		
		// return type is a variable -> try to get the type info from the input of the base class directly
		if (returnType instanceof TypeVariable<?>) {
			ParameterizedType immediateBaseChild = (ParameterizedType) typeHierarchy.get(typeHierarchy.size() - 1);
			typeInfo = (TypeInformation<OUT>) createTypeInfoWithImmediateBaseChildInput(immediateBaseChild, (TypeVariable<?>) returnType,
					in1Type, in2Type);
			
			if (typeInfo != null) {
				return typeInfo;
			}
		}
		
		// get info from hierarchy
		return (TypeInformation<OUT>) createTypeInfoWithTypeHierarchy(typeHierarchy, returnType, in1Type, in2Type);
	}
	
	public static Type getParameterType(Class<?> baseClass, Class<?> clazz, int pos) {
		return getParameterType(baseClass, null, clazz, pos);
	}
	
	private static void validateInputType(Class<?> baseClass, Class<?> clazz, int inputParamPos, TypeInformation<?> inType) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();		
		try {
			validateInfo(typeHierarchy, getParameterType(baseClass, typeHierarchy, clazz, inputParamPos), inType);
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
				
				TypeInformation<?> actual = null;
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
				if (!(type instanceof Class<?> && Tuple.class.isAssignableFrom((Class<?>) type))
						&& !(type instanceof ParameterizedType && Tuple.class.isAssignableFrom((Class<?>) ((ParameterizedType) type)
								.getRawType()))) {
					throw new InvalidTypesException("Tuple type expected.");
				}
				
				// do not allow usage of Tuple as type
				if (type instanceof Class<?> && ((Class<?>) type).equals(Tuple.class)) {
					throw new InvalidTypesException("Concrete subclass of Tuple expected.");
				}
				
				// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
				while (!(type instanceof ParameterizedType && ((Class<?>) ((ParameterizedType) type).getRawType()).getSuperclass().equals(
						Tuple.class))
						&& !(type instanceof Class<?> && ((Class<?>) type).getSuperclass().equals(Tuple.class))) {
					typeHierarchy.add(type);
					// parameterized type
					if (type instanceof ParameterizedType) {
						type = ((Class<?>) ((ParameterizedType) type).getRawType()).getGenericSuperclass();
					}
					// class
					else {
						type = ((Class<?>) type).getGenericSuperclass();
					}
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
				Class<?> clazz = null;
				if (((WritableTypeInfo<?>) typeInfo).getTypeClass() != (clazz = (Class<?>) type)) {
					throw new InvalidTypesException("Writable type '"
							+ ((WritableTypeInfo<?>) typeInfo).getTypeClass().getCanonicalName() + "' expected but was '"
							+ clazz.getCanonicalName() + "'.");
				}
			}
			// check for basic array
			else if (typeInfo instanceof BasicArrayTypeInfo<?, ?>) {
				Type component = null;
				// check if array at all
				if (!(type instanceof Class<?> && ((Class<?>) type).isArray() && (component = ((Class<?>) type).getComponentType()) != null)
						&& !(type instanceof GenericArrayType && (component = ((GenericArrayType) type).getGenericComponentType()) != null)) {
					throw new InvalidTypesException("Array type expected.");
				}
				
				if (component instanceof TypeVariable<?>) {
					component = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) component);
					if (component == null) {
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
				Type component = null;
				if (type instanceof Class<?>) {
					component = ((Class<?>) type).getComponentType();
				} else {
					component = ((GenericArrayType) type).getGenericComponentType();
				}
				
				if (component instanceof TypeVariable<?>) {
					component = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) component);
					if (component == null) {
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
				
				TypeInformation<?> actual = null;
				// check value type contents
				if (!((ValueTypeInfo<?>) typeInfo).equals(actual = ValueTypeInfo.getValueTypeInfo((Class<? extends Value>) type))) {
					throw new InvalidTypesException("Value type '" + typeInfo + "' expected but was '" + actual + "'.");
				}
			}
			// check for custom object
			else if (typeInfo instanceof GenericTypeInfo<?>) {
				Class<?> clazz = null;
				if (!(type instanceof Class<?> && ((GenericTypeInfo<?>) typeInfo).getTypeClass() == (clazz = (Class<?>) type))
						&& !(type instanceof ParameterizedType && (clazz = (Class<?>) ((ParameterizedType) type).getRawType()) == ((GenericTypeInfo<?>) typeInfo)
								.getTypeClass())) {
					throw new InvalidTypesException("Generic object type '"
							+ ((GenericTypeInfo<?>) typeInfo).getTypeClass().getCanonicalName() + "' expected but was '"
							+ clazz.getCanonicalName() + "'.");
				}
			}
		} else {
			type = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) type);
			if (type != null) {
				validateInfo(typeHierarchy, type, typeInfo);
			}
		}
	}
	
	private static Type getParameterType(Class<?> baseClass, ArrayList<Type> typeHierarchy, Class<?> clazz, int pos) {
		Type t = clazz.getGenericSuperclass();
		
		// check if type is child of the base class
		if (!(t instanceof Class<?> && baseClass.isAssignableFrom((Class<?>) t))
				&& !(t instanceof ParameterizedType && baseClass.isAssignableFrom((Class<?>) ((ParameterizedType) t).getRawType()))) {
			throw new IllegalArgumentException("A generic function base class must be a super class.");
		}
		if (typeHierarchy != null) {
			typeHierarchy.add(t);
		}
		
		Type curT = t;
		// go up the hierarchy until we reach the base class (with or without generics)
		// collect the types while moving up for a later top-down 
		while (!(curT instanceof ParameterizedType && ((Class<?>) ((ParameterizedType) curT).getRawType()).equals(baseClass))
				&& !(curT instanceof Class<?> && ((Class<?>) curT).equals(baseClass))) {
			if (typeHierarchy != null) {
				typeHierarchy.add(curT);
			}
			
			// parameterized type
			if (curT instanceof ParameterizedType) {
				curT = ((Class<?>) ((ParameterizedType) curT).getRawType()).getGenericSuperclass();
			}
			// class
			else {
				curT = ((Class<?>) curT).getGenericSuperclass();
			}
		}
		
		// check if immediate child of base class has generics
		if (curT instanceof Class<?>) {
			throw new InvalidTypesException("Function needs to be parameterized by using generics.");
		}
		
		if (typeHierarchy != null) {
			typeHierarchy.add(curT);
		}
		
		ParameterizedType baseClassChild = (ParameterizedType) curT;
		
		return baseClassChild.getActualTypeArguments()[pos];
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfoWithTypeHierarchy(ArrayList<Type> typeHierarchy, Type t,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		
		// check if type is a subclass of tuple
		if ((t instanceof Class<?> && Tuple.class.isAssignableFrom((Class<?>) t))
				|| (t instanceof ParameterizedType && Tuple.class.isAssignableFrom((Class<?>) ((ParameterizedType) t).getRawType()))) {
			
			Type curT = t;
			
			// do not allow usage of Tuple as type
			if (curT instanceof Class<?> && ((Class<?>) curT).equals(Tuple.class)) {
				throw new InvalidTypesException(
						"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
			}
			
			// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
			// collect the types while moving up for a later top-down 
			while (!(curT instanceof ParameterizedType && ((Class<?>) ((ParameterizedType) curT).getRawType()).getSuperclass().equals(
					Tuple.class))
					&& !(curT instanceof Class<?> && ((Class<?>) curT).getSuperclass().equals(Tuple.class))) {
				typeHierarchy.add(curT);
				
				// parameterized type
				if (curT instanceof ParameterizedType) {
					curT = ((Class<?>) ((ParameterizedType) curT).getRawType()).getGenericSuperclass();
				}
				// class
				else {
					curT = ((Class<?>) curT).getGenericSuperclass();
				}
			}
			
			// check if immediate child of Tuple has generics
			if (curT instanceof Class<?>) {
				throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
			}
			
			ParameterizedType tupleChild = (ParameterizedType) curT;
			
			Type[] subtypes = new Type[tupleChild.getActualTypeArguments().length];
			
			// materialize possible type variables
			for (int i = 0; i < subtypes.length; i++) {
				// materialize immediate TypeVariables
				if (tupleChild.getActualTypeArguments()[i] instanceof TypeVariable<?>) {
					Type varContent = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) tupleChild.getActualTypeArguments()[i]);
					// variable could not be materialized
					if (varContent == null) {
						// add the TypeVariable as subtype for step in next section
						subtypes[i] = tupleChild.getActualTypeArguments()[i];
					} else {
						// add class or parameterized type
						subtypes[i] = varContent;
					}
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
					ParameterizedType immediateBaseChild = (ParameterizedType) typeHierarchy.get(typeHierarchy.size() - 1);
					tupleSubTypes[i] = createTypeInfoWithImmediateBaseChildInput(immediateBaseChild, (TypeVariable<?>) subtypes[i],
							in1Type, in2Type);
					
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
			
			// TODO: Check that type that extends Tuple does not have additional fields.
			// Right now, these fields are not be serialized by the TupleSerializer. 
			// We might want to add an ExtendedTupleSerializer for that. 
			
			if (t instanceof Class<?>) {
				return new TupleTypeInfo(((Class<? extends Tuple>) t), tupleSubTypes);
			} else if (t instanceof ParameterizedType) {
				return new TupleTypeInfo(((Class<? extends Tuple>) ((ParameterizedType) t).getRawType()), tupleSubTypes);
			}
		}
		// type depends on another type
		// e.g. class MyMapper<E> extends MapFunction<String, E>
		else if (t instanceof TypeVariable) {
			Type typeVar = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) t);
			
			if (typeVar != null) {
				return createTypeInfoWithTypeHierarchy(typeHierarchy, typeVar, in1Type, in2Type);
			}
			// try to derive the type info of the TypeVariable from the immediate base child input as a last attempt
			else {
				ParameterizedType immediateBaseChild = (ParameterizedType) typeHierarchy.get(typeHierarchy.size() - 1);
				TypeInformation<OUT> typeInfo = (TypeInformation<OUT>) createTypeInfoWithImmediateBaseChildInput(immediateBaseChild,
						(TypeVariable<?>) t, in1Type, in2Type);
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
				
				Class<OUT> classArray = null;
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
		// objects with generics are treated as raw type
		else if (t instanceof ParameterizedType) {
			return getForClass((Class<OUT>) ((ParameterizedType) t).getRawType());
		}
		// no tuple, no TypeVariable, no generic type
		else if (t instanceof Class) {
			return getForClass((Class<OUT>) t);
		}
		
		throw new InvalidTypesException("Type Information could not be created.");
	}
	
	private static String encodePrimitiveClass(Class<?> primitiveClass) {
		final String name = primitiveClass.getName();
		if (name.equals("boolean")) {
			return "Z";
		}
		else if (name.equals("byte")) {
			return "B";
		}
		else if (name.equals("char")) {
			return "C";
		}
		else if (name.equals("double")) {
			return "D";
		}
		else if (name.equals("float")) {
			return "F";
		}
		else if (name.equals("int")) {
			return "I";
		}
		else if (name.equals("long")) {
			return "J";
		}
		else if (name.equals("short")) {
			return "S";
		}
		throw new InvalidTypesException();
	}
	
	private static <IN1, IN2> TypeInformation<?> createTypeInfoWithImmediateBaseChildInput(ParameterizedType baseChild,
			TypeVariable<?> typeVar, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		Type[] baseChildArgs = baseChild.getActualTypeArguments();
		
		TypeInformation<?> info = null;
		if (in1Type != null) {
			info = findCorrespondingInfo(typeVar, baseChildArgs[0], in1Type);
		}
		
		if (info == null && in2Type != null) {
			info = findCorrespondingInfo(typeVar, baseChildArgs[1], in2Type);
		}
		
		if (info != null) {
			return info;
		}
		
		return null;
	}
	
	private static TypeInformation<?> findCorrespondingInfo(TypeVariable<?> typeVar, Type type, TypeInformation<?> corrInfo) {
		if (type instanceof TypeVariable) {
			TypeVariable<?> variable = (TypeVariable<?>) type;
			if (variable.getName().equals(typeVar.getName()) && variable.getGenericDeclaration().equals(typeVar.getGenericDeclaration())) {
				return corrInfo;
			}
		} else if (type instanceof ParameterizedType && Tuple.class.isAssignableFrom((Class<?>) ((ParameterizedType) type).getRawType())) {
			ParameterizedType tuple = (ParameterizedType) type;
			Type[] args = tuple.getActualTypeArguments();
			
			for (int i = 0; i < args.length; i++) {
				TypeInformation<?> info = findCorrespondingInfo(typeVar, args[i], ((TupleTypeInfo<?>) corrInfo).getTypeAt(i));
				if (info != null) {
					return info;
				}
			}
		}
		return null;
	}
	
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
					if (curVarOfCurT.getName().equals(inTypeTypeVar.getName())
							&& curVarOfCurT.getGenericDeclaration().equals(inTypeTypeVar.getGenericDeclaration())) {
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
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public static <X> TypeInformation<X> getForClass(Class<X> clazz) {
		Validate.notNull(clazz);
		
		// check for abstract classes or interfaces
		if (Modifier.isInterface(clazz.getModifiers()) || (Modifier.isAbstract(clazz.getModifiers()) && !clazz.isArray())) {
			throw new InvalidTypesException("Interfaces and abstract classes are not valid types.");
		}
		
		// check for arrays
		if (clazz.isArray()) {

			// primitive arrays: int[], byte[], ...
			PrimitiveArrayTypeInfo<X> primitiveArrayInfo = PrimitiveArrayTypeInfo.getInfoFor(clazz);
			if (primitiveArrayInfo != null) {
				return primitiveArrayInfo;
			}
			
			// basic type arrays: String[], Integer[], Double[]
			BasicArrayTypeInfo<X, ?> basicArrayInfo = BasicArrayTypeInfo.getInfoFor(clazz);
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
			return (TypeInformation<X>) WritableTypeInfo.getWritableTypeInfo((Class<? extends Writable>) clazz);
		}
		
		// check for basic types
		TypeInformation<X> basicTypeInfo = BasicTypeInfo.getInfoFor(clazz);
		if (basicTypeInfo != null) {
			return basicTypeInfo;
		}
		
		// check for subclasses of Value
		if (Value.class.isAssignableFrom(clazz)) {
			Class<? extends Value> valueClass = clazz.asSubclass(Value.class);
			return (TypeInformation<X>) ValueTypeInfo.getValueTypeInfo(valueClass);
		}
		
		// check for subclasses of Tuple
		if (Tuple.class.isAssignableFrom(clazz)) {
			throw new InvalidTypesException("Type information extraction for tuples cannot be done based on the class.");
		}
		
		// return a generic type
		return new GenericTypeInfo<X>(clazz);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <X> TypeInformation<X> getForObject(X value) {
		Validate.notNull(value);
		
		// check if we can extract the types from tuples, otherwise work with the class
		if (value instanceof Tuple) {
			Tuple t = (Tuple) value;
			int numFields = t.getArity();
			
			TypeInformation<?>[] infos = new TypeInformation[numFields];
			for (int i = 0; i < numFields; i++) {
				Object field = t.getField(i);
				
				if (field == null) {
					throw new InvalidTypesException("Automatic type extraction is not possible on candidates with null values. "
							+ "Please specify the types directly.");
				}
				
				infos[i] = getForObject(field);
			}
			
			return (TypeInformation<X>) new TupleTypeInfo(value.getClass(), infos);
		} else {
			return getForClass((Class<X>) value.getClass());
		}
	}
	
	private TypeExtractor() {
	}
}
