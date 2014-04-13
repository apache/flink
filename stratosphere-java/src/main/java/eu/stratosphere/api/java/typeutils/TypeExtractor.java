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

import org.apache.commons.lang3.Validate;

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

	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapFunction, TypeInformation<IN> inType) {
		return createTypeInfo(MapFunction.class, mapFunction.getClass(), 1, inType, null);
	}

	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapFunction, TypeInformation<IN> inType) {
		return createTypeInfo(FlatMapFunction.class, flatMapFunction.getClass(), 1, inType, null);
	}

	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(GroupReduceFunction<IN, OUT> groupReduceFunction,
			TypeInformation<IN> inType) {
		return createTypeInfo(GroupReduceFunction.class, groupReduceFunction.getClass(), 1, inType, null);
	}

	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(JoinFunction<IN1, IN2, OUT> joinFunction,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		return createTypeInfo(JoinFunction.class, joinFunction.getClass(), 2, in1Type, in2Type);
	}

	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(CoGroupFunction<IN1, IN2, OUT> coGroupFunction,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		return createTypeInfo(CoGroupFunction.class, coGroupFunction.getClass(), 2, in1Type, in2Type);
	}

	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(CrossFunction<IN1, IN2, OUT> crossFunction,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		return createTypeInfo(CrossFunction.class, crossFunction.getClass(), 2, in1Type, in2Type);
	}

	public static <IN, OUT> TypeInformation<OUT> getKeyExtractorType(KeySelector<IN, OUT> selector, TypeInformation<IN> inType) {
		return createTypeInfo(KeySelector.class, selector.getClass(), 1, inType, null);
	}

	public static <IN> TypeInformation<IN> extractInputFormatTypes(InputFormat<IN, ?> format) {
		throw new UnsupportedOperationException("not implemented yet");
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
						throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) t).getName() + "' in '"
								+ ((TypeVariable<?>) t).getGenericDeclaration()
								+ "' could not be determined. This is most likely a type erasure problem.");
					}
				} else {
					tupleSubTypes[i] = createTypeInfoWithTypeHierarchy(typeHierarchy, subtypes[i], in1Type, in2Type);
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
							+ ((TypeVariable<?>) t).getGenericDeclaration() + "' could not be determined. " +
							"The type extraction currently supports types with generic variables only in cases where " +
							"all variables in the return type can be deduced from the input type(s).");
				}
			}
		}
		// arrays with generics 
		// (due to a Java 6 bug, it is possible that BasicArrayTypes also get classified as ObjectArrayTypes
		// since the JVM classifies e.g. String[] as GenericArrayType instead of Class)
		else if (t instanceof GenericArrayType) {
			GenericArrayType genericArray = (GenericArrayType) t;

			TypeInformation<?> componentInfo = createTypeInfoWithTypeHierarchy(typeHierarchy, genericArray.getGenericComponentType(),
					in1Type, in2Type);
			return ObjectArrayTypeInfo.getInfoFor(t, componentInfo);
		}
		// objects with generics are treated as raw type
		else if (t instanceof ParameterizedType) {
			return getForClass((Class<OUT>)((ParameterizedType) t).getRawType());
		}
		// no tuple, no TypeVariable, no generic type
		else if (t instanceof Class) {
			return getForClass((Class<OUT>) t);
		}

		throw new InvalidTypesException("Type Information could not be created.");
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
			// basic arrays
			if (BasicTypeInfo.getInfoFor(clazz.getComponentType()) != null) {
				return BasicArrayTypeInfo.getInfoFor(clazz);
			}
			// object arrays
			else {
				return ObjectArrayTypeInfo.getInfoFor(clazz);
			}
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
