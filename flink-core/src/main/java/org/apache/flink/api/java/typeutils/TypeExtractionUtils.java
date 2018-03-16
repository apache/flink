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
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.InvalidTypesException;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.shaded.asm5.org.objectweb.asm.Type.getConstructorDescriptor;
import static org.apache.flink.shaded.asm5.org.objectweb.asm.Type.getMethodDescriptor;

@Internal
public class TypeExtractionUtils {

	private TypeExtractionUtils() {
		// do not allow instantiation
	}

	/**
	 * Similar to a Java 8 Executable but with a return type.
	 */
	public static class LambdaExecutable {

		private Type[] parameterTypes;
		private Type returnType;
		private String name;
		private Object executable;

		public LambdaExecutable(Constructor<?> constructor) {
			this.parameterTypes = constructor.getGenericParameterTypes();
			this.returnType = constructor.getDeclaringClass();
			this.name = constructor.getName();
			this.executable = constructor;
		}

		public LambdaExecutable(Method method) {
			this.parameterTypes = method.getGenericParameterTypes();
			this.returnType = method.getGenericReturnType();
			this.name = method.getName();
			this.executable = method;
		}

		public Type[] getParameterTypes() {
			return parameterTypes;
		}

		public Type getReturnType() {
			return returnType;
		}

		public String getName() {
			return name;
		}

		public boolean executablesEquals(Method m) {
			return executable.equals(m);
		}

		public boolean executablesEquals(Constructor<?> c) {
			return executable.equals(c);
		}
	}

	/**
	 * Checks if the given function has been implemented using a Java 8 lambda. If yes, a LambdaExecutable
	 * is returned describing the method/constructor. Otherwise null.
	 *
	 * @throws TypeExtractionException lambda extraction is pretty hacky, it might fail for unknown JVM issues.
	 */
	public static LambdaExecutable checkAndExtractLambda(Function function) throws TypeExtractionException {
		try {
			// get serialized lambda
			SerializedLambda serializedLambda = null;
			for (Class<?> clazz = function.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
				try {
					Method replaceMethod = clazz.getDeclaredMethod("writeReplace");
					replaceMethod.setAccessible(true);
					Object serialVersion = replaceMethod.invoke(function);

					// check if class is a lambda function
					if (serialVersion != null && serialVersion.getClass() == SerializedLambda.class) {
						serializedLambda = (SerializedLambda) serialVersion;
						break;
					}
				}
				catch (NoSuchMethodException e) {
					// thrown if the method is not there. fall through the loop
				}
			}

			// not a lambda method -> return null
			if (serializedLambda == null) {
				return null;
			}

			// find lambda method
			String className = serializedLambda.getImplClass();
			String methodName = serializedLambda.getImplMethodName();
			String methodSig = serializedLambda.getImplMethodSignature();

			Class<?> implClass = Class.forName(className.replace('/', '.'), true, Thread.currentThread().getContextClassLoader());

			// find constructor
			if (methodName.equals("<init>")) {
				Constructor<?>[] constructors = implClass.getDeclaredConstructors();
				for (Constructor<?> constructor : constructors) {
					if (getConstructorDescriptor(constructor).equals(methodSig)) {
						return new LambdaExecutable(constructor);
					}
				}
			}
			// find method
			else {
				List<Method> methods = getAllDeclaredMethods(implClass);
				for (Method method : methods) {
					if (method.getName().equals(methodName) && getMethodDescriptor(method).equals(methodSig)) {
						return new LambdaExecutable(method);
					}
				}
			}
			throw new TypeExtractionException("No lambda method found.");
		}
		catch (Exception e) {
			throw new TypeExtractionException("Could not extract lambda method out of function: " +
				e.getClass().getSimpleName() + " - " + e.getMessage(), e);
		}
	}

	/**
	 * Extracts type from given index from lambda. It supports nested types.
	 *
	 * @param exec lambda function to extract the type from
	 * @param lambdaTypeArgumentIndices position of type to extract in type hierarchy
	 * @param paramLen count of total parameters of the lambda (including closure parameters)
	 * @param baseParametersLen count of lambda interface parameters (without closure parameters)
	 * @return extracted type
	 */
	public static Type extractTypeFromLambda(
		LambdaExecutable exec,
		int[] lambdaTypeArgumentIndices,
		int paramLen,
		int baseParametersLen) {
		Type output = exec.getParameterTypes()[paramLen - baseParametersLen + lambdaTypeArgumentIndices[0]];
		for (int i = 1; i < lambdaTypeArgumentIndices.length; i++) {
			output = extractTypeArgument(output, lambdaTypeArgumentIndices[i]);
		}
		return output;
	}

	/**
	 * This method extracts the n-th type argument from the given type. An InvalidTypesException
	 * is thrown if the type does not have any type arguments or if the index exceeds the number
	 * of type arguments.
	 *
	 * @param t Type to extract the type arguments from
	 * @param index Index of the type argument to extract
	 * @return The extracted type argument
	 * @throws InvalidTypesException if the given type does not have any type arguments or if the
	 * index exceeds the number of type arguments.
	 */
	public static Type extractTypeArgument(Type t, int index) throws InvalidTypesException {
		if (t instanceof ParameterizedType) {
			Type[] actualTypeArguments = ((ParameterizedType) t).getActualTypeArguments();

			if (index < 0 || index >= actualTypeArguments.length) {
				throw new InvalidTypesException("Cannot extract the type argument with index " +
												index + " because the type has only " + actualTypeArguments.length +
												" type arguments.");
			} else {
				return actualTypeArguments[index];
			}
		} else {
			throw new InvalidTypesException("The given type " + t + " is not a parameterized type.");
		}
	}

	/**
	 * Extracts a Single Abstract Method (SAM) as defined in Java Specification (4.3.2. The Class Object,
	 * 9.8 Functional Interfaces, 9.4.3 Interface Method Body) from given class.
	 *
	 * @param baseClass a class that is a FunctionalInterface to retrieve a SAM from
	 * @throws InvalidTypesException if the given class does not implement FunctionalInterface
	 * @return single abstract method of the given class
	 */
	public static Method getSingleAbstractMethod(Class<?> baseClass) {

		if (!baseClass.isInterface()) {
			throw new InvalidTypesException("Given class: " + baseClass + "is not a FunctionalInterface.");
		}

		Method sam = null;
		for (Method method : baseClass.getMethods()) {
			if (Modifier.isAbstract(method.getModifiers())) {
				if (sam == null) {
					sam = method;
				} else {
					throw new InvalidTypesException("Given class: " + baseClass +
						" is not a FunctionalInterface. It has more than one abstract method.");
				}
			}
		}

		if (sam == null) {
			throw new InvalidTypesException(
				"Given class: " + baseClass + " is not a FunctionalInterface. It does not have any abstract methods.");
		}

		return sam;
	}

	/**
	 * Returns all declared methods of a class including methods of superclasses.
	 */
	public static List<Method> getAllDeclaredMethods(Class<?> clazz) {
		List<Method> result = new ArrayList<>();
		while (clazz != null) {
			Method[] methods = clazz.getDeclaredMethods();
			Collections.addAll(result, methods);
			clazz = clazz.getSuperclass();
		}
		return result;
	}

	/**
	 * Convert ParameterizedType or Class to a Class.
	 */
	public static Class<?> typeToClass(Type t) {
		if (t instanceof Class) {
			return (Class<?>)t;
		}
		else if (t instanceof ParameterizedType) {
			return ((Class<?>) ((ParameterizedType) t).getRawType());
		}
		throw new IllegalArgumentException("Cannot convert type to class");
	}

	/**
	 * Checks if a type can be converted to a Class. This is true for ParameterizedType and Class.
	 */
	public static boolean isClassType(Type t) {
		return t instanceof Class<?> || t instanceof ParameterizedType;
	}

	/**
	 * Checks whether two types are type variables describing the same.
	 */
	public static boolean sameTypeVars(Type t1, Type t2) {
		return t1 instanceof TypeVariable &&
			t2 instanceof TypeVariable &&
			((TypeVariable<?>) t1).getName().equals(((TypeVariable<?>) t2).getName()) &&
			((TypeVariable<?>) t1).getGenericDeclaration().equals(((TypeVariable<?>) t2).getGenericDeclaration());
	}

	/**
	 * Traverses the type hierarchy of a type up until a certain stop class is found.
	 *
	 * @param t type for which a hierarchy need to be created
	 * @return type of the immediate child of the stop class
	 */
	public static Type getTypeHierarchy(List<Type> typeHierarchy, Type t, Class<?> stopAtClass) {
		while (!(isClassType(t) && typeToClass(t).equals(stopAtClass))) {
			typeHierarchy.add(t);
			t = typeToClass(t).getGenericSuperclass();

			if (t == null) {
				break;
			}
		}
		return t;
	}

	/**
	 * Returns true if the given class has a superclass of given name.
	 *
	 * @param clazz class to be analyzed
	 * @param superClassName class name of the super class
	 */
	public static boolean hasSuperclass(Class<?> clazz, String superClassName) {
		List<Type> hierarchy = new ArrayList<>();
		getTypeHierarchy(hierarchy, clazz, Object.class);
		for (Type t : hierarchy) {
			if (isClassType(t) && typeToClass(t).getName().equals(superClassName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns the raw class of both parameterized types and generic arrays.
	 * Returns java.lang.Object for all other types.
	 */
	public static Class<?> getRawClass(Type t) {
		if (isClassType(t)) {
			return typeToClass(t);
		} else if (t instanceof GenericArrayType) {
			Type component = ((GenericArrayType) t).getGenericComponentType();
			return Array.newInstance(getRawClass(component), 0).getClass();
		}
		return Object.class;
	}
}
