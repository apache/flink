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

package org.apache.flink.table.types.extraction.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.StructuredType;

import org.apache.flink.shaded.asm7.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm7.org.objectweb.asm.ClassVisitor;
import org.apache.flink.shaded.asm7.org.objectweb.asm.Label;
import org.apache.flink.shaded.asm7.org.objectweb.asm.MethodVisitor;
import org.apache.flink.shaded.asm7.org.objectweb.asm.Opcodes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.shaded.asm7.org.objectweb.asm.Type.getConstructorDescriptor;

/**
 * Utilities for performing reflection tasks.
 */
public final class ExtractionUtils {

	/**
	 * Helper method for creating consistent exceptions during extraction.
	 */
	public static ValidationException extractionError(String message, Object... args) {
		return extractionError(null, message, args);
	}

	/**
	 * Helper method for creating consistent exceptions during extraction.
	 */
	public static ValidationException extractionError(Throwable cause, String message, Object... args) {
		return new ValidationException(
			String.format(
				message,
				args
			),
			cause
		);
	}

	/**
	 * Collects the partially ordered type hierarchy (i.e. all involved super classes and super
	 * interfaces) of the given type.
	 */
	public static List<Type> collectTypeHierarchy(Type type) {
		Type currentType = type;
		Class<?> currentClass = toClass(type);
		final List<Type> typeHierarchy = new ArrayList<>();
		while (currentClass != null) {
			// collect type
			typeHierarchy.add(currentType);
			// collect super interfaces
			for (Type genericInterface : currentClass.getGenericInterfaces()) {
				final Class<?> interfaceClass = toClass(genericInterface);
				if (interfaceClass != null) {
					typeHierarchy.addAll(collectTypeHierarchy(genericInterface));
				}
			}
			currentType = currentClass.getGenericSuperclass();
			currentClass = toClass(currentType);
		}
		return typeHierarchy;
	}

	/**
	 * Converts a {@link Type} to {@link Class} if possible, {@code null} otherwise.
	 */
	public static @Nullable Class<?> toClass(Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			// this is always a class
			return (Class<?>) ((ParameterizedType) type).getRawType();
		}
		// unsupported: generic arrays, type variables, wildcard types
		return null;
	}

	/**
	 * Creates a raw data type.
	 */
	@SuppressWarnings("unchecked")
	public static DataType createRawType(
			DataTypeLookup lookup,
			@Nullable Class<? extends TypeSerializer<?>> rawSerializer,
			@Nullable Class<?> conversionClass) {
		if (rawSerializer != null) {
			return DataTypes.RAW((Class) createConversionClass(conversionClass), instantiateRawSerializer(rawSerializer));
		}
		return lookup.resolveRawDataType(createConversionClass(conversionClass));
	}

	private static Class<?> createConversionClass(@Nullable Class<?> conversionClass) {
		if (conversionClass != null) {
			return conversionClass;
		}
		return Object.class;
	}

	private static TypeSerializer<?> instantiateRawSerializer(Class<? extends TypeSerializer<?>> rawSerializer) {
		try {
			return rawSerializer.newInstance();
		} catch (Exception e) {
			throw extractionError(
				e,
				"Cannot instantiate type serializer '%s' for RAW type. " +
					"Make sure the class is publicly accessible and has a default constructor.",
				rawSerializer.getName()
			);
		}
	}

	/**
	 * Resolves a {@link TypeVariable} using the given type hierarchy if possible.
	 */
	public static Type resolveVariable(List<Type> typeHierarchy, TypeVariable<?> variable) {
		// iterate through hierarchy from top to bottom until type variable gets a non-variable assigned
		for (int i = typeHierarchy.size() - 1; i >= 0; i--) {
			final Type currentType = typeHierarchy.get(i);

			if (currentType instanceof ParameterizedType) {
				final Type resolvedType = resolveVariableInParameterizedType(
					variable,
					(ParameterizedType) currentType);
				if (resolvedType instanceof TypeVariable) {
					// follow type variables transitively
					variable = (TypeVariable<?>) resolvedType;
				} else if (resolvedType != null) {
					return resolvedType;
				}
			}
		}
		// unresolved variable
		return variable;
	}

	private static @Nullable Type resolveVariableInParameterizedType(
			TypeVariable<?> variable,
			ParameterizedType currentType) {
		final Class<?> currentRaw = (Class<?>) currentType.getRawType();
		final TypeVariable<?>[] currentVariables = currentRaw.getTypeParameters();
		// search for matching type variable
		for (int paramPos = 0; paramPos < currentVariables.length; paramPos++) {
			if (typeVariableEquals(variable, currentVariables[paramPos])) {
				return currentType.getActualTypeArguments()[paramPos];
			}
		}
		return null;
	}

	private static boolean typeVariableEquals(TypeVariable<?> variable, TypeVariable<?> currentVariable) {
		return currentVariable.getGenericDeclaration().equals(variable.getGenericDeclaration()) &&
				currentVariable.getName().equals(variable.getName());
	}

	/**
	 * Validates the characteristics of a class for a {@link StructuredType} such as accessibility.
	 */
	public static void validateStructuredClass(Class<?> clazz) {
		final int m = clazz.getModifiers();
		if (Modifier.isAbstract(m)) {
			throw extractionError("Class '%s' must not be abstract.", clazz.getName());
		}
		if (!Modifier.isPublic(m)) {
			throw extractionError("Class '%s' is not public.", clazz.getName());
		}
		if (clazz.getEnclosingClass() != null &&
				(clazz.getDeclaringClass() == null || !Modifier.isStatic(m))) {
			throw extractionError("Class '%s' is a not a static, globally accessible class.", clazz.getName());
		}
	}

	/**
	 * Returns the fields of a class for a {@link StructuredType}.
	 */
	public static List<Field> collectStructuredFields(Class<?> clazz) {
		final List<Field> fields = new ArrayList<>();
		while (clazz != Object.class) {
			final Field[] declaredFields = clazz.getDeclaredFields();
			Stream.of(declaredFields)
				.filter(field -> {
					final int m = field.getModifiers();
					return !Modifier.isStatic(m) && !Modifier.isTransient(m);
				})
				.forEach(fields::add);
			clazz = clazz.getSuperclass();
		}
		return fields;
	}

	/**
	 * Validates if a field is properly readable either directly or through a getter.
	 */
	public static void validateStructuredFieldReadability(Class<?> clazz, Field field) {
		final int m = field.getModifiers();

		// field is accessible
		if (Modifier.isPublic(m)) {
			return;
		}

		// field needs a getter
		if (!hasStructuredFieldGetter(clazz, field)) {
			throw extractionError(
				"Field '%s' of class '%s' is neither publicly accessible nor does it have " +
					"a corresponding getter method.",
				field.getName(),
				clazz.getName());
		}
	}

	/**
	 * Checks if a field is mutable or immutable. Returns {@code true} if the field is properly
	 * mutable. Returns {@code false} if it is properly immutable.
	 */
	public static boolean isStructuredFieldMutable(Class<?> clazz, Field field) {
		final int m = field.getModifiers();

		// field is immutable
		if (Modifier.isFinal(m)) {
			return false;
		}
		// field is directly mutable
		if (Modifier.isPublic(m)) {
			return true;
		}
		// field has setters by which it is mutable
		if (hasFieldSetter(clazz, field)) {
			return true;
		}

		throw extractionError(
			"Field '%s' of class '%s' is mutable but is neither publicly accessible nor does it have " +
				"a corresponding setter method.",
			field.getName(),
			clazz.getName());
	}

	/**
	 * Checks for a field setters. The logic is as broad as possible to support both Java and Scala
	 * in different flavors.
	 */
	public static boolean hasFieldSetter(Class<?> clazz, Field field) {
		final String normalizedFieldName = field.getName().toUpperCase();

		final List<Method> methods = collectStructuredMethods(clazz);
		for (Method method : methods) {

			// check name:
			// set<Name>(type)
			// <Name>(type)
			// <Name>_$eq(type) for Scala
			final String normalizedMethodName = method.getName().toUpperCase();
			final boolean hasName = normalizedMethodName.equals("SET" + normalizedFieldName) ||
				normalizedMethodName.equals(normalizedFieldName) ||
				normalizedMethodName.equals(normalizedFieldName + "_$EQ");
			if (!hasName) {
				continue;
			}

			// check return type:
			// void or the declaring class
			final Class<?> returnType = method.getReturnType();
			final boolean hasReturnType = returnType == Void.TYPE || returnType == clazz;
			if (!hasReturnType) {
				continue;
			}

			// check parameters:
			// one parameter that has the same (or primitive) type of the field
			final boolean hasParameter = method.getParameterCount() == 1 &&
				(method.getGenericParameterTypes()[0].equals(field.getGenericType()) ||
					boxPrimitive(method.getGenericParameterTypes()[0]).equals(field.getGenericType()));
			if (!hasParameter) {
				continue;
			}

			// matching setter found
			return true;
		}

		// no setter found
		return false;
	}

	private static final Map<Class<?>, Class<?>> primitiveWrapperMap = new HashMap<>();
	static {
		primitiveWrapperMap.put(Boolean.TYPE, Boolean.class);
		primitiveWrapperMap.put(Byte.TYPE, Byte.class);
		primitiveWrapperMap.put(Character.TYPE, Character.class);
		primitiveWrapperMap.put(Short.TYPE, Short.class);
		primitiveWrapperMap.put(Integer.TYPE, Integer.class);
		primitiveWrapperMap.put(Long.TYPE, Long.class);
		primitiveWrapperMap.put(Double.TYPE, Double.class);
		primitiveWrapperMap.put(Float.TYPE, Float.class);
	}

	/**
	 * Returns the boxed type of a primitive type.
	 */
	public static Type boxPrimitive(Type type) {
		if (type instanceof Class && ((Class) type).isPrimitive()) {
			return primitiveWrapperMap.get(type);
		}
		return type;
	}

	/**
	 * Checks for a field getter. The logic is as broad as possible to support both Java and Scala
	 * in different flavors.
	 */
	public static boolean hasStructuredFieldGetter(Class<?> clazz, Field field) {
		final String normalizedFieldName = field.getName().toUpperCase();

		final List<Method> methods = collectStructuredMethods(clazz);
		for (Method method : methods) {
			// check name:
			// get<Name>()
			// is<Name>()
			// <Name>() for Scala
			final String normalizedMethodName = method.getName().toUpperCase();
			final boolean hasName = normalizedMethodName.equals("GET" + normalizedFieldName) ||
				normalizedMethodName.equals("IS" + normalizedFieldName) ||
				normalizedMethodName.equals(normalizedFieldName);
			if (!hasName) {
				continue;
			}

			// check return type:
			// equal to field type
			final Type returnType = method.getGenericReturnType();
			final boolean hasReturnType = returnType.equals(field.getGenericType());
			if (!hasReturnType) {
				continue;
			}

			// check parameters:
			// no parameters
			final boolean hasNoParameters = method.getParameterCount() == 0;
			if (!hasNoParameters) {
				continue;
			}

			// matching getter found
			return true;
		}

		// no getter found
		return false;
	}

	/**
	 * Collects all methods that qualify as methods of a {@link StructuredType}.
	 */
	public static List<Method> collectStructuredMethods(Class<?> clazz) {
		final List<Method> methods = new ArrayList<>();
		while (clazz != Object.class) {
			final Method[] declaredMethods = clazz.getDeclaredMethods();
			Stream.of(declaredMethods)
				.filter(field -> {
					final int m = field.getModifiers();
					return Modifier.isPublic(m) && !Modifier.isNative(m) && !Modifier.isAbstract(m);
				})
				.forEach(methods::add);
			clazz = clazz.getSuperclass();
		}
		return methods;
	}

	// --------------------------------------------------------------------------------------------
	// Assignable Constructor Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Result of the extraction in {@link #extractAssigningConstructor(Class, List)}.
	 */
	public static class AssigningConstructor {
		public final Constructor<?> constructor;
		public final List<String> parameterNames;

		private AssigningConstructor(Constructor<?> constructor, List<String> parameterNames) {
			this.constructor = constructor;
			this.parameterNames = parameterNames;
		}
	}

	/**
	 * Checks whether the given constructor takes all of the given fields with matching (possibly
	 * primitive) type and name. An assigning constructor can define the order of fields.
	 */
	public static @Nullable AssigningConstructor extractAssigningConstructor(
			Class<?> clazz,
			List<Field> fields) {
		AssigningConstructor foundConstructor = null;
		for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
			final boolean qualifyingConstructor = Modifier.isPublic(constructor.getModifiers()) &&
				constructor.getParameterTypes().length == fields.size();
			if (!qualifyingConstructor) {
				continue;
			}
			final List<String> parameterNames = extractConstructorParameterNames(clazz, constructor, fields);
			if (parameterNames != null) {
				if (foundConstructor != null) {
					throw extractionError(
						"Multiple constructors found that assign all fields for class '%s'.",
						clazz.getName());
				}
				foundConstructor = new AssigningConstructor(constructor, parameterNames);
			}
		}
		return foundConstructor;
	}

	/**
	 * Extracts ordered parameter names from a constructor that takes all of the given fields with
	 * matching (possibly primitive) type and name.
	 */
	private static @Nullable List<String> extractConstructorParameterNames(
			Class<?> clazz,
			Constructor<?> constructor,
			List<Field> fields) {
		final Type[] parameterTypes = constructor.getGenericParameterTypes();

		// by default parameter names are "arg0, arg1, arg2, ..." if compiler flag is not set
		// so we need to extract them manually if possible
		List<String> parameterNames = Stream.of(constructor.getParameters())
			.map(Parameter::getName)
			.collect(Collectors.toList());
		if (parameterNames.stream().allMatch(n -> n.startsWith("arg"))) {
			final ParameterExtractor extractor = new ParameterExtractor(constructor);
			getClassReader(clazz).accept(extractor, 0);

			final List<String> extractedNames = extractor.getParameterNames();
			if (extractedNames.size() == 0 || !extractedNames.get(0).equals("this")) {
				return null;
			}
			// remove "this" and additional local variables
			// select less names if class file has not the required information
			parameterNames = extractedNames.subList(1, Math.min(fields.size() + 1, extractedNames.size()));
		}

		if (parameterNames.size() != fields.size()) {
			return null;
		}

		final Map<String, Type> fieldMap = fields.stream()
			.collect(Collectors.toMap(Field::getName, Field::getGenericType));

		// check that all fields are represented in the parameters of the constructor
		for (int i = 0; i < parameterNames.size(); i++) {
			final String parameterName = parameterNames.get(i);
			final Type fieldType = fieldMap.get(parameterName); // might be null
			final Type parameterType = parameterTypes[i];
			// we are tolerant here because frameworks such as Avro accept a boxed type even though
			// the field is primitive
			if (!boxPrimitive(parameterType).equals(boxPrimitive(fieldType))) {
				return null;
			}
		}

		return parameterNames;
	}

	private static ClassReader getClassReader(Class<?> cls) {
		final String className = cls.getName().replaceFirst("^.*\\.", "") + ".class";
		try {
			return new ClassReader(cls.getResourceAsStream(className));
		} catch (IOException e) {
			throw new IllegalStateException("Could not instantiate ClassReader.", e);
		}
	}

	/**
	 * Extracts the parameter names and descriptors from a constructor. Assuming the existence of a
	 * local variable table.
	 *
	 * <p>For example:
	 * <pre>
	 * {@code
	 * public WC(java.lang.String arg0, long arg1) { // <init> //(Ljava/lang/String;J)V
	 *   <localVar:index=0 , name=this , desc=Lorg/apache/flink/WC;, sig=null, start=L1, end=L2>
	 *   <localVar:index=1 , name=word , desc=Ljava/lang/String;, sig=null, start=L1, end=L2>
	 *   <localVar:index=2 , name=frequency , desc=J, sig=null, start=L1, end=L2>
	 *   <localVar:index=2 , name=otherLocal , desc=J, sig=null, start=L1, end=L2>
	 *   <localVar:index=2 , name=otherLocal2 , desc=J, sig=null, start=L1, end=L2>
	 * }
	 * }
	 * </pre>
	 */
	private static class ParameterExtractor extends ClassVisitor {

		private final String constructorDescriptor;

		private final List<String> parameterNames = new ArrayList<>();

		public ParameterExtractor(Constructor constructor) {
			super(Opcodes.ASM7);
			constructorDescriptor = getConstructorDescriptor(constructor);
		}

		public List<String> getParameterNames() {
			return parameterNames;
		}

		@Override
		public MethodVisitor visitMethod(
				int access,
				String name,
				String descriptor,
				String signature,
				String[] exceptions) {
			if (descriptor.equals(constructorDescriptor)) {
				return new MethodVisitor(Opcodes.ASM7) {
					@Override
					public void visitLocalVariable(
							String name,
							String descriptor,
							String signature,
							Label start,
							Label end,
							int index) {
						parameterNames.add(name);
					}
				};
			}
			return super.visitMethod(access, name, descriptor, signature, exceptions);
		}
	}

	private ExtractionUtils() {
		// no instantiation
	}
}
