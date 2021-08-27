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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.StructuredType;

import org.apache.flink.shaded.asm7.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm7.org.objectweb.asm.ClassVisitor;
import org.apache.flink.shaded.asm7.org.objectweb.asm.Label;
import org.apache.flink.shaded.asm7.org.objectweb.asm.MethodVisitor;
import org.apache.flink.shaded.asm7.org.objectweb.asm.Opcodes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.shaded.asm7.org.objectweb.asm.Type.getConstructorDescriptor;
import static org.apache.flink.shaded.asm7.org.objectweb.asm.Type.getMethodDescriptor;

/** Utilities for performing reflection tasks. */
@Internal
public final class ExtractionUtils {

    // --------------------------------------------------------------------------------------------
    // Methods shared across packages
    // --------------------------------------------------------------------------------------------

    /** Collects methods of the given name. */
    public static List<Method> collectMethods(Class<?> function, String methodName) {
        return Arrays.stream(function.getMethods())
                .filter(method -> method.getName().equals(methodName))
                .sorted(Comparator.comparing(Method::toString)) // for deterministic order
                .collect(Collectors.toList());
    }

    /**
     * Checks whether a method/constructor can be called with the given argument classes. This
     * includes type widening and vararg. {@code null} is a wildcard.
     *
     * <p>E.g., {@code (int.class, int.class)} matches {@code f(Object...), f(int, int), f(Integer,
     * Object)} and so forth.
     */
    public static boolean isInvokable(Executable executable, Class<?>... classes) {
        final int m = executable.getModifiers();
        if (!Modifier.isPublic(m)) {
            return false;
        }
        final int paramCount = executable.getParameterCount();
        final int classCount = classes.length;
        // check for enough classes for each parameter
        if ((!executable.isVarArgs() && classCount != paramCount)
                || (executable.isVarArgs() && classCount < paramCount - 1)) {
            return false;
        }
        int currentClass = 0;
        for (int currentParam = 0; currentParam < paramCount; currentParam++) {
            final Class<?> param = executable.getParameterTypes()[currentParam];
            // last parameter is a vararg that needs to consume remaining classes
            if (currentParam == paramCount - 1 && executable.isVarArgs()) {
                final Class<?> paramComponent =
                        executable.getParameterTypes()[currentParam].getComponentType();
                // we have more than 1 classes left so the vararg needs to consume them all
                if (classCount - currentClass > 1) {
                    while (currentClass < classCount
                            && ExtractionUtils.isAssignable(
                                    classes[currentClass], paramComponent, true)) {
                        currentClass++;
                    }
                } else if (currentClass < classCount
                        && (parameterMatches(classes[currentClass], param)
                                || parameterMatches(classes[currentClass], paramComponent))) {
                    currentClass++;
                }
            }
            // entire parameter matches
            else if (parameterMatches(classes[currentClass], param)) {
                currentClass++;
            }
        }
        // check if all classes have been consumed
        return currentClass == classCount;
    }

    private static boolean parameterMatches(Class<?> clz, Class<?> param) {
        return clz == null || ExtractionUtils.isAssignable(clz, param, true);
    }

    /** Creates a method signature string like {@code int eval(Integer, String)}. */
    public static String createMethodSignatureString(
            String methodName, Class<?>[] parameters, @Nullable Class<?> returnType) {
        final StringBuilder builder = new StringBuilder();
        if (returnType != null) {
            builder.append(returnType.getCanonicalName()).append(" ");
        }
        builder.append(methodName)
                .append(
                        Stream.of(parameters)
                                .map(
                                        parameter -> {
                                            // in case we don't know the parameter at this location
                                            // (i.e. for accumulators)
                                            if (parameter == null) {
                                                return "_";
                                            } else {
                                                return parameter.getCanonicalName();
                                            }
                                        })
                                .collect(Collectors.joining(", ", "(", ")")));
        return builder.toString();
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
        if (clazz.getEnclosingClass() != null
                && (clazz.getDeclaringClass() == null || !Modifier.isStatic(m))) {
            throw extractionError(
                    "Class '%s' is a not a static, globally accessible class.", clazz.getName());
        }
    }

    /**
     * Returns the field of a structured type. The logic is as broad as possible to support both
     * Java and Scala in different flavors.
     */
    public static Field getStructuredField(Class<?> clazz, String fieldName) {
        final String normalizedFieldName = fieldName.toUpperCase();

        final List<Field> fields = collectStructuredFields(clazz);
        for (Field field : fields) {
            if (field.getName().toUpperCase().equals(normalizedFieldName)) {
                return field;
            }
        }
        throw extractionError(
                "Could not find a field named '%s' in class '%s' for structured type.",
                fieldName, clazz.getName());
    }

    /**
     * Checks for a field getter of a structured type. The logic is as broad as possible to support
     * both Java and Scala in different flavors.
     */
    public static Optional<Method> getStructuredFieldGetter(Class<?> clazz, Field field) {
        final String normalizedFieldName = normalizeAccessorName(field.getName());

        final List<Method> methods = collectStructuredMethods(clazz);
        for (Method method : methods) {
            // check name:
            // get<Name>()
            // is<Name>()
            // <Name>() for Scala
            final String normalizedMethodName = normalizeAccessorName(method.getName());
            final boolean hasName =
                    normalizedMethodName.equals("GET" + normalizedFieldName)
                            || normalizedMethodName.equals("IS" + normalizedFieldName)
                            || normalizedMethodName.equals(normalizedFieldName);
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
            return Optional.of(method);
        }

        // no getter found
        return Optional.empty();
    }

    /**
     * Checks for a field setters of a structured type. The logic is as broad as possible to support
     * both Java and Scala in different flavors.
     */
    public static Optional<Method> getStructuredFieldSetter(Class<?> clazz, Field field) {
        final String normalizedFieldName = normalizeAccessorName(field.getName());

        final List<Method> methods = collectStructuredMethods(clazz);
        for (Method method : methods) {

            // check name:
            // set<Name>(type)
            // <Name>(type)
            // <Name>_$eq(type) for Scala
            final String normalizedMethodName = normalizeAccessorName(method.getName());
            final boolean hasName =
                    normalizedMethodName.equals("SET" + normalizedFieldName)
                            || normalizedMethodName.equals(normalizedFieldName)
                            || normalizedMethodName.equals(normalizedFieldName + "$EQ");
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
            final boolean hasParameter =
                    method.getParameterCount() == 1
                            && (method.getGenericParameterTypes()[0].equals(field.getGenericType())
                                    || primitiveToWrapper(method.getGenericParameterTypes()[0])
                                            .equals(field.getGenericType()));
            if (!hasParameter) {
                continue;
            }

            // matching setter found
            return Optional.of(method);
        }

        // no setter found
        return Optional.empty();
    }

    private static String normalizeAccessorName(String name) {
        return name.toUpperCase().replaceAll(Pattern.quote("_"), "");
    }

    /**
     * Checks for an invokable constructor matching the given arguments.
     *
     * @see #isInvokable(Executable, Class[])
     */
    public static boolean hasInvokableConstructor(Class<?> clazz, Class<?>... classes) {
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (isInvokable(constructor, classes)) {
                return true;
            }
        }
        return false;
    }

    /** Checks whether a field is directly readable without a getter. */
    public static boolean isStructuredFieldDirectlyReadable(Field field) {
        final int m = field.getModifiers();

        // field is directly readable
        return Modifier.isPublic(m);
    }

    /** Checks whether a field is directly writable without a setter or constructor. */
    public static boolean isStructuredFieldDirectlyWritable(Field field) {
        final int m = field.getModifiers();

        // field is immutable
        if (Modifier.isFinal(m)) {
            return false;
        }

        // field is directly writable
        return Modifier.isPublic(m);
    }

    /**
     * A minimal version to extract a generic parameter from a given class.
     *
     * <p>This method should only be used for very specific use cases, in most cases {@link
     * DataTypeExtractor#extractFromGeneric(DataTypeFactory, Class, int, Type)} should be more
     * appropriate.
     */
    public static Optional<Class<?>> extractSimpleGeneric(
            Class<?> baseClass, Class<?> clazz, int pos) {
        try {
            if (clazz.getSuperclass() != baseClass) {
                return Optional.empty();
            }
            final Type t =
                    ((ParameterizedType) clazz.getGenericSuperclass())
                            .getActualTypeArguments()[pos];
            return Optional.ofNullable(toClass(t));
        } catch (Exception unused) {
            return Optional.empty();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Methods intended for this package
    // --------------------------------------------------------------------------------------------

    /** Helper method for creating consistent exceptions during extraction. */
    static ValidationException extractionError(String message, Object... args) {
        return extractionError(null, message, args);
    }

    /** Helper method for creating consistent exceptions during extraction. */
    static ValidationException extractionError(Throwable cause, String message, Object... args) {
        return new ValidationException(String.format(message, args), cause);
    }

    /**
     * Collects the partially ordered type hierarchy (i.e. all involved super classes and super
     * interfaces) of the given type.
     */
    static List<Type> collectTypeHierarchy(Type type) {
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

    /** Converts a {@link Type} to {@link Class} if possible, {@code null} otherwise. */
    static @Nullable Class<?> toClass(Type type) {
        if (type instanceof Class) {
            return (Class<?>) type;
        } else if (type instanceof ParameterizedType) {
            // this is always a class
            return (Class<?>) ((ParameterizedType) type).getRawType();
        }
        // unsupported: generic arrays, type variables, wildcard types
        return null;
    }

    /** Creates a raw data type. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static DataType createRawType(
            DataTypeFactory typeFactory,
            @Nullable Class<? extends TypeSerializer<?>> rawSerializer,
            @Nullable Class<?> conversionClass) {
        if (rawSerializer != null) {
            return DataTypes.RAW(
                    (Class) createConversionClass(conversionClass),
                    instantiateRawSerializer(rawSerializer));
        }
        return typeFactory.createRawDataType(createConversionClass(conversionClass));
    }

    static Class<?> createConversionClass(@Nullable Class<?> conversionClass) {
        if (conversionClass != null) {
            return conversionClass;
        }
        return Object.class;
    }

    private static TypeSerializer<?> instantiateRawSerializer(
            Class<? extends TypeSerializer<?>> rawSerializer) {
        try {
            return rawSerializer.newInstance();
        } catch (Exception e) {
            throw extractionError(
                    e,
                    "Cannot instantiate type serializer '%s' for RAW type. "
                            + "Make sure the class is publicly accessible and has a default constructor.",
                    rawSerializer.getName());
        }
    }

    /** Resolves a {@link TypeVariable} using the given type hierarchy if possible. */
    static Type resolveVariable(List<Type> typeHierarchy, TypeVariable<?> variable) {
        // iterate through hierarchy from top to bottom until type variable gets a non-variable
        // assigned
        for (int i = typeHierarchy.size() - 1; i >= 0; i--) {
            final Type currentType = typeHierarchy.get(i);

            if (currentType instanceof ParameterizedType) {
                final Type resolvedType =
                        resolveVariableInParameterizedType(
                                variable, (ParameterizedType) currentType);
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
            TypeVariable<?> variable, ParameterizedType currentType) {
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

    private static boolean typeVariableEquals(
            TypeVariable<?> variable, TypeVariable<?> currentVariable) {
        return currentVariable.getGenericDeclaration().equals(variable.getGenericDeclaration())
                && currentVariable.getName().equals(variable.getName());
    }

    /**
     * Validates if a given type is not already contained in the type hierarchy of a structured
     * type.
     *
     * <p>Otherwise this would lead to infinite data type extraction cycles.
     */
    static void validateStructuredSelfReference(Type t, List<Type> typeHierarchy) {
        final Class<?> clazz = toClass(t);
        if (clazz != null
                && !clazz.isInterface()
                && clazz != Object.class
                && typeHierarchy.contains(t)) {
            throw extractionError(
                    "Cyclic reference detected for class '%s'. Attributes of structured types must not "
                            + "(transitively) reference the structured type itself.",
                    clazz.getName());
        }
    }

    /** Returns the fields of a class for a {@link StructuredType}. */
    static List<Field> collectStructuredFields(Class<?> clazz) {
        final List<Field> fields = new ArrayList<>();
        while (clazz != Object.class) {
            final Field[] declaredFields = clazz.getDeclaredFields();
            Stream.of(declaredFields)
                    .filter(
                            field -> {
                                final int m = field.getModifiers();
                                return !Modifier.isStatic(m) && !Modifier.isTransient(m);
                            })
                    .forEach(fields::add);
            clazz = clazz.getSuperclass();
        }
        return fields;
    }

    /** Validates if a field is properly readable either directly or through a getter. */
    static void validateStructuredFieldReadability(Class<?> clazz, Field field) {
        // field is accessible
        if (isStructuredFieldDirectlyReadable(field)) {
            return;
        }

        // field needs a getter
        if (!getStructuredFieldGetter(clazz, field).isPresent()) {
            throw extractionError(
                    "Field '%s' of class '%s' is neither publicly accessible nor does it have "
                            + "a corresponding getter method.",
                    field.getName(), clazz.getName());
        }
    }

    /**
     * Checks if a field is mutable or immutable. Returns {@code true} if the field is properly
     * mutable. Returns {@code false} if it is properly immutable.
     */
    static boolean isStructuredFieldMutable(Class<?> clazz, Field field) {
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
        if (getStructuredFieldSetter(clazz, field).isPresent()) {
            return true;
        }

        throw extractionError(
                "Field '%s' of class '%s' is mutable but is neither publicly accessible nor does it have "
                        + "a corresponding setter method.",
                field.getName(), clazz.getName());
    }

    /** Returns the boxed type of a primitive type. */
    static Type primitiveToWrapper(Type type) {
        if (type instanceof Class) {
            return primitiveToWrapper((Class<?>) type);
        }
        return type;
    }

    /** Collects all methods that qualify as methods of a {@link StructuredType}. */
    static List<Method> collectStructuredMethods(Class<?> clazz) {
        final List<Method> methods = new ArrayList<>();
        while (clazz != Object.class) {
            final Method[] declaredMethods = clazz.getDeclaredMethods();
            Stream.of(declaredMethods)
                    .filter(
                            field -> {
                                final int m = field.getModifiers();
                                return Modifier.isPublic(m)
                                        && !Modifier.isNative(m)
                                        && !Modifier.isAbstract(m);
                            })
                    .forEach(methods::add);
            clazz = clazz.getSuperclass();
        }
        return methods;
    }

    /**
     * Collects all annotations of the given type defined in the current class or superclasses.
     * Duplicates are ignored.
     */
    static <T extends Annotation> Set<T> collectAnnotationsOfClass(
            Class<T> annotation, Class<?> annotatedClass) {
        final List<Class<?>> classHierarchy = new ArrayList<>();
        Class<?> currentClass = annotatedClass;
        while (currentClass != null) {
            classHierarchy.add(currentClass);
            currentClass = currentClass.getSuperclass();
        }
        // convert to top down
        Collections.reverse(classHierarchy);
        return classHierarchy.stream()
                .flatMap(c -> Stream.of(c.getAnnotationsByType(annotation)))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Collects all annotations of the given type defined in the given method. Duplicates are
     * ignored.
     */
    static <T extends Annotation> Set<T> collectAnnotationsOfMethod(
            Class<T> annotation, Method annotatedMethod) {
        return new LinkedHashSet<>(Arrays.asList(annotatedMethod.getAnnotationsByType(annotation)));
    }

    // --------------------------------------------------------------------------------------------
    // Parameter Extraction Utilities
    // --------------------------------------------------------------------------------------------

    /** Result of the extraction in {@link #extractAssigningConstructor(Class, List)}. */
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
            Class<?> clazz, List<Field> fields) {
        AssigningConstructor foundConstructor = null;
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            final boolean qualifyingConstructor =
                    Modifier.isPublic(constructor.getModifiers())
                            && constructor.getParameterTypes().length == fields.size();
            if (!qualifyingConstructor) {
                continue;
            }
            final List<String> parameterNames =
                    extractConstructorParameterNames(constructor, fields);
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

    /** Extracts the parameter names of a method if possible. */
    static @Nullable List<String> extractMethodParameterNames(Method method) {
        return extractExecutableNames(method);
    }

    /**
     * Extracts ordered parameter names from a constructor that takes all of the given fields with
     * matching (possibly primitive and lenient) type and name.
     */
    private static @Nullable List<String> extractConstructorParameterNames(
            Constructor<?> constructor, List<Field> fields) {
        final Type[] parameterTypes = constructor.getGenericParameterTypes();

        List<String> parameterNames = extractExecutableNames(constructor);
        if (parameterNames == null) {
            return null;
        }

        final Map<String, Field> fieldMap =
                fields.stream()
                        .collect(
                                Collectors.toMap(
                                        f -> normalizeAccessorName(f.getName()),
                                        Function.identity()));

        // check that all fields are represented in the parameters of the constructor
        final List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < parameterNames.size(); i++) {
            final String parameterName = normalizeAccessorName(parameterNames.get(i));
            final Field field = fieldMap.get(parameterName);
            if (field == null) {
                return null;
            }
            final Type fieldType = field.getGenericType();
            final Type parameterType = parameterTypes[i];
            // we are tolerant here because frameworks such as Avro accept a boxed type even though
            // the field is primitive
            if (!primitiveToWrapper(parameterType).equals(primitiveToWrapper(fieldType))) {
                return null;
            }
            fieldNames.add(field.getName());
        }

        return fieldNames;
    }

    private static @Nullable List<String> extractExecutableNames(Executable executable) {
        final int offset;
        if (!Modifier.isStatic(executable.getModifiers())) {
            // remove "this" as first parameter
            offset = 1;
        } else {
            offset = 0;
        }
        // by default parameter names are "arg0, arg1, arg2, ..." if compiler flag is not set
        // so we need to extract them manually if possible
        List<String> parameterNames =
                Stream.of(executable.getParameters())
                        .map(Parameter::getName)
                        .collect(Collectors.toList());
        if (parameterNames.stream().allMatch(n -> n.startsWith("arg"))) {
            final ParameterExtractor extractor;
            if (executable instanceof Constructor) {
                extractor = new ParameterExtractor((Constructor<?>) executable);
            } else {
                extractor = new ParameterExtractor((Method) executable);
            }
            getClassReader(executable.getDeclaringClass()).accept(extractor, 0);

            final List<String> extractedNames = extractor.getParameterNames();
            if (extractedNames.size() == 0) {
                return null;
            }
            // remove "this" and additional local variables
            // select less names if class file has not the required information
            parameterNames =
                    extractedNames.subList(
                            offset,
                            Math.min(
                                    executable.getParameterCount() + offset,
                                    extractedNames.size()));
        }

        if (parameterNames.size() != executable.getParameterCount()) {
            return null;
        }

        return parameterNames;
    }

    private static ClassReader getClassReader(Class<?> cls) {
        final String className = cls.getName().replaceFirst("^.*\\.", "") + ".class";
        try (InputStream i = cls.getResourceAsStream(className)) {
            return new ClassReader(i);
        } catch (IOException e) {
            throw new IllegalStateException("Could not instantiate ClassReader.", e);
        }
    }

    /**
     * Extracts the parameter names and descriptors from a constructor or method. Assuming the
     * existence of a local variable table.
     *
     * <p>For example:
     *
     * <pre>{@code
     * public WC(java.lang.String arg0, long arg1) { // <init> //(Ljava/lang/String;J)V
     *   <localVar:index=0 , name=this , desc=Lorg/apache/flink/WC;, sig=null, start=L1, end=L2>
     *   <localVar:index=1 , name=word , desc=Ljava/lang/String;, sig=null, start=L1, end=L2>
     *   <localVar:index=2 , name=frequency , desc=J, sig=null, start=L1, end=L2>
     *   <localVar:index=2 , name=otherLocal , desc=J, sig=null, start=L1, end=L2>
     *   <localVar:index=2 , name=otherLocal2 , desc=J, sig=null, start=L1, end=L2>
     * }
     * }</pre>
     */
    private static class ParameterExtractor extends ClassVisitor {

        private static final int OPCODE = Opcodes.ASM7;

        private final String methodDescriptor;

        private final List<String> parameterNames = new ArrayList<>();

        ParameterExtractor(Constructor<?> constructor) {
            super(OPCODE);
            methodDescriptor = getConstructorDescriptor(constructor);
        }

        ParameterExtractor(Method method) {
            super(OPCODE);
            methodDescriptor = getMethodDescriptor(method);
        }

        List<String> getParameterNames() {
            return parameterNames;
        }

        @Override
        public MethodVisitor visitMethod(
                int access, String name, String descriptor, String signature, String[] exceptions) {
            if (descriptor.equals(methodDescriptor)) {
                return new MethodVisitor(OPCODE) {
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

    // --------------------------------------------------------------------------------------------
    // Class Assignment and Boxing
    //
    // copied from o.a.commons.lang3.ClassUtils (commons-lang3:3.3.2)
    // --------------------------------------------------------------------------------------------

    /**
     * Checks if one {@code Class} can be assigned to a variable of another {@code Class}.
     *
     * <p>Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this method takes into
     * account widenings of primitive classes and {@code null}s.
     *
     * <p>Primitive widenings allow an int to be assigned to a long, float or double. This method
     * returns the correct result for these cases.
     *
     * <p>{@code Null} may be assigned to any reference type. This method will return {@code true}
     * if {@code null} is passed in and the toClass is non-primitive.
     *
     * <p>Specifically, this method tests whether the type represented by the specified {@code
     * Class} parameter can be converted to the type represented by this {@code Class} object via an
     * identity conversion widening primitive or widening reference conversion. See <em><a
     * href="http://docs.oracle.com/javase/specs/">The Java Language Specification</a></em>,
     * sections 5.1.1, 5.1.2 and 5.1.4 for details.
     *
     * @param cls the Class to check, may be null
     * @param toClass the Class to try to assign into, returns false if null
     * @param autoboxing whether to use implicit autoboxing/unboxing between primitives and wrappers
     * @return {@code true} if assignment possible
     */
    public static boolean isAssignable(
            Class<?> cls, final Class<?> toClass, final boolean autoboxing) {
        if (toClass == null) {
            return false;
        }
        // have to check for null, as isAssignableFrom doesn't
        if (cls == null) {
            return !toClass.isPrimitive();
        }
        // autoboxing:
        if (autoboxing) {
            if (cls.isPrimitive() && !toClass.isPrimitive()) {
                cls = primitiveToWrapper(cls);
                if (cls == null) {
                    return false;
                }
            }
            if (toClass.isPrimitive() && !cls.isPrimitive()) {
                cls = wrapperToPrimitive(cls);
                if (cls == null) {
                    return false;
                }
            }
        }
        if (cls.equals(toClass)) {
            return true;
        }
        if (cls.isPrimitive()) {
            if (!toClass.isPrimitive()) {
                return false;
            }
            if (Integer.TYPE.equals(cls)) {
                return Long.TYPE.equals(toClass)
                        || Float.TYPE.equals(toClass)
                        || Double.TYPE.equals(toClass);
            }
            if (Long.TYPE.equals(cls)) {
                return Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
            }
            if (Boolean.TYPE.equals(cls)) {
                return false;
            }
            if (Double.TYPE.equals(cls)) {
                return false;
            }
            if (Float.TYPE.equals(cls)) {
                return Double.TYPE.equals(toClass);
            }
            if (Character.TYPE.equals(cls)) {
                return Integer.TYPE.equals(toClass)
                        || Long.TYPE.equals(toClass)
                        || Float.TYPE.equals(toClass)
                        || Double.TYPE.equals(toClass);
            }
            if (Short.TYPE.equals(cls)) {
                return Integer.TYPE.equals(toClass)
                        || Long.TYPE.equals(toClass)
                        || Float.TYPE.equals(toClass)
                        || Double.TYPE.equals(toClass);
            }
            if (Byte.TYPE.equals(cls)) {
                return Short.TYPE.equals(toClass)
                        || Integer.TYPE.equals(toClass)
                        || Long.TYPE.equals(toClass)
                        || Float.TYPE.equals(toClass)
                        || Double.TYPE.equals(toClass);
            }
            // should never get here
            return false;
        }
        return toClass.isAssignableFrom(cls);
    }

    /** Maps primitive {@code Class}es to their corresponding wrapper {@code Class}. */
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
        primitiveWrapperMap.put(Void.TYPE, Void.TYPE);
    }

    /** Maps wrapper {@code Class}es to their corresponding primitive types. */
    private static final Map<Class<?>, Class<?>> wrapperPrimitiveMap = new HashMap<>();

    static {
        for (final Class<?> primitiveClass : primitiveWrapperMap.keySet()) {
            final Class<?> wrapperClass = primitiveWrapperMap.get(primitiveClass);
            if (!primitiveClass.equals(wrapperClass)) {
                wrapperPrimitiveMap.put(wrapperClass, primitiveClass);
            }
        }
    }

    /**
     * Converts the specified primitive Class object to its corresponding wrapper Class object.
     *
     * <p>NOTE: From v2.2, this method handles {@code Void.TYPE}, returning {@code Void.TYPE}.
     *
     * @param cls the class to convert, may be null
     * @return the wrapper class for {@code cls} or {@code cls} if {@code cls} is not a primitive.
     *     {@code null} if null input.
     * @since 2.1
     */
    public static Class<?> primitiveToWrapper(final Class<?> cls) {
        Class<?> convertedClass = cls;
        if (cls != null && cls.isPrimitive()) {
            convertedClass = primitiveWrapperMap.get(cls);
        }
        return convertedClass;
    }

    /**
     * Converts the specified wrapper class to its corresponding primitive class.
     *
     * <p>This method is the counter part of {@code primitiveToWrapper()}. If the passed in class is
     * a wrapper class for a primitive type, this primitive type will be returned (e.g. {@code
     * Integer.TYPE} for {@code Integer.class}). For other classes, or if the parameter is
     * <b>null</b>, the return value is <b>null</b>.
     *
     * @param cls the class to convert, may be <b>null</b>
     * @return the corresponding primitive type if {@code cls} is a wrapper class, <b>null</b>
     *     otherwise
     * @see #primitiveToWrapper(Class)
     * @since 2.4
     */
    public static Class<?> wrapperToPrimitive(final Class<?> cls) {
        return wrapperPrimitiveMap.get(cls);
    }

    // --------------------------------------------------------------------------------------------

    private ExtractionUtils() {
        // no instantiation
    }
}
