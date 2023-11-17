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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava31.com.google.common.primitives.Primitives;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Ported {@code UserDefinedFunctionUtils} to run some of the type inference for legacy functions in
 * Table API.
 */
@Internal
@Deprecated
public class LegacyUserDefinedFunctionInference {

    public static InputTypeStrategy getInputTypeStrategy(ImperativeAggregateFunction<?, ?> func) {
        return new InputTypeStrategy() {
            @Override
            public ArgumentCount getArgumentCount() {
                return ConstantArgumentCount.any();
            }

            @Override
            public Optional<List<DataType>> inferInputTypes(
                    CallContext callContext, boolean throwOnFailure) {
                final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
                final DataType accType = getAccumulatorType(func);
                final LogicalType[] input =
                        Stream.concat(Stream.of(accType), argumentDataTypes.stream())
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new);

                final Optional<Optional<Method>> foundMethod =
                        Stream.of(
                                        logicalTypesToInternalClasses(input),
                                        logicalTypesToExternalClasses(input))
                                .map(
                                        signature ->
                                                getUserDefinedMethod(
                                                        func,
                                                        "accumulate",
                                                        signature,
                                                        input,
                                                        cls ->
                                                                Stream.concat(
                                                                                Stream.of(accType),
                                                                                Arrays.stream(cls)
                                                                                        .skip(1)
                                                                                        .map(
                                                                                                c ->
                                                                                                        fromLegacyInfoToDataType(
                                                                                                                TypeExtractor
                                                                                                                        .createTypeInfo(
                                                                                                                                c))))
                                                                        .toArray(DataType[]::new)))
                                .filter(Optional::isPresent)
                                .findAny();

                if (foundMethod.isPresent()) {
                    return Optional.of(argumentDataTypes);
                } else {
                    return callContext.fail(throwOnFailure, "");
                }
            }

            @Override
            public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
                return getSignatures(func, "accumulate");
            }
        };
    }

    public static InputTypeStrategy getInputTypeStrategy(TableFunction<?> func) {
        return new InputTypeStrategy() {
            @Override
            public ArgumentCount getArgumentCount() {
                return ConstantArgumentCount.any();
            }

            @Override
            public Optional<List<DataType>> inferInputTypes(
                    CallContext callContext, boolean throwOnFailure) {
                final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
                final LogicalType[] input =
                        argumentDataTypes.stream()
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new);

                final Optional<Method> foundMethod =
                        getUserDefinedMethod(
                                func,
                                "eval",
                                argumentDataTypes.stream()
                                        .map(DataType::getConversionClass)
                                        .toArray(Class<?>[]::new),
                                input,
                                cls ->
                                        Arrays.stream(cls)
                                                .map(
                                                        c ->
                                                                fromLegacyInfoToDataType(
                                                                        TypeExtractor
                                                                                .createTypeInfo(c)))
                                                .toArray(DataType[]::new));

                if (foundMethod.isPresent()) {
                    return Optional.of(argumentDataTypes);
                } else {
                    return callContext.fail(
                            throwOnFailure, "Given parameters do not match any signature.");
                }
            }

            @Override
            public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
                return getSignatures(func, "eval");
            }
        };
    }

    public static TypeStrategy getOutputTypeStrategy(ScalarFunction func) {
        return callContext -> {
            final LogicalType[] params =
                    callContext.getArgumentDataTypes().stream()
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            Optional<Class<?>[]> evalParams = getEvalMethodSignature(func, params);
            if (!evalParams.isPresent()) {
                return Optional.empty();
            }

            final TypeInformation<?> userDefinedTypeInfo = func.getResultType(evalParams.get());
            if (userDefinedTypeInfo != null) {
                return Optional.of(fromLegacyInfoToDataType(userDefinedTypeInfo));
            } else {
                final Optional<Method> eval =
                        getUserDefinedMethod(
                                func,
                                "eval",
                                logicalTypesToExternalClasses(params),
                                params,
                                (paraClasses) ->
                                        Arrays.stream(func.getParameterTypes(paraClasses))
                                                .map(TypeConversions::fromLegacyInfoToDataType)
                                                .toArray(DataType[]::new));
                return eval.flatMap(m -> TypeConversions.fromClassToDataType(m.getReturnType()));
            }
        };
    }

    public static InputTypeStrategy getInputTypeStrategy(ScalarFunction func) {
        return new InputTypeStrategy() {
            @Override
            public ArgumentCount getArgumentCount() {
                return ConstantArgumentCount.any();
            }

            @Override
            public Optional<List<DataType>> inferInputTypes(
                    CallContext callContext, boolean throwOnFailure) {
                final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
                final LogicalType[] input =
                        argumentDataTypes.stream()
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new);

                final Optional<Method> foundMethod =
                        getUserDefinedMethod(
                                func,
                                "eval",
                                logicalTypesToExternalClasses(input),
                                input,
                                (paraClasses) ->
                                        Arrays.stream(func.getParameterTypes(paraClasses))
                                                .map(TypeConversions::fromLegacyInfoToDataType)
                                                .toArray(DataType[]::new));

                if (foundMethod.isPresent()) {
                    return Optional.of(argumentDataTypes);
                } else {
                    return callContext.fail(
                            throwOnFailure, "Given parameters do not match any signature.");
                }
            }

            @Override
            public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
                return getSignatures(func, "eval");
            }
        };
    }

    private static Optional<Class<?>[]> getEvalMethodSignature(
            ScalarFunction func, LogicalType[] expectedTypes) {
        return getUserDefinedMethod(
                        func,
                        "eval",
                        logicalTypesToExternalClasses(expectedTypes),
                        expectedTypes,
                        (paraClasses) ->
                                Arrays.stream(func.getParameterTypes(paraClasses))
                                        .map(TypeConversions::fromLegacyInfoToDataType)
                                        .toArray(DataType[]::new))
                .map(
                        m ->
                                getParamClassesConsiderVarArgs(
                                        m.isVarArgs(),
                                        m.getParameterTypes(),
                                        expectedTypes.length));
    }

    private static Class<?>[] getParamClassesConsiderVarArgs(
            boolean isVarArgs, Class<?>[] matchingSignature, int expectedLength) {
        return IntStream.range(0, expectedLength)
                .mapToObj(
                        i -> {
                            if (i < matchingSignature.length - 1) {
                                return matchingSignature[i];
                            } else if (isVarArgs) {
                                return matchingSignature[matchingSignature.length - 1]
                                        .getComponentType();
                            } else {
                                // last argument is not an array type
                                return matchingSignature[matchingSignature.length - 1];
                            }
                        })
                .toArray(Class<?>[]::new);
    }

    private static DataType getAccumulatorType(ImperativeAggregateFunction<?, ?> func) {
        final TypeInformation<?> accType = func.getAccumulatorType();
        if (accType != null) {
            return fromLegacyInfoToDataType(accType);
        } else {
            try {
                return fromLegacyInfoToDataType(
                        TypeExtractor.createTypeInfo(
                                func, ImperativeAggregateFunction.class, func.getClass(), 1));
            } catch (InvalidTypesException ite) {
                throw new TableException(
                        String.format(
                                "Cannot infer generic type of %s}. You can override"
                                        + " ImperativeAggregateFunction.getAccumulatorType()"
                                        + " to specify the type.",
                                func.getClass()),
                        ite);
            }
        }
    }

    private static Class<?>[] logicalTypesToExternalClasses(LogicalType[] types) {
        return Arrays.stream(types)
                .map(
                        t -> {
                            if (t == null) {
                                return null;
                            } else {
                                return TypeConversions.fromLogicalToDataType(t)
                                        .getConversionClass();
                            }
                        })
                .toArray(Class<?>[]::new);
    }

    private static Class<?>[] logicalTypesToInternalClasses(LogicalType[] types) {
        return Arrays.stream(types)
                .map(
                        t -> {
                            if (t == null) {
                                return null;
                            } else {
                                return toInternalConversionClass(t);
                            }
                        })
                .toArray(Class<?>[]::new);
    }

    private static boolean parameterClassEquals(Class<?> candidate, Class<?> expected) {
        return candidate == null
                || candidate == expected
                || expected == Object.class
                || candidate == Object.class
                || // Special case when we don't know the type
                expected.isPrimitive() && Primitives.wrap(expected) == candidate
                || candidate == Date.class && (expected == Integer.class || expected == int.class)
                || candidate == Time.class && (expected == Integer.class || expected == int.class)
                || candidate == StringData.class && expected == String.class
                || candidate == String.class && expected == StringData.class
                || candidate == TimestampData.class && expected == LocalDateTime.class
                || candidate == Timestamp.class && expected == TimestampData.class
                || candidate == TimestampData.class && expected == Timestamp.class
                || candidate == LocalDateTime.class && expected == TimestampData.class
                || candidate == TimestampData.class && expected == Instant.class
                || candidate == Instant.class && expected == TimestampData.class
                || RowData.class.isAssignableFrom(candidate) && expected == Row.class
                || candidate == Row.class && RowData.class.isAssignableFrom(expected)
                || RowData.class.isAssignableFrom(candidate) && expected == RowData.class
                || candidate == RowData.class && RowData.class.isAssignableFrom(expected)
                || candidate == DecimalData.class && expected == BigDecimal.class
                || candidate == BigDecimal.class && expected == DecimalData.class
                || (candidate.isArray()
                        && expected.isArray()
                        && candidate.getComponentType() != null
                        && expected.getComponentType() == Object.class);
    }

    private static boolean parameterDataTypeEquals(LogicalType internal, DataType parameterType) {
        if (internal.is(LogicalTypeRoot.RAW)
                && parameterType.getLogicalType().is(LogicalTypeRoot.RAW)) {
            return TypeConversions.fromLogicalToDataType(internal).getConversionClass()
                    == parameterType.getConversionClass();
        } else {
            // There is a special equal to GenericType. We need rewrite type extract to RowData
            // etc...
            return parameterType.getLogicalType() == internal
                    || toInternalConversionClass(internal)
                            == toInternalConversionClass(parameterType.getLogicalType());
        }
    }

    private static Optional<Method> getUserDefinedMethod(
            UserDefinedFunction function,
            String methodName,
            Class<?>[] methodSignature,
            LogicalType[] internalTypes,
            Function<Class<?>[], DataType[]> parameterTypesFunc) {
        final List<Method> methods = checkAndExtractMethods(function, methodName);

        final List<Method> filteredMethods =
                methods.stream()
                        .filter(
                                cur -> {
                                    final Class<?>[] parameterTypes = cur.getParameterTypes();
                                    final DataType[] parameterDataTypes =
                                            parameterTypesFunc.apply(parameterTypes);
                                    if (cur.isVarArgs()) {
                                        return varArgsMethodMatch(
                                                methodSignature,
                                                internalTypes,
                                                parameterTypes,
                                                parameterDataTypes);

                                    } else {
                                        return methodMatch(
                                                methodSignature,
                                                internalTypes,
                                                parameterTypes,
                                                parameterDataTypes);
                                    }
                                })
                        .collect(Collectors.toList());

        // if there is a fixed method, compiler will call this method preferentially

        final List<Method> found =
                filteredMethods.stream()
                        .sorted(Comparator.comparing(Method::isVarArgs, Boolean::compareTo))
                        .filter(cur -> !Modifier.isVolatile(cur.getModifiers()))
                        .collect(Collectors.toList());

        final int foundCount = found.size();
        if (foundCount == 1) {
            return Optional.of(found.get(0));
        } else if (foundCount > 1) {
            if (Arrays.asList(methodSignature).contains(Object.class)) {
                return Optional.of(found.get(0));
            }

            final List<Method> nonObjectParameterMethods =
                    found.stream()
                            .filter(
                                    m ->
                                            !Arrays.asList(m.getParameterTypes())
                                                    .contains(Object.class))
                            .collect(Collectors.toList());

            if (nonObjectParameterMethods.size() == 1) {
                return Optional.of(nonObjectParameterMethods.get(0));
            }

            throw new ValidationException(
                    String.format(
                            "Found multiple '%s' methods which match the signature.", methodName));
        }

        return Optional.empty();
    }

    private static boolean methodMatch(
            Class<?>[] methodSignature,
            LogicalType[] internalTypes,
            Class<?>[] parameterTypes,
            DataType[] parameterDataTypes) {
        // match parameters of signature to actual parameters
        return methodSignature.length == parameterTypes.length
                && IntStream.range(0, parameterTypes.length)
                        .allMatch(
                                i ->
                                        parameterClassEquals(methodSignature[i], parameterTypes[i])
                                                || parameterDataTypeEquals(
                                                        internalTypes[i], parameterDataTypes[i]));
    }

    private static boolean varArgsMethodMatch(
            Class<?>[] methodSignature,
            LogicalType[] internalTypes,
            Class<?>[] parameterTypes,
            DataType[] parameterDataTypes) {
        if (methodSignature.length == 0 && parameterTypes.length == 1) {
            return true;
        } else {
            return IntStream.range(0, methodSignature.length)
                    .allMatch(
                            i -> {
                                if (i < parameterTypes.length - 1) {
                                    return parameterClassEquals(
                                                    methodSignature[i], parameterTypes[i])
                                            || parameterDataTypeEquals(
                                                    internalTypes[i], parameterDataTypes[i]);
                                } else {
                                    return parameterClassEquals(
                                                    methodSignature[i],
                                                    parameterTypes[parameterTypes.length - 1]
                                                            .getComponentType())
                                            || parameterDataTypeEquals(
                                                    internalTypes[i], parameterDataTypes[i]);
                                }
                            });
        }
    }

    private static List<Method> checkAndExtractMethods(
            UserDefinedFunction function, String methodName) {
        final List<Method> methods =
                Arrays.stream(function.getClass().getMethods())
                        .filter(
                                m -> {
                                    final int modifiers = m.getModifiers();
                                    return m.getName().equals(methodName)
                                            && Modifier.isPublic(modifiers)
                                            && !Modifier.isAbstract(modifiers)
                                            && !(function instanceof TableFunction
                                                    && Modifier.isStatic(modifiers));
                                })
                        .collect(Collectors.toList());

        if (methods.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Function class '%s' does not implement at least "
                                    + "one method named '%s' which is public, not abstract and "
                                    + "(in case of table functions) not static.",
                            function.getClass().getCanonicalName(), methodName));
        }

        return methods;
    }

    private static List<Signature> getSignatures(UserDefinedFunction func, String methodName) {
        return checkAndExtractMethods(func, methodName).stream()
                .map(Method::getParameterTypes)
                .map(
                        sig ->
                                Signature.of(
                                        Arrays.stream(sig)
                                                .map(s -> Signature.Argument.of(s.getSimpleName()))
                                                .collect(Collectors.toList())))
                .collect(Collectors.toList());
    }

    private LegacyUserDefinedFunctionInference() {}
}
