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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;

/** Template of a function signature with argument types and argument names. */
@Internal
final class FunctionSignatureTemplate {

    final List<FunctionArgumentTemplate> argumentTemplates;

    final boolean isVarArgs;

    final EnumSet<StaticArgumentTrait>[] argumentTraits;

    final String[] argumentNames;

    final boolean[] argumentOptionals;

    private FunctionSignatureTemplate(
            List<FunctionArgumentTemplate> argumentTemplates,
            boolean isVarArgs,
            EnumSet<StaticArgumentTrait>[] argumentTraits,
            @Nullable String[] argumentNames,
            boolean[] argumentOptionals) {
        this.argumentTemplates = argumentTemplates;
        this.isVarArgs = isVarArgs;
        this.argumentTraits = argumentTraits;
        this.argumentNames =
                argumentNames == null
                        ? IntStream.range(0, argumentTemplates.size())
                                .mapToObj(pos -> "arg" + pos)
                                .toArray(String[]::new)
                        : argumentNames;
        this.argumentOptionals = argumentOptionals;
    }

    static FunctionSignatureTemplate of(
            List<FunctionArgumentTemplate> argumentTemplates,
            boolean isVarArgs,
            EnumSet<StaticArgumentTrait>[] argumentTraits,
            @Nullable String[] argumentNames,
            boolean[] argumentOptionals) {
        if (argumentNames != null && argumentNames.length != argumentTemplates.size()) {
            throw extractionError(
                    "Mismatch between number of argument names '%s' and argument types '%s'.",
                    argumentNames.length, argumentTemplates.size());
        }
        if (argumentNames != null
                && argumentNames.length != Arrays.stream(argumentNames).distinct().count()) {
            throw extractionError(
                    "Argument name conflict, there are at least two argument names that are the same.");
        }
        if (argumentOptionals != null && argumentOptionals.length != argumentTemplates.size()) {
            throw extractionError(
                    "Mismatch between number of argument optionals '%s' and argument types '%s'.",
                    argumentOptionals.length, argumentTemplates.size());
        }
        if (argumentOptionals != null) {
            for (int i = 0; i < argumentTemplates.size(); i++) {
                DataType dataType = argumentTemplates.get(i).toDataType();
                if (dataType != null
                        && !dataType.getLogicalType().isNullable()
                        && argumentOptionals[i]) {
                    throw extractionError(
                            "Argument at position %s is optional but its type doesn't accept null value.",
                            i);
                }
            }
        }
        return new FunctionSignatureTemplate(
                argumentTemplates, isVarArgs, argumentTraits, argumentNames, argumentOptionals);
    }

    /**
     * Converts the given signature into a list of static arguments if the signature allows it. E.g.
     * no var-args and all arguments are named.
     */
    @Nullable
    List<StaticArgument> toStaticArguments() {
        if (isVarArgs || argumentNames == null) {
            return null;
        }
        final List<StaticArgument> arguments =
                IntStream.range(0, argumentTemplates.size())
                        .mapToObj(
                                pos -> {
                                    final String name = argumentNames[pos];
                                    final boolean isOptional = argumentOptionals[pos];
                                    final FunctionArgumentTemplate template =
                                            argumentTemplates.get(pos);
                                    final EnumSet<StaticArgumentTrait> traits = argumentTraits[pos];
                                    if (traits.contains(StaticArgumentTrait.TABLE_AS_ROW)
                                            || traits.contains(StaticArgumentTrait.TABLE_AS_SET)) {
                                        return createTableArgument(
                                                name,
                                                isOptional,
                                                traits,
                                                template.toDataType(),
                                                template.toConversionClass());
                                    } else if (traits.contains(StaticArgumentTrait.SCALAR)) {
                                        return createScalarArgument(
                                                name, isOptional, template.toDataType());
                                    } else {
                                        return null;
                                    }
                                })
                        .collect(Collectors.toList());
        if (arguments.contains(null)) {
            return null;
        }
        return arguments;
    }

    private static @Nullable StaticArgument createTableArgument(
            String name,
            boolean isOptional,
            EnumSet<StaticArgumentTrait> traits,
            @Nullable DataType dataType,
            @Nullable Class<?> conversionClass) {
        if (dataType != null) {
            return StaticArgument.table(name, dataType, isOptional, traits);
        }
        if (conversionClass != null) {
            return StaticArgument.table(name, conversionClass, isOptional, traits);
        }
        return null;
    }

    private static @Nullable StaticArgument createScalarArgument(
            String name, boolean isOptional, @Nullable DataType dataType) {
        if (dataType != null) {
            return StaticArgument.scalar(name, dataType, isOptional);
        }
        return null;
    }

    InputTypeStrategy toInputTypeStrategy() {
        final ArgumentTypeStrategy[] argumentStrategies =
                argumentTemplates.stream()
                        .map(FunctionArgumentTemplate::toArgumentTypeStrategy)
                        .toArray(ArgumentTypeStrategy[]::new);

        final InputTypeStrategy strategy;
        if (isVarArgs) {
            if (argumentNames == null) {
                strategy = InputTypeStrategies.varyingSequence(argumentStrategies);
            } else {
                strategy = InputTypeStrategies.varyingSequence(argumentNames, argumentStrategies);
            }
        } else {
            if (argumentNames == null) {
                strategy = InputTypeStrategies.sequence(argumentStrategies);
            } else {
                strategy = InputTypeStrategies.sequence(argumentNames, argumentStrategies);
            }
        }
        return strategy;
    }

    List<Class<?>> toClassList() {
        return IntStream.range(0, argumentTemplates.size())
                .mapToObj(
                        i -> {
                            final Class<?> clazz = argumentTemplates.get(i).toConversionClass();
                            if (i == argumentTemplates.size() - 1 && isVarArgs) {
                                return Array.newInstance(clazz, 0).getClass();
                            }
                            return clazz;
                        })
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionSignatureTemplate that = (FunctionSignatureTemplate) o;
        // argument names are not part of equality
        return isVarArgs == that.isVarArgs && argumentTemplates.equals(that.argumentTemplates);
    }

    @Override
    public int hashCode() {
        // argument names are not part of equality
        return Objects.hash(argumentTemplates, isVarArgs);
    }
}
