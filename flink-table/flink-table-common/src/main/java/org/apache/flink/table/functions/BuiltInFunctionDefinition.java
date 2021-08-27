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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Optional;

/**
 * Definition of a built-in function. It enables unique identification across different modules by
 * reference equality.
 *
 * <p>Compared to regular {@link FunctionDefinition}, built-in functions have a default name. This
 * default name is used to lookup the function in a catalog during resolution.
 *
 * <p>Equality is defined by reference equality.
 */
@Internal
public final class BuiltInFunctionDefinition implements SpecializedFunction {

    private final String name;

    private final FunctionKind kind;

    private final TypeInference typeInference;

    private final boolean isDeterministic;

    private final boolean isRuntimeProvided;

    private final @Nullable String runtimeClass;

    private BuiltInFunctionDefinition(
            String name,
            FunctionKind kind,
            TypeInference typeInference,
            boolean isDeterministic,
            boolean isRuntimeProvided,
            String runtimeClass) {
        this.name = Preconditions.checkNotNull(name, "Name must not be null.");
        this.kind = Preconditions.checkNotNull(kind, "Kind must not be null.");
        this.typeInference =
                Preconditions.checkNotNull(typeInference, "Type inference must not be null.");
        this.isDeterministic = isDeterministic;
        this.isRuntimeProvided = isRuntimeProvided;
        this.runtimeClass = runtimeClass;
    }

    /** Builder for configuring and creating instances of {@link BuiltInFunctionDefinition}. */
    public static BuiltInFunctionDefinition.Builder newBuilder() {
        return new BuiltInFunctionDefinition.Builder();
    }

    public String getName() {
        return name;
    }

    public Optional<String> getRuntimeClass() {
        return Optional.ofNullable(runtimeClass);
    }

    public boolean hasRuntimeImplementation() {
        return isRuntimeProvided || runtimeClass != null;
    }

    @Override
    public UserDefinedFunction specialize(SpecializedContext context) {
        if (runtimeClass == null) {
            throw new TableException(
                    String.format(
                            "Could not find a runtime implementation for built-in function '%s'. "
                                    + "The planner should have provided an implementation.",
                            name));
        }
        try {
            final Class<?> udfClass =
                    Class.forName(runtimeClass, true, context.getBuiltInClassLoader());
            final Constructor<?> udfConstructor = udfClass.getConstructor(SpecializedContext.class);
            final UserDefinedFunction udf =
                    (UserDefinedFunction) udfConstructor.newInstance(context);
            // in case another level of specialization is required
            if (udf instanceof SpecializedFunction) {
                return ((SpecializedFunction) udf).specialize(context);
            }
            return udf;
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Could not instantiate a runtime implementation for built-in function '%s'.",
                            name),
                    e);
        }
    }

    @Override
    public FunctionKind getKind() {
        return kind;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return typeInference;
    }

    @Override
    public boolean isDeterministic() {
        return isDeterministic;
    }

    @Override
    public String toString() {
        return name;
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for fluent definition of built-in functions. */
    public static final class Builder {

        private String name;

        private FunctionKind kind;

        private final TypeInference.Builder typeInferenceBuilder = TypeInference.newBuilder();

        private boolean isDeterministic = true;

        private boolean isRuntimeProvided = false;

        private String runtimeClass;

        public Builder() {
            // default constructor to allow a fluent definition
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder kind(FunctionKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder namedArguments(String... argumentNames) {
            this.typeInferenceBuilder.namedArguments(Arrays.asList(argumentNames));
            return this;
        }

        public Builder typedArguments(DataType... argumentTypes) {
            this.typeInferenceBuilder.typedArguments(Arrays.asList(argumentTypes));
            return this;
        }

        public Builder inputTypeStrategy(InputTypeStrategy inputTypeStrategy) {
            this.typeInferenceBuilder.inputTypeStrategy(inputTypeStrategy);
            return this;
        }

        public Builder outputTypeStrategy(TypeStrategy outputTypeStrategy) {
            this.typeInferenceBuilder.outputTypeStrategy(outputTypeStrategy);
            return this;
        }

        public Builder notDeterministic() {
            this.isDeterministic = false;
            return this;
        }

        /**
         * Specifies that this {@link BuiltInFunctionDefinition} is implemented during code
         * generation.
         */
        public Builder runtimeProvided() {
            this.isRuntimeProvided = true;
            return this;
        }

        /** Specifies the runtime class implementing this {@link BuiltInFunctionDefinition}. */
        public Builder runtimeClass(String runtimeClass) {
            this.runtimeClass = runtimeClass;
            return this;
        }

        /**
         * Specifies that this {@link BuiltInFunctionDefinition} will be mapped to a Calcite
         * function.
         */
        public Builder runtimeDeferred() {
            // This method is just a marker method for clarity. It is equivalent to calling
            // neither {@link #runtimeProvided} nor {@link #runtimeClass}.
            return this;
        }

        public BuiltInFunctionDefinition build() {
            return new BuiltInFunctionDefinition(
                    name,
                    kind,
                    typeInferenceBuilder.build(),
                    isDeterministic,
                    isRuntimeProvided,
                    runtimeClass);
        }
    }
}
