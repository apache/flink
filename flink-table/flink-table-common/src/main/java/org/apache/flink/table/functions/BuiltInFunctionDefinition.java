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

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Definition of a built-in function. It enables unique identification across different modules by
 * reference equality.
 *
 * <p>Compared to regular {@link FunctionDefinition}, built-in functions have a default name. This
 * default name is used to look up the function in a catalog during resolution. However, note that
 * every built-in function is actually fully qualified by a name and a version. Internal functions
 * are required to have a name that includes the version (e.g. {@code $REPLICATE_ROWS$1}). The most
 * recent version of a regular function is picked during a lookup if a call does not reference an
 * internal function.
 *
 * <p>Equality is defined by reference equality.
 */
@Internal
public final class BuiltInFunctionDefinition implements SpecializedFunction {

    private final String name;

    private final @Nullable Integer version;

    private final FunctionKind kind;

    private final TypeInference typeInference;

    private final boolean isDeterministic;

    private final boolean isRuntimeProvided;

    private final @Nullable String runtimeClass;

    private final boolean isInternal;

    private BuiltInFunctionDefinition(
            String name,
            int version,
            FunctionKind kind,
            TypeInference typeInference,
            boolean isDeterministic,
            boolean isRuntimeProvided,
            String runtimeClass,
            boolean isInternal) {
        this.name = checkNotNull(name, "Name must not be null.");
        this.version = isInternal ? null : version;
        this.kind = checkNotNull(kind, "Kind must not be null.");
        this.typeInference = checkNotNull(typeInference, "Type inference must not be null.");
        this.isDeterministic = isDeterministic;
        this.isRuntimeProvided = isRuntimeProvided;
        this.runtimeClass = runtimeClass;
        this.isInternal = isInternal;
        validateFunction(this.name, this.version, this.isInternal);
    }

    /** Builder for configuring and creating instances of {@link BuiltInFunctionDefinition}. */
    public static BuiltInFunctionDefinition.Builder newBuilder() {
        return new BuiltInFunctionDefinition.Builder();
    }

    public String getName() {
        return name;
    }

    public Optional<Integer> getVersion() {
        return Optional.ofNullable(version);
    }

    public Optional<String> getRuntimeClass() {
        return Optional.ofNullable(runtimeClass);
    }

    public boolean hasRuntimeImplementation() {
        return isRuntimeProvided || runtimeClass != null;
    }

    public boolean isInternal() {
        return isInternal;
    }

    public String getQualifiedName() {
        if (isInternal) {
            return name;
        }
        assert version != null;
        return qualifyFunctionName(name, version);
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
            // In case another level of specialization is required
            if (SpecializedFunction.class.isAssignableFrom(udfClass)) {
                final SpecializedFunction specializedFunction =
                        (SpecializedFunction) udfClass.newInstance();
                return specializedFunction.specialize(context);
            } else {
                final Constructor<?> udfConstructor =
                        udfClass.getConstructor(SpecializedContext.class);
                return (UserDefinedFunction) udfConstructor.newInstance(context);
            }
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
    // Shared with BuiltInSqlOperator and BuiltInSqlFunction in planner
    // --------------------------------------------------------------------------------------------

    // note that for SQL operators the name can contain spaces and dollar signs
    private static final Pattern INTERNAL_NAME_PATTERN =
            Pattern.compile("\\$[A-Z0-9_ $]+\\$[1-9][0-9]*");

    private static final String INTERNAL_NAME_FORMAT = "$%s$%s";

    public static final int DEFAULT_VERSION = 1;

    public static void validateFunction(
            String name, @Nullable Integer version, boolean isInternal) {
        if (isInternal) {
            checkArgument(
                    INTERNAL_NAME_PATTERN.matcher(name).matches(),
                    "Internal function '%s' does not adhere to the naming convention: %s",
                    name,
                    INTERNAL_NAME_PATTERN);
            checkArgument(version == null, "Internal function must not define a version.");
        } else {
            checkArgument(
                    version == null || version >= DEFAULT_VERSION, "Version must be at least 1.");
        }
    }

    public static String qualifyFunctionName(String name, int version) {
        return String.format(INTERNAL_NAME_FORMAT, name.toUpperCase(Locale.ROOT), version);
    }

    // --------------------------------------------------------------------------------------------
    // Builder
    // --------------------------------------------------------------------------------------------

    /** Builder for fluent definition of built-in functions. */
    @Internal
    public static final class Builder {

        private String name;

        private int version = DEFAULT_VERSION;

        private FunctionKind kind;

        private final TypeInference.Builder typeInferenceBuilder = TypeInference.newBuilder();

        private boolean isDeterministic = true;

        private boolean isRuntimeProvided = false;

        private String runtimeClass;

        private boolean isInternal = false;

        public Builder() {
            // default constructor to allow a fluent definition
        }

        /**
         * Specifies a name that uniquely identifies a built-in function.
         *
         * <p>Please adhere to the following naming convention:
         *
         * <ul>
         *   <li>Use upper case and separate words with underscore.
         *   <li>Depending on the importance of the function, the underscore is sometimes omitted
         *       e.g. for {@code IFNULL} or {@code TYPEOF} but not for {@code TO_TIMESTAMP_LTZ}.
         *   <li>Internal functions must start with $ and include a version starting from 1. The
         *       following format is enforced: {@code $NAME$VERSION} such as {@code
         *       $REPLICATE_ROWS$1}.
         * </ul>
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Specifies a version that will be persisted in the plan together with the function's name.
         * The default version is 1 for non-internal functions.
         *
         * <p>Note: Internal functions don't need to specify a version as we enforce a unique name
         * that includes a version (see {@link #name(String)}).
         */
        public Builder version(int version) {
            this.version = version;
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

        /**
         * Specifies that this {@link BuiltInFunctionDefinition} is meant for internal purposes only
         * and should not be exposed when listing functions.
         */
        public Builder internal() {
            this.isInternal = true;
            return this;
        }

        public BuiltInFunctionDefinition build() {
            return new BuiltInFunctionDefinition(
                    name,
                    version,
                    kind,
                    typeInferenceBuilder.build(),
                    isDeterministic,
                    isRuntimeProvided,
                    runtimeClass,
                    isInternal);
        }
    }
}
