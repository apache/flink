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

package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;

/** Planner placeholder for a Python user-defined process table function. */
@Internal
public class PythonProcessTableFunction extends ProcessTableFunction<Row>
        implements PythonFunction {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final byte[] serializedProcessTableFunction;
    private final String[] argumentNames;
    private final DataType[] argumentDataTypes;
    private final boolean[] tableArguments;
    private final String[] argumentTraits;
    private final String[] stateNames;
    private final DataType[] stateDataTypes;
    private final Duration[] stateTimeToLive;
    private final DataType resultType;
    private final boolean deterministic;
    private final boolean hasOnTimer;
    private final PythonEnv pythonEnv;

    public PythonProcessTableFunction(
            String name,
            byte[] serializedProcessTableFunction,
            String[] argumentNames,
            DataType[] argumentDataTypes,
            boolean[] tableArguments,
            String[] argumentTraits,
            String[] stateNames,
            DataType[] stateDataTypes,
            Duration[] stateTimeToLive,
            DataType resultType,
            boolean deterministic,
            boolean hasOnTimer,
            PythonEnv pythonEnv) {
        this.name = Preconditions.checkNotNull(name);
        this.serializedProcessTableFunction =
                Preconditions.checkNotNull(serializedProcessTableFunction);
        this.argumentNames = Preconditions.checkNotNull(argumentNames);
        this.argumentDataTypes = Preconditions.checkNotNull(argumentDataTypes);
        this.tableArguments = Preconditions.checkNotNull(tableArguments);
        this.argumentTraits = Preconditions.checkNotNull(argumentTraits);
        this.stateNames = Preconditions.checkNotNull(stateNames);
        this.stateDataTypes = Preconditions.checkNotNull(stateDataTypes);
        this.stateTimeToLive = Preconditions.checkNotNull(stateTimeToLive);
        this.resultType = Preconditions.checkNotNull(resultType);
        this.deterministic = deterministic;
        this.hasOnTimer = hasOnTimer;
        this.pythonEnv = Preconditions.checkNotNull(pythonEnv);
        validateMetadata();
    }

    public void eval(Object... args) {
        throw new UnsupportedOperationException(
                "This method is a placeholder and should not be called.");
    }

    public void onTimer(Object... states) {
        throw new UnsupportedOperationException(
                "This method is a placeholder and should not be called.");
    }

    @Override
    public byte[] getSerializedPythonFunction() {
        return serializedProcessTableFunction;
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pythonEnv;
    }

    @Override
    public boolean isDeterministic() {
        return deterministic;
    }

    public boolean hasOnTimer() {
        return hasOnTimer;
    }

    public String[] getArgumentNames() {
        return argumentNames;
    }

    public DataType[] getArgumentDataTypes() {
        return argumentDataTypes;
    }

    public boolean[] getTableArguments() {
        return tableArguments;
    }

    public String[] getArgumentTraits() {
        return argumentTraits;
    }

    public String[] getStateNames() {
        return stateNames;
    }

    public DataType[] getStateDataTypes() {
        return stateDataTypes;
    }

    public Duration[] getStateTimeToLive() {
        return stateTimeToLive;
    }

    public DataType getResultType() {
        return resultType;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        final List<StaticArgument> staticArguments = new ArrayList<>();
        for (int i = 0; i < argumentNames.length; i++) {
            if (tableArguments[i]) {
                final EnumSet<StaticArgumentTrait> traits = parseTraits(argumentTraits[i]);
                final @Nullable DataType dataType = argumentDataTypes[i];
                if (dataType == null) {
                    staticArguments.add(
                            StaticArgument.table(argumentNames[i], Row.class, false, traits));
                } else {
                    staticArguments.add(
                            StaticArgument.table(argumentNames[i], dataType, false, traits));
                }
            } else {
                staticArguments.add(
                        StaticArgument.scalar(argumentNames[i], argumentDataTypes[i], false));
            }
        }

        final LinkedHashMap<String, StateTypeStrategy> stateStrategies = new LinkedHashMap<>();
        for (int i = 0; i < stateNames.length; i++) {
            stateStrategies.put(
                    stateNames[i],
                    StateTypeStrategy.of(
                            TypeStrategies.explicit(stateDataTypes[i]), stateTimeToLive[i]));
        }

        return TypeInference.newBuilder()
                .staticArguments(staticArguments)
                .stateTypeStrategies(stateStrategies)
                .outputTypeStrategy(TypeStrategies.explicit(resultType))
                .build();
    }

    @Override
    public String toString() {
        return name;
    }

    private void validateMetadata() {
        Preconditions.checkArgument(
                argumentNames.length == argumentDataTypes.length
                        && argumentNames.length == tableArguments.length
                        && argumentNames.length == argumentTraits.length,
                "Argument metadata must have equal lengths.");
        Preconditions.checkArgument(
                stateNames.length == stateDataTypes.length
                        && stateNames.length == stateTimeToLive.length,
                "State metadata must have equal lengths.");
    }

    private static EnumSet<StaticArgumentTrait> parseTraits(String serializedTraits) {
        final EnumSet<StaticArgumentTrait> traits = EnumSet.noneOf(StaticArgumentTrait.class);
        if (serializedTraits.isEmpty()) {
            return traits;
        }
        for (String trait : serializedTraits.split(",")) {
            traits.add(StaticArgumentTrait.valueOf(trait));
        }
        return traits;
    }
}
