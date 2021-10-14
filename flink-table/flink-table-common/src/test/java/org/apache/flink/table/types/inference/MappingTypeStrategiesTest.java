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

package org.apache.flink.table.types.inference;

import org.apache.flink.table.api.DataTypes;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.types.inference.TypeStrategies.explicit;

/** Tests for {@link TypeStrategies#mapping(Map)}. */
public class MappingTypeStrategiesTest extends TypeStrategiesTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                // (INT, BOOLEAN) -> STRING
                TestSpec.forStrategy(createMappingTypeStrategy())
                        .inputTypes(DataTypes.INT(), DataTypes.BOOLEAN())
                        .expectDataType(DataTypes.STRING()),

                // (INT, STRING) -> BOOLEAN
                TestSpec.forStrategy(createMappingTypeStrategy())
                        .inputTypes(DataTypes.INT(), DataTypes.STRING())
                        .expectDataType(DataTypes.BOOLEAN().bridgedTo(boolean.class)),

                // (INT, CHAR(10)) -> BOOLEAN
                // but avoiding casts (mapping actually expects STRING)
                TestSpec.forStrategy(createMappingTypeStrategy())
                        .inputTypes(DataTypes.INT(), DataTypes.CHAR(10))
                        .expectDataType(DataTypes.BOOLEAN().bridgedTo(boolean.class)),

                // invalid mapping strategy
                TestSpec.forStrategy(createMappingTypeStrategy())
                        .inputTypes(DataTypes.INT(), DataTypes.INT())
                        .expectErrorMessage(
                                "Could not infer an output type for the given arguments."));
    }

    private static TypeStrategy createMappingTypeStrategy() {
        final Map<InputTypeStrategy, TypeStrategy> mappings = new HashMap<>();
        mappings.put(
                InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.INT()),
                        InputTypeStrategies.explicit(DataTypes.STRING())),
                explicit(DataTypes.BOOLEAN().bridgedTo(boolean.class)));
        mappings.put(
                InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.INT()),
                        InputTypeStrategies.explicit(DataTypes.BOOLEAN())),
                explicit(DataTypes.STRING()));
        return TypeStrategies.mapping(mappings);
    }
}
