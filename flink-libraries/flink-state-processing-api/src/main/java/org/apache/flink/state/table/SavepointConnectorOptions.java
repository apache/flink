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

package org.apache.flink.state.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import static org.apache.flink.configuration.description.TextElement.code;

/** Options for the state connector. */
@PublicEvolving
public class SavepointConnectorOptions {

    public static final String FIELDS = "fields";
    public static final String STATE_NAME = "state-name";
    public static final String STATE_TYPE = "state-type";
    public static final String MAP_KEY_FORMAT = "map-key-format";
    public static final String VALUE_FORMAT = "value-format";

    /** Value state types. */
    public enum StateType {
        VALUE,
        LIST,
        MAP
    }

    // --------------------------------------------------------------------------------------------
    // Common options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> STATE_BACKEND_TYPE =
            ConfigOptions.key("state.backend.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("The state backend to be used to read state.")
                                    .linebreak()
                                    .text(
                                            "The implementation can be specified either via their shortcut "
                                                    + " name, or via the class name of a %s. "
                                                    + "If a factory is specified it is instantiated via its "
                                                    + "zero argument constructor and its %s "
                                                    + "method is called.",
                                            TextElement.code("StateBackendFactory"),
                                            TextElement.code(
                                                    "StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)"))
                                    .linebreak()
                                    .text(
                                            "Recognized shortcut names are 'hashmap', 'rocksdb' and 'forst'.")
                                    .build());

    public static final ConfigOption<String> STATE_PATH =
            ConfigOptions.key("state.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the state path which must be used for state reading.");

    public static final ConfigOption<String> OPERATOR_UID =
            ConfigOptions.key("operator.uid")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the operator UID which must be used for state reading (Can't be used together with UID hash).");

    public static final ConfigOption<String> OPERATOR_UID_HASH =
            ConfigOptions.key("operator.uid.hash")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the operator UID hash which must be used for state reading (Can't be used together with UID).");

    // --------------------------------------------------------------------------------------------
    // Value options
    // --------------------------------------------------------------------------------------------

    /** Placeholder {@link ConfigOption}. Not used for retrieving values. */
    public static final ConfigOption<String> STATE_NAME_PLACEHOLDER =
            ConfigOptions.key(String.format("%s.#.%s", FIELDS, STATE_NAME))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the state name which must be used for state reading.");

    /** Placeholder {@link ConfigOption}. Not used for retrieving values. */
    public static final ConfigOption<StateType> STATE_TYPE_PLACEHOLDER =
            ConfigOptions.key(String.format("%s.#.%s", FIELDS, STATE_TYPE))
                    .enumType(StateType.class)
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines the state type which must be used for state reading, including %s, %s and %s. "
                                                    + "When it's not provided then it tries to be inferred from the SQL type (ARRAY=list, MAP=map, all others=value).",
                                            code(StateType.VALUE.toString()),
                                            code(StateType.LIST.toString()),
                                            code(StateType.MAP.toString()))
                                    .build());

    /** Placeholder {@link ConfigOption}. Not used for retrieving values. */
    public static final ConfigOption<String> MAP_KEY_FORMAT_PLACEHOLDER =
            ConfigOptions.key(String.format("%s.#.%s", FIELDS, MAP_KEY_FORMAT))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format class scheme for decoding map value key data. "
                                    + "When it's not provided then it tries to be inferred from the SQL type (only primitive types supported).");

    /** Placeholder {@link ConfigOption}. Not used for retrieving values. */
    public static final ConfigOption<String> VALUE_FORMAT_PLACEHOLDER =
            ConfigOptions.key(String.format("%s.#.%s", FIELDS, VALUE_FORMAT))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format class scheme for decoding value data. "
                                    + "When it's not provided then it tries to be inferred from the SQL type (only primitive types supported).");

    private SavepointConnectorOptions() {}
}
