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

/** Options for the savepoint connector. */
@PublicEvolving
public class SavepointConnectorOptions {

    public static final String FIELDS = "fields";
    public static final String STATE_NAME = "state-name";

    /** Value state types. */
    public enum StateType {
        VALUE,
        LIST,
        MAP
    }

    /** Determines how a savepoint table's schema and rows are derived from keyed state. */
    public enum StateReaderMode {

        /** One row per key, one column per keyed state (the general keyed-state table). */
        KEYED("keyed"),

        /**
         * Exposes a single keyed LIST/MAP state flattened into one row per list element / map
         * entry, instead of one row per key.
         */
        KEYED_FLAT("keyed-flat"),

        /**
         * One row per (key, namespace), one column per VALUE-shaped namespaced state (e.g. a window
         * operator's window-contents accumulator or window-registered value state).
         */
        WINDOWED("windowed"),

        /**
         * Exposes a single namespaced LIST/MAP state flattened into one row per list element / map
         * entry, instead of one row per (key, namespace).
         */
        WINDOWED_FLAT("windowed-flat"),

        /** One row per element of an operator {@code ListState} (no key/namespace concept). */
        LIST("list"),

        /** One row per element of an operator {@code UnionState} (no key/namespace concept). */
        UNION("union"),

        /** One row per entry of an operator {@code BroadcastState} (no key/namespace concept). */
        BROADCAST("broadcast");

        private final String value;

        StateReaderMode(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
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

    /**
     * Determines whether the table exposes the general keyed-state schema (one row per key, one
     * column per keyed state) or the flattened schema for a single LIST/MAP state (one row per list
     * element / map entry). Set automatically by {@code StateCatalog}; not intended for manual use.
     * In flattened mode, the state type (LIST or MAP) is not configured separately: it is inferred
     * from whether the table's second column is named {@code list_index} (LIST) or {@code map_key}
     * (MAP), and the state name is inferred from the name of the third column.
     */
    public static final ConfigOption<StateReaderMode> STATE_READER_MODE =
            ConfigOptions.key("state.reader.mode")
                    .enumType(StateReaderMode.class)
                    .defaultValue(StateReaderMode.KEYED)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Determines whether the table exposes the general keyed-state schema "
                                                    + "(%s, the default) or the flattened schema for a single LIST/MAP "
                                                    + "state (%s), which exposes one row per list element / map entry "
                                                    + "instead of one row per key, or one of the namespaced-state "
                                                    + "equivalents (%s, %s), which expose state registered under a "
                                                    + "non-void namespace (e.g. window-scoped state).",
                                            code(StateReaderMode.KEYED.toString()),
                                            code(StateReaderMode.KEYED_FLAT.toString()),
                                            code(StateReaderMode.WINDOWED.toString()),
                                            code(StateReaderMode.WINDOWED_FLAT.toString()))
                                    .build());

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

    /**
     * Explicitly identifies the single LIST/MAP/UNION/BROADCAST state exposed by a table whose
     * value column(s) no longer carry the state's name themselves — either because the value is
     * flattened directly into top-level columns (one column per field for a structured value, or a
     * single value column named {@code list_value}/{@code map_value} for a scalar one, used by
     * {@link #STATE_READER_MODE} {@code KEYED_FLAT}, {@code WINDOWED_FLAT}, {@code LIST}, and
     * {@code UNION}), or because the value column has a fixed name ({@code map_value}, used by
     * {@code BROADCAST}). Naming the value column after the state itself risked colliding with a
     * table's other (reserved) column names, e.g. a state literally named {@code map_key}. Set
     * automatically by {@code StateCatalog}; not intended for manual use.
     */
    public static final ConfigOption<String> FLATTENED_STATE_NAME =
            ConfigOptions.key(STATE_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the name of the single LIST/MAP/UNION/BROADCAST state exposed by this table.");

    private SavepointConnectorOptions() {}
}
