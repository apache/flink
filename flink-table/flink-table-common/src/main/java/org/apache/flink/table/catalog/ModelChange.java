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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Objects;

/** {@link ModelChange} represents the modification of the model. */
@PublicEvolving
public interface ModelChange {
    /**
     * A model change to set the model option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *     ALTER MODEL &lt;model_name&gt; SET (key=value);
     * </pre>
     *
     * @param key the option name to set.
     * @param value the option value to set.
     * @return a ModelChange represents the modification.
     */
    static SetOption set(String key, String value) {
        return new SetOption(key, value);
    }

    /**
     * A model change to reset the model option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *     ALTER MODEL &lt;model_name&gt; RESET (key)
     * </pre>
     *
     * @param key the option name to set.
     * @return a ModelChange represents the modification.
     */
    static ResetOption reset(String key) {
        return new ResetOption(key);
    }

    /**
     * A model change to set the model option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER MODEL &lt;model_name&gt; SET (key=value);
     * </pre>
     */
    @PublicEvolving
    class SetOption implements ModelChange {
        private final String key;
        private final String value;

        public SetOption(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ModelChange.SetOption)) {
                return false;
            }
            ModelChange.SetOption setOption = (ModelChange.SetOption) o;
            return Objects.equals(key, setOption.key) && Objects.equals(value, setOption.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "SetOption{" + "key='" + key + '\'' + ", value='" + value + '\'' + '}';
        }
    }

    /**
     * A model change to reset the model option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER MODEL &lt;model_name&gt; RESET (key);
     * </pre>
     */
    @PublicEvolving
    class ResetOption implements ModelChange {
        private final String key;

        public ResetOption(String key) {
            this.key = key;
        }

        /** Returns the Option key to reset. */
        public String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ModelChange.ResetOption)) {
                return false;
            }
            ModelChange.ResetOption that = (ModelChange.ResetOption) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

        @Override
        public String toString() {
            return "ResetOption{" + "key='" + key + '\'' + '}';
        }
    }
}
