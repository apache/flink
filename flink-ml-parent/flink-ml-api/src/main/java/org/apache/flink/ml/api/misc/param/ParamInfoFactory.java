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

package org.apache.flink.ml.api.misc.param;

/** Factory to create ParamInfo, all ParamInfos should be created via this class. */
public class ParamInfoFactory {
    /**
     * Returns a ParamInfoBuilder to configure and build a new ParamInfo.
     *
     * @param name name of the new ParamInfo
     * @param valueClass value class of the new ParamInfo
     * @param <V> value type of the new ParamInfo
     * @return a ParamInfoBuilder
     */
    public static <V> ParamInfoBuilder<V> createParamInfo(String name, Class<V> valueClass) {
        return new ParamInfoBuilder<>(name, valueClass);
    }

    /**
     * Builder to build a new ParamInfo. Builder is created by ParamInfoFactory with name and
     * valueClass set.
     *
     * @param <V> value type of the new ParamInfo
     */
    public static class ParamInfoBuilder<V> {
        private String name;
        private String[] alias = new String[0];
        private String description;
        private boolean isOptional = true;
        private boolean hasDefaultValue = false;
        private V defaultValue;
        private ParamValidator<V> validator;
        private Class<V> valueClass;

        ParamInfoBuilder(String name, Class<V> valueClass) {
            this.name = name;
            this.valueClass = valueClass;
        }

        /**
         * Sets the aliases of the parameter.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setAlias(String[] alias) {
            this.alias = alias;
            return this;
        }

        /**
         * Sets the description of the parameter.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setDescription(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the flag indicating the parameter is optional. The parameter is optional by default.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setOptional() {
            this.isOptional = true;
            return this;
        }

        /**
         * Sets the flag indicating the parameter is required.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setRequired() {
            this.isOptional = false;
            return this;
        }

        /**
         * Sets the flag indicating the parameter has default value, and sets the default value.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setHasDefaultValue(V defaultValue) {
            this.hasDefaultValue = true;
            this.defaultValue = defaultValue;
            return this;
        }

        /**
         * Sets the validator to validate the parameter value set by users.
         *
         * @return the builder itself
         */
        public ParamInfoBuilder<V> setValidator(ParamValidator<V> validator) {
            this.validator = validator;
            return this;
        }

        /**
         * Builds the defined ParamInfo and returns it. The ParamInfo will be immutable.
         *
         * @return the defined ParamInfo
         */
        public ParamInfo<V> build() {
            return new ParamInfo<>(
                    name,
                    alias,
                    description,
                    isOptional,
                    hasDefaultValue,
                    defaultValue,
                    validator,
                    valueClass);
        }
    }
}
