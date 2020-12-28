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

/**
 * Parameters are widely used in machine learning realm. This class defines a common interface to
 * interact with classes with parameters.
 *
 * @param <T> the actual type of this WithParams, as the return type of setter
 */
public interface WithParams<T> {
    /**
     * Returns the all the parameters.
     *
     * @return all the parameters.
     */
    Params getParams();

    /**
     * Set the value of a specific parameter.
     *
     * @param info the info of the specific param to set
     * @param value the value to be set to the specific param
     * @param <V> the type of the specific param
     * @return the WithParams itself
     */
    @SuppressWarnings("unchecked")
    default <V> T set(ParamInfo<V> info, V value) {
        getParams().set(info, value);
        return (T) this;
    }

    /**
     * Returns the value of the specific param.
     *
     * @param info the info of the specific param, usually with default value
     * @param <V> the type of the specific param
     * @return the value of the specific param, or default value defined in the {@code info} if the
     *     inner Params doesn't contains this param
     */
    default <V> V get(ParamInfo<V> info) {
        return getParams().get(info);
    }
}
