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

package org.apache.flink.connector.pulsar.common.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * Pulsar's config have some non-serializable fields which should be provided by user. You can add
 * modify the config instance by using this interface.
 *
 * @param <T> The class type for related pulsar config.
 */
@PublicEvolving
@FunctionalInterface
public interface ConfigurationDataCustomizer<T> extends Serializable {

    /**
     * Modify the given config instance, this instance is created from {@link Configuration} and
     * would be used in the later logic.
     */
    void customize(T configurationDate);

    /**
     * Returns a composed customizer that first applies this customizer to its input, and then
     * applies the {@code after} customizer to the result.
     */
    default ConfigurationDataCustomizer<T> compose(ConfigurationDataCustomizer<T> after) {
        return data -> {
            customize(data);
            // Apply the given customizer.
            after.customize(data);
        };
    }

    /** Return a blank customizer. */
    static <Y> ConfigurationDataCustomizer<Y> blankCustomizer() {
        return a -> {};
    }
}
