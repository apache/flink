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

package org.apache.flink.connector.datagen.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Container class for wrapping a {@link GeneratorFunction} with its configuration options. */
@Internal
public class DataGeneratorContainer {
    private final GeneratorFunction<Long, ?> generator;

    /** Generator config options, for validation. */
    private final Set<ConfigOption<?>> options;

    private DataGeneratorContainer(
            GeneratorFunction<Long, ?> generator, Set<ConfigOption<?>> options) {
        this.generator = generator;
        this.options = options;
    }

    public static DataGeneratorContainer of(
            GeneratorFunction<Long, ?> generator, ConfigOption<?>... options) {
        return new DataGeneratorContainer(generator, new HashSet<>(Arrays.asList(options)));
    }

    public GeneratorFunction<Long, ?> getGenerator() {
        return generator;
    }

    public Set<ConfigOption<?>> getOptions() {
        return options;
    }
}
