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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * Base interface for converting data during runtime.
 *
 * <p>Instances of this interface are provided by the planner. They are used for converting between
 * data structures or performing other mapping transformations.
 *
 * <p>Because runtime converters are {@link Serializable}, instances can be directly passed into a
 * runtime implementation, stored in a member variable, and used when it comes to the execution.
 */
@PublicEvolving
public interface RuntimeConverter extends Serializable {

    /**
     * Initializes the converter during runtime.
     *
     * <p>This should be called in the {@code open()} method of a runtime class.
     */
    void open(Context context);

    /** Context for conversions during runtime. */
    interface Context {

        /** Runtime classloader for loading user-defined classes. */
        ClassLoader getClassLoader();

        /**
         * Creates a new instance of {@link Context}.
         *
         * @param classLoader runtime classloader for loading user-defined classes.
         */
        static Context create(ClassLoader classLoader) {
            return new Context() {
                @Override
                public ClassLoader getClassLoader() {
                    return classLoader;
                }
            };
        }
    }
}
