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

package org.apache.flink.table.functions.python.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.functions.python.PythonFunction;

/** Utilities for creating PythonFunction from the fully qualified name of a Python function. */
@Internal
public enum PythonFunctionUtils {
    ;

    public static PythonFunction getPythonFunction(
            String fullyQualifiedName, ReadableConfig config, ClassLoader classLoader) {
        try {
            Class pythonFunctionFactory =
                    Class.forName(
                            "org.apache.flink.client.python.PythonFunctionFactory",
                            true,
                            classLoader);
            return (PythonFunction)
                    pythonFunctionFactory
                            .getMethod(
                                    "getPythonFunction",
                                    String.class,
                                    ReadableConfig.class,
                                    ClassLoader.class)
                            .invoke(null, fullyQualifiedName, config, classLoader);
        } catch (Throwable t) {
            throw new IllegalStateException(
                    String.format("Instantiating python function '%s' failed.", fullyQualifiedName),
                    t);
        }
    }
}
