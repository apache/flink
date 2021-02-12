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

package org.apache.flink.client.python;

import org.apache.flink.table.functions.python.PythonFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;

import static org.apache.flink.client.python.PythonEnvUtils.PythonProcessShutdownHook;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of PythonFunctionFactory. */
public class PythonFunctionFactoryImpl implements PythonFunctionFactory, Closeable {

    @Nonnull private final PythonFunctionFactory realPythonFunctionFactory;

    @Nullable private final PythonProcessShutdownHook shutdownHook;

    public PythonFunctionFactoryImpl(
            PythonFunctionFactory realPythonFunctionFactory,
            PythonProcessShutdownHook shutdownHook) {
        this.realPythonFunctionFactory = checkNotNull(realPythonFunctionFactory);
        this.shutdownHook = shutdownHook;
    }

    @Override
    public PythonFunction getPythonFunction(String moduleName, String objectName) {
        return realPythonFunctionFactory.getPythonFunction(moduleName, objectName);
    }

    @Override
    public void close() {
        if (shutdownHook != null) {
            if (Runtime.getRuntime().removeShutdownHook(shutdownHook)) {
                shutdownHook.run();
            }
        }
    }
}
