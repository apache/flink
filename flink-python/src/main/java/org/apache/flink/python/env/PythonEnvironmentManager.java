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

package org.apache.flink.python.env;

import org.apache.flink.annotation.Internal;
import org.apache.flink.python.PythonFunctionRunner;

/**
 * The base interface of python environment manager which is used to create the PythonEnvironment
 * object and the RetrievalToken.
 */
@Internal
public interface PythonEnvironmentManager extends AutoCloseable {

    /** Initialize the environment manager. */
    void open() throws Exception;

    /**
     * Creates the PythonEnvironment object used in {@link PythonFunctionRunner}.
     *
     * @return The PythonEnvironment object which represents the environment(process, docker, etc)
     *     the python worker would run in.
     */
    PythonEnvironment createEnvironment() throws Exception;

    /**
     * Creates the RetrievalToken used in {@link PythonFunctionRunner}. It contains a list of files
     * which need to transmit through ArtifactService provided by {@link PythonFunctionRunner}.
     *
     * @return The path of the RetrievalToken file.
     */
    String createRetrievalToken() throws Exception;

    /** Returns the boot log of the Python Environment. */
    String getBootLog() throws Exception;
}
