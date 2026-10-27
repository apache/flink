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

package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

/**
 * PythonFunctionInfo contains the execution information of a Python function, such as: the actual
 * Python function, the input arguments, etc.
 *
 * <p>It is itself a {@link PythonFunctionInput}, so that a nested call can be used as an argument
 * of another function.
 */
@Internal
public class PythonFunctionInfo implements PythonFunctionInput {

    private static final long serialVersionUID = 1L;

    /** The python function to be executed. */
    private final PythonFunction pythonFunction;

    /** The input arguments of this function. */
    private PythonFunctionInput[] inputs;

    public PythonFunctionInfo(PythonFunction pythonFunction, PythonFunctionInput[] inputs) {
        this.pythonFunction = Preconditions.checkNotNull(pythonFunction);
        this.inputs = Preconditions.checkNotNull(inputs);
    }

    public PythonFunction getPythonFunction() {
        return this.pythonFunction;
    }

    public PythonFunctionInput[] getInputs() {
        return this.inputs;
    }

    public void setInputs(PythonFunctionInput[] inputs) {
        Preconditions.checkNotNull(inputs);
        this.inputs = inputs;
    }
}
