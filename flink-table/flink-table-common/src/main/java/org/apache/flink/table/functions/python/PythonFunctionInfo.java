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

import java.io.Serializable;

/**
 * PythonFunctionInfo contains the execution information of a Python function, such as: the actual
 * Python function, the input arguments, etc.
 */
@Internal
public class PythonFunctionInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The python function to be executed. */
    private final PythonFunction pythonFunction;

    /**
     * The input arguments. It could be one of the following:
     *
     * <ul>
     *   <li>{@link Integer} – an input offset of the input row
     *   <li>{@link PythonFunctionInfo} – the execution result of another python function (nested
     *       call)
     *   <li>{@code byte[]} – a constant value
     *   <li>{@link ResultRef} – a reference to the result of a previously computed function in the
     *       flattened UDF list (used for cross-subtree CSE)
     * </ul>
     */
    private Object[] inputs;

    /**
     * Represents a reference to the result of a previously computed function in a flattened UDF
     * list. This enables cross-subtree Common Subexpression Elimination (CSE) where a nested UDF
     * call that appears in multiple positions can be computed once and its result referenced by
     * index.
     */
    @Internal
    public static class ResultRef implements Serializable {
        private static final long serialVersionUID = 1L;

        /** The index of the referenced function in the flattened UDF list. */
        public final int index;

        public ResultRef(int index) {
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResultRef resultRef = (ResultRef) o;
            return index == resultRef.index;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(index);
        }

        @Override
        public String toString() {
            return "ResultRef(" + index + ")";
        }
    }

    public PythonFunctionInfo(PythonFunction pythonFunction, Object[] inputs) {
        this.pythonFunction = Preconditions.checkNotNull(pythonFunction);
        this.inputs = Preconditions.checkNotNull(inputs);
    }

    public PythonFunction getPythonFunction() {
        return this.pythonFunction;
    }

    public Object[] getInputs() {
        return this.inputs;
    }

    public void setInputs(Object[] inputs) {
        Preconditions.checkNotNull(inputs);
        this.inputs = inputs;
    }
}
