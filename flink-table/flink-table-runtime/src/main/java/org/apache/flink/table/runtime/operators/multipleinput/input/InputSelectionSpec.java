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

package org.apache.flink.table.runtime.operators.multipleinput.input;

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;

import java.io.Serializable;
import java.util.Objects;

/** Describe the inputId and read order of MultipleInput operator. */
public class InputSelectionSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The input id (start from 1) used for identifying each {@link Input} in {@link
     * MultipleInputStreamOperator#getInputs()}.
     */
    private final int multipleInputId;

    /** The read order for current input in multple operator. */
    private final int readOrder;

    public InputSelectionSpec(int multipleInputId, int readOrder) {
        this.multipleInputId = multipleInputId;
        this.readOrder = readOrder;
    }

    public int getMultipleInputId() {
        return multipleInputId;
    }

    public int getReadOrder() {
        return readOrder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InputSelectionSpec that = (InputSelectionSpec) o;
        return multipleInputId == that.multipleInputId && readOrder == that.readOrder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(multipleInputId, readOrder);
    }

    @Override
    public String toString() {
        return "InputSelectionSpec{"
                + "multipleInputId="
                + multipleInputId
                + ", readOrder="
                + readOrder
                + '}';
    }
}
