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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.PublicEvolving;

/** An accumulator that finds the minimum {@code integer} value. */
@PublicEvolving
public class IntMinimum implements SimpleAccumulator<Integer> {

    private static final long serialVersionUID = 1L;

    private int min = Integer.MAX_VALUE;

    public IntMinimum() {}

    public IntMinimum(int value) {
        this.min = value;
    }

    // ------------------------------------------------------------------------
    //  Accumulator
    // ------------------------------------------------------------------------

    /** Consider using {@link #add(int)} instead for primitive integer values */
    @Override
    public void add(Integer value) {
        this.min = Math.min(this.min, value);
    }

    @Override
    public Integer getLocalValue() {
        return this.min;
    }

    @Override
    public void merge(Accumulator<Integer, Integer> other) {
        this.min = Math.min(this.min, other.getLocalValue());
    }

    @Override
    public void resetLocal() {
        this.min = Integer.MAX_VALUE;
    }

    @Override
    public IntMinimum clone() {
        IntMinimum clone = new IntMinimum();
        clone.min = this.min;
        return clone;
    }

    // ------------------------------------------------------------------------
    //  Primitive Specializations
    // ------------------------------------------------------------------------

    public void add(int value) {
        this.min = Math.min(this.min, value);
    }

    public int getLocalValuePrimitive() {
        return this.min;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "IntMinimum " + this.min;
    }
}
