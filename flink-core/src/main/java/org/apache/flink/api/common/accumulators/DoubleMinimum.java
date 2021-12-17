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

/** An accumulator that finds the minimum {@code double} value. */
@PublicEvolving
public class DoubleMinimum implements SimpleAccumulator<Double> {

    private static final long serialVersionUID = 1L;

    private double min = Double.POSITIVE_INFINITY;

    public DoubleMinimum() {}

    public DoubleMinimum(double value) {
        this.min = value;
    }

    // ------------------------------------------------------------------------
    //  Accumulator
    // ------------------------------------------------------------------------

    /** Consider using {@link #add(double)} instead for primitive double values */
    @Override
    public void add(Double value) {
        this.min = Math.min(this.min, value);
    }

    @Override
    public Double getLocalValue() {
        return this.min;
    }

    @Override
    public void merge(Accumulator<Double, Double> other) {
        this.min = Math.min(this.min, other.getLocalValue());
    }

    @Override
    public void resetLocal() {
        this.min = Double.POSITIVE_INFINITY;
    }

    @Override
    public DoubleMinimum clone() {
        DoubleMinimum clone = new DoubleMinimum();
        clone.min = this.min;
        return clone;
    }

    // ------------------------------------------------------------------------
    //  Primitive Specializations
    // ------------------------------------------------------------------------

    public void add(double value) {
        this.min = Math.min(this.min, value);
    }

    public double getLocalValuePrimitive() {
        return this.min;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "DoubleMinimum " + this.min;
    }
}
