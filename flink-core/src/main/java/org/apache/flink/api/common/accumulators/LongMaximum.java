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

/** An accumulator that finds the maximum {@code long} value. */
@PublicEvolving
public class LongMaximum implements SimpleAccumulator<Long> {

    private static final long serialVersionUID = 1L;

    private long max = Long.MIN_VALUE;

    public LongMaximum() {}

    public LongMaximum(long value) {
        this.max = value;
    }

    // ------------------------------------------------------------------------
    //  Accumulator
    // ------------------------------------------------------------------------

    /** Consider using {@link #add(long)} instead for primitive long values */
    @Override
    public void add(Long value) {
        this.max = Math.max(this.max, value);
    }

    @Override
    public Long getLocalValue() {
        return this.max;
    }

    @Override
    public void merge(Accumulator<Long, Long> other) {
        this.max = Math.max(this.max, other.getLocalValue());
    }

    @Override
    public void resetLocal() {
        this.max = Long.MIN_VALUE;
    }

    @Override
    public LongMaximum clone() {
        LongMaximum clone = new LongMaximum();
        clone.max = this.max;
        return clone;
    }

    // ------------------------------------------------------------------------
    //  Primitive Specializations
    // ------------------------------------------------------------------------

    public void add(long value) {
        this.max = Math.max(this.max, value);
    }

    public long getLocalValuePrimitive() {
        return this.max;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "LongMaximum " + this.max;
    }
}
