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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/**
 * Internal built-in WELFORD_M2 aggregate function to calculate the m2 term in Welford's online
 * algorithm. This is a helper function for rewriting variance related functions.
 *
 * @see <a
 *     href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm">Welford's
 *     online algorithm</a>
 */
@Internal
public abstract class WelfordM2AggFunction
        extends BuiltInAggregateFunction<Double, WelfordM2AggFunction.WelfordM2Accumulator> {

    protected final transient DataType valueType;

    public WelfordM2AggFunction(LogicalType inputType) {
        this.valueType = toInternalDataType(inputType);
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(valueType);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                WelfordM2Accumulator.class,
                DataTypes.FIELD("n", DataTypes.BIGINT().notNull()),
                DataTypes.FIELD("mean", DataTypes.DOUBLE().notNull()),
                DataTypes.FIELD("m2", DataTypes.DOUBLE().notNull()));
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypes.DOUBLE();
    }

    // --------------------------------------------------------------------------------------------
    // Accumulator
    // --------------------------------------------------------------------------------------------

    /** Accumulator for WELFORD_M2. */
    public static class WelfordM2Accumulator {

        public long n = 0L;
        public double mean = 0.0D;
        public double m2 = 0.0D;

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            WelfordM2Accumulator that = (WelfordM2Accumulator) obj;
            return n == that.n
                    && Double.compare(mean, that.mean) == 0
                    && Double.compare(m2, that.m2) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(n, mean, m2);
        }
    }

    @Override
    public WelfordM2Accumulator createAccumulator() {
        return new WelfordM2Accumulator();
    }

    public void resetAccumulator(WelfordM2Accumulator acc) {
        acc.n = 0L;
        acc.mean = 0.0D;
        acc.m2 = 0.0D;
    }

    @Override
    public Double getValue(WelfordM2Accumulator acc) {
        return acc.n <= 0 || acc.m2 < 0 ? null : acc.m2;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    protected abstract double doubleValue(Object value);

    public void accumulate(WelfordM2Accumulator acc, @Nullable Object value) {
        if (value == null) {
            return;
        }

        acc.n += 1;
        // Ignore accumulate when acc.n <= 0, but keep counting to align with the outside COUNT.
        if (acc.n <= 0) {
            return;
        }

        double val = doubleValue(value);
        double delta = val - acc.mean;
        acc.mean += delta / acc.n;
        double delta2 = val - acc.mean;
        acc.m2 += delta * delta2;
    }

    public void retract(WelfordM2Accumulator acc, @Nullable Object value) {
        if (value == null) {
            return;
        }

        acc.n -= 1;
        // Ignore accumulate when acc.n <= 0, but keep counting to align with the outside COUNT.
        if (acc.n <= 0) {
            if (acc.n == 0) {
                acc.mean = 0.0D;
                acc.m2 = 0.0D;
            }
            return;
        }

        double val = doubleValue(value);
        double delta2 = val - acc.mean;
        acc.mean -= delta2 / acc.n;
        double delta = val - acc.mean;
        acc.m2 -= delta * delta2;
    }

    public void merge(WelfordM2Accumulator acc, Iterable<WelfordM2Accumulator> its) {
        // Ignore acc with negative acc.n because it is invalid, but keep counting to align with the
        // outside COUNT.
        // Merge negativeSum to acc.n at last to avoid data loss caused by intermediate negative
        // total count.
        long negativeSum = 0;
        for (WelfordM2Accumulator other : its) {
            if (other.n <= 0) {
                negativeSum += other.n;
                continue;
            }

            if (acc.n == 0) {
                acc.n = other.n;
                acc.mean = other.mean;
                acc.m2 = other.m2;
                continue;
            }

            long newCount = acc.n + other.n;
            double deltaMean = other.mean - acc.mean;
            double newMean = acc.mean + (double) other.n / newCount * deltaMean;
            double newM2 =
                    acc.m2 + other.m2 + (double) acc.n * other.n / newCount * deltaMean * deltaMean;

            acc.n = newCount;
            acc.mean = newMean;
            acc.m2 = newM2;
        }

        acc.n += negativeSum;
        if (acc.n <= 0) {
            acc.mean = 0.0D;
            acc.m2 = 0.0D;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Sub-classes
    // --------------------------------------------------------------------------------------------

    /** Implementation for numeric types excluding DECIMAL. */
    public static class NumberFunction extends WelfordM2AggFunction {

        public NumberFunction(LogicalType inputType) {
            super(inputType);
        }

        @Override
        protected double doubleValue(Object value) {
            return ((Number) value).doubleValue();
        }
    }

    /** Implementation for DECIMAL. */
    public static class DecimalFunction extends WelfordM2AggFunction {

        public DecimalFunction(LogicalType inputType) {
            super(inputType);
        }

        @Override
        protected double doubleValue(Object value) {
            return DecimalDataUtils.doubleValue((DecimalData) value);
        }
    }
}
