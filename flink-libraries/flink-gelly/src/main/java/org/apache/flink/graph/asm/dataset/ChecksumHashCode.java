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

package org.apache.flink.graph.asm.dataset;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.AnalyticHelper;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;

/**
 * Convenience method to get the count (number of elements) of a {@link DataSet} as well as the
 * checksum (sum over element hashes).
 *
 * @param <T> element type
 */
public class ChecksumHashCode<T> extends DataSetAnalyticBase<T, Checksum> {

    private static final String CHECKSUM = "checksum";

    private ChecksumHashCodeHelper<T> checksumHashCodeHelper;

    @Override
    public ChecksumHashCode<T> run(DataSet<T> input) throws Exception {
        super.run(input);

        checksumHashCodeHelper = new ChecksumHashCodeHelper<>();

        input.output(checksumHashCodeHelper).name("ChecksumHashCode");

        return this;
    }

    @Override
    public Checksum getResult() {
        return checksumHashCodeHelper.getAccumulator(env, CHECKSUM);
    }

    /**
     * Helper class to count elements and sum element hashcodes.
     *
     * @param <U> element type
     */
    private static class ChecksumHashCodeHelper<U> extends AnalyticHelper<U> {
        private long count;
        private long checksum;

        @Override
        public void writeRecord(U record) throws IOException {
            count++;
            // convert 32-bit integer to non-negative long
            checksum += record.hashCode() & 0xffffffffL;
        }

        @Override
        public void close() throws IOException {
            addAccumulator(CHECKSUM, new Checksum(count, checksum));
        }
    }

    /** Wraps checksum and count. */
    public static class Checksum implements SimpleAccumulator<Checksum> {
        private long count;

        private long checksum;

        /**
         * Instantiate an immutable result.
         *
         * @param count count
         * @param checksum checksum
         */
        public Checksum(long count, long checksum) {
            this.count = count;
            this.checksum = checksum;
        }

        /**
         * Get the number of elements.
         *
         * @return number of elements
         */
        public long getCount() {
            return count;
        }

        /**
         * Get the checksum over the hash() of elements.
         *
         * @return checksum
         */
        public long getChecksum() {
            return checksum;
        }

        @Override
        public String toString() {
            return String.format("ChecksumHashCode 0x%016x, count %d", this.checksum, this.count);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(count).append(checksum).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (obj.getClass() != getClass()) {
                return false;
            }

            Checksum rhs = (Checksum) obj;

            return new EqualsBuilder()
                    .append(count, rhs.count)
                    .append(checksum, rhs.checksum)
                    .isEquals();
        }

        // Methods implementing SimpleAccumulator

        @Override
        public void add(Checksum value) {
            count += value.count;
            checksum += value.checksum;
        }

        @Override
        public Checksum getLocalValue() {
            return this;
        }

        @Override
        public void resetLocal() {
            count = 0;
            checksum = 0;
        }

        @Override
        public void merge(Accumulator<Checksum, Checksum> other) {
            add(other.getLocalValue());
        }

        @Override
        public Accumulator<Checksum, Checksum> clone() {
            return new Checksum(count, checksum);
        }
    }
}
