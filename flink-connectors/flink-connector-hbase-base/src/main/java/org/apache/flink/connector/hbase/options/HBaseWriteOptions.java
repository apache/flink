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

package org.apache.flink.connector.hbase.options;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.hbase.client.ConnectionConfiguration;

import java.io.Serializable;
import java.util.Objects;

/** Options for HBase writing. */
@Internal
public class HBaseWriteOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxRows;
    private final long bufferFlushIntervalMillis;
    private final Integer parallelism;

    private HBaseWriteOptions(
            long bufferFlushMaxSizeInBytes,
            long bufferFlushMaxMutations,
            long bufferFlushIntervalMillis,
            Integer parallelism) {
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
        this.bufferFlushMaxRows = bufferFlushMaxMutations;
        this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
        this.parallelism = parallelism;
    }

    public long getBufferFlushMaxSizeInBytes() {
        return bufferFlushMaxSizeInBytes;
    }

    public long getBufferFlushMaxRows() {
        return bufferFlushMaxRows;
    }

    public long getBufferFlushIntervalMillis() {
        return bufferFlushIntervalMillis;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    @Override
    public String toString() {
        return "HBaseWriteOptions{"
                + "bufferFlushMaxSizeInBytes="
                + bufferFlushMaxSizeInBytes
                + ", bufferFlushMaxRows="
                + bufferFlushMaxRows
                + ", bufferFlushIntervalMillis="
                + bufferFlushIntervalMillis
                + ", parallelism="
                + parallelism
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HBaseWriteOptions that = (HBaseWriteOptions) o;
        return bufferFlushMaxSizeInBytes == that.bufferFlushMaxSizeInBytes
                && bufferFlushMaxRows == that.bufferFlushMaxRows
                && bufferFlushIntervalMillis == that.bufferFlushIntervalMillis
                && parallelism == that.parallelism;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bufferFlushMaxSizeInBytes,
                bufferFlushMaxRows,
                bufferFlushIntervalMillis,
                parallelism);
    }

    /** Creates a builder for {@link HBaseWriteOptions}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link HBaseWriteOptions}. */
    public static class Builder {

        private long bufferFlushMaxSizeInBytes = ConnectionConfiguration.WRITE_BUFFER_SIZE_DEFAULT;
        private long bufferFlushMaxRows = 0;
        private long bufferFlushIntervalMillis = 0;
        private Integer parallelism;

        /**
         * Optional. Sets when to flush a buffered request based on the memory size of rows
         * currently added. Default to <code>2mb</code>.
         */
        public Builder setBufferFlushMaxSizeInBytes(long bufferFlushMaxSizeInBytes) {
            this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
            return this;
        }

        /**
         * Optional. Sets when to flush buffered request based on the number of rows currently
         * added. Defaults to not set, i.e. won't flush based on the number of buffered rows.
         */
        public Builder setBufferFlushMaxRows(long bufferFlushMaxRows) {
            this.bufferFlushMaxRows = bufferFlushMaxRows;
            return this;
        }

        /**
         * Optional. Sets a flush interval flushing buffered requesting if the interval passes, in
         * milliseconds. Defaults to not set, i.e. won't flush based on flush interval.
         */
        public Builder setBufferFlushIntervalMillis(long bufferFlushIntervalMillis) {
            this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
            return this;
        }

        /**
         * Optional. Defines the parallelism of the HBase sink operator. By default, the parallelism
         * is determined by the framework using the same parallelism of the upstream chained
         * operator.
         */
        public Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /** Creates a new instance of {@link HBaseWriteOptions}. */
        public HBaseWriteOptions build() {
            return new HBaseWriteOptions(
                    bufferFlushMaxSizeInBytes,
                    bufferFlushMaxRows,
                    bufferFlushIntervalMillis,
                    parallelism);
        }
    }
}
