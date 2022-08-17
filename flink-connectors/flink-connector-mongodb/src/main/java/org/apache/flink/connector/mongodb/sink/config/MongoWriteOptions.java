/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.BULK_FLUSH_INTERVAL;
import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.BULK_FLUSH_MAX_ACTIONS;
import static org.apache.flink.connector.mongodb.table.config.MongoConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The configured class for Mongo sink. */
@PublicEvolving
public class MongoWriteOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int bulkFlushMaxActions;
    private final long bulkFlushIntervalMs;
    private final int maxRetryTimes;
    private final DeliveryGuarantee deliveryGuarantee;
    private final @Nullable Integer parallelism;

    private MongoWriteOptions(
            int bulkFlushMaxActions,
            long bulkFlushIntervalMs,
            int maxRetryTimes,
            DeliveryGuarantee deliveryGuarantee,
            @Nullable Integer parallelism) {
        this.bulkFlushMaxActions = bulkFlushMaxActions;
        this.bulkFlushIntervalMs = bulkFlushIntervalMs;
        this.maxRetryTimes = maxRetryTimes;
        this.deliveryGuarantee = deliveryGuarantee;
        this.parallelism = parallelism;
    }

    public int getBulkFlushMaxActions() {
        return bulkFlushMaxActions;
    }

    public long getBulkFlushIntervalMs() {
        return bulkFlushIntervalMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    @Nullable
    public Integer getParallelism() {
        return parallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoWriteOptions that = (MongoWriteOptions) o;
        return bulkFlushMaxActions == that.bulkFlushMaxActions
                && bulkFlushIntervalMs == that.bulkFlushIntervalMs
                && maxRetryTimes == that.maxRetryTimes
                && deliveryGuarantee == that.deliveryGuarantee
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bulkFlushMaxActions,
                bulkFlushIntervalMs,
                maxRetryTimes,
                deliveryGuarantee,
                parallelism);
    }

    public static MongoWriteOptionsBuilder builder() {
        return new MongoWriteOptionsBuilder();
    }

    /** Builder for {@link MongoWriteOptions}. */
    public static class MongoWriteOptionsBuilder {
        private int bulkFlushMaxActions = BULK_FLUSH_MAX_ACTIONS.defaultValue();
        private long bulkFlushIntervalMs = BULK_FLUSH_INTERVAL.defaultValue().toMillis();
        private int maxRetryTimes = SINK_MAX_RETRIES.defaultValue();
        private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        private @Nullable Integer parallelism;

        /**
         * Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
         * disable it. The default flush size 1000.
         *
         * @param numMaxActions the maximum number of actions to buffer per bulk request.
         * @return this builder
         */
        public MongoWriteOptionsBuilder setBulkFlushMaxActions(int numMaxActions) {
            checkState(
                    numMaxActions == -1 || numMaxActions > 0,
                    "Max number of buffered actions must be larger than 0.");
            this.bulkFlushMaxActions = numMaxActions;
            return this;
        }

        /**
         * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
         *
         * @param intervalMillis the bulk flush interval, in milliseconds.
         * @return this builder
         */
        public MongoWriteOptionsBuilder setBulkFlushIntervalMs(long intervalMillis) {
            checkState(
                    intervalMillis == -1 || intervalMillis >= 0,
                    "Interval (in milliseconds) between each flush must be larger than "
                            + "or equal to 0.");
            this.bulkFlushIntervalMs = intervalMillis;
            return this;
        }

        /**
         * Sets the max retry times if writing records failed.
         *
         * @param maxRetryTimes the max retry times.
         * @return this builder
         */
        public MongoWriteOptionsBuilder setMaxRetryTimes(int maxRetryTimes) {
            checkArgument(
                    maxRetryTimes >= 0, "The max retry times must be larger than or equal to 0.");
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        /**
         * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
         * DeliveryGuarantee#NONE}
         *
         * @param deliveryGuarantee which describes the record emission behaviour
         * @return this builder
         */
        public MongoWriteOptionsBuilder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            checkState(
                    deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                    "Mongo sink does not support the EXACTLY_ONCE guarantee.");
            this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
            return this;
        }

        /**
         * Sets the write parallelism.
         *
         * @param parallelism the write parallelism
         * @return this builder
         */
        public MongoWriteOptionsBuilder setParallelism(Integer parallelism) {
            checkArgument(
                    parallelism == null || parallelism > 0,
                    "Mongo sink parallelism must be larger than 0.");
            this.parallelism = parallelism;
            return this;
        }

        /**
         * Build the {@link MongoWriteOptions}.
         *
         * @return a MongoWriteOptions with the settings made for this builder.
         */
        public MongoWriteOptions build() {
            return new MongoWriteOptions(
                    bulkFlushMaxActions,
                    bulkFlushIntervalMs,
                    maxRetryTimes,
                    deliveryGuarantee,
                    parallelism);
        }
    }
}
