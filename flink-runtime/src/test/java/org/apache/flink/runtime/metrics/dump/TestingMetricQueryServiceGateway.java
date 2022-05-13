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

package org.apache.flink.runtime.metrics.dump;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Testing implementation of the {@link MetricQueryServiceGateway}. */
public class TestingMetricQueryServiceGateway implements MetricQueryServiceGateway {

    @Nonnull
    private final Supplier<CompletableFuture<MetricDumpSerialization.MetricSerializationResult>>
            queryMetricsSupplier;

    @Nonnull private final String address;

    public TestingMetricQueryServiceGateway(
            @Nonnull
                    Supplier<CompletableFuture<MetricDumpSerialization.MetricSerializationResult>>
                            queryMetricsSupplier,
            @Nonnull String address) {
        this.queryMetricsSupplier = queryMetricsSupplier;
        this.address = address;
    }

    @Override
    public CompletableFuture<MetricDumpSerialization.MetricSerializationResult> queryMetrics(
            Time timeout) {
        return queryMetricsSupplier.get();
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return "localhost";
    }

    /** Builder for the {@link TestingMetricQueryServiceGateway}. */
    public static class Builder {
        private Supplier<CompletableFuture<MetricDumpSerialization.MetricSerializationResult>>
                queryMetricsSupplier = CompletableFuture::new;
        private String address = "localhost";

        public Builder setQueryMetricsSupplier(
                Supplier<CompletableFuture<MetricDumpSerialization.MetricSerializationResult>>
                        queryMetricsSupplier) {
            this.queryMetricsSupplier = queryMetricsSupplier;
            return this;
        }

        public Builder setAddress(String address) {
            this.address = address;
            return this;
        }

        public TestingMetricQueryServiceGateway build() {
            return new TestingMetricQueryServiceGateway(queryMetricsSupplier, address);
        }
    }
}
