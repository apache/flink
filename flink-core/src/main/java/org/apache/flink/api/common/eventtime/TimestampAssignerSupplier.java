/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;

/**
 * A supplier for {@link TimestampAssigner TimestampAssigners}. The supplier pattern is used to
 * avoid having to make {@link TimestampAssigner} {@link Serializable} for use in API methods.
 *
 * <p>This interface is {@link Serializable} because the supplier may be shipped to workers during
 * distributed execution.
 */
@PublicEvolving
@FunctionalInterface
public interface TimestampAssignerSupplier<T> extends Serializable {

    /** Instantiates a {@link TimestampAssigner}. */
    TimestampAssigner<T> createTimestampAssigner(Context context);

    static <T> TimestampAssignerSupplier<T> of(SerializableTimestampAssigner<T> assigner) {
        return new SupplierFromSerializableTimestampAssigner<>(assigner);
    }

    /**
     * Additional information available to {@link #createTimestampAssigner(Context)}. This can be
     * access to {@link org.apache.flink.metrics.MetricGroup MetricGroups}, for example.
     */
    interface Context {

        /**
         * Returns the metric group for the context in which the created {@link TimestampAssigner}
         * is used.
         *
         * <p>Instances of this class can be used to register new metrics with Flink and to create a
         * nested hierarchy based on the group names. See {@link MetricGroup} for more information
         * for the metrics system.
         *
         * @see MetricGroup
         */
        MetricGroup getMetricGroup();
    }

    /**
     * We need an actual class. Implementing this as a lambda in {@link
     * #of(SerializableTimestampAssigner)} would not allow the {@link ClosureCleaner} to "reach"
     * into the {@link SerializableTimestampAssigner}.
     */
    class SupplierFromSerializableTimestampAssigner<T> implements TimestampAssignerSupplier<T> {

        private static final long serialVersionUID = 1L;

        private final SerializableTimestampAssigner<T> assigner;

        public SupplierFromSerializableTimestampAssigner(
                SerializableTimestampAssigner<T> assigner) {
            this.assigner = assigner;
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(Context context) {
            return assigner;
        }
    }
}
