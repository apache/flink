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

package org.apache.flink.runtime.io.network.partition;

/**
 * {@link SubpartitionSelector} helps to choose from multiple available subpartitions when their
 * output buffers should union into one stream.
 *
 * @param <T> data type that could be used to represent a subpartition.
 */
public interface SubpartitionSelector<T> {
    /**
     * Marks a subpartition as having data available.
     *
     * @return true if this selector did not already contain the subpartition.
     */
    boolean notifyDataAvailable(T subpartition);

    /** Returns the next subpartition to consume data. */
    T getNextSubpartitionToConsume();

    /**
     * Records the status of the last consumption attempt on the subpartition returned by the last
     * invocation of {@link #getNextSubpartitionToConsume()}.
     *
     * <p>This method must be invoked every time a subpartition acquired from this class is
     * consumed.
     *
     * @param isDataAvailable whether the consumption returned a valid data.
     * @param isPartialRecord whether the returned data contains partial record. Ignored if there
     *     was no data available.
     */
    void markLastConsumptionStatus(boolean isDataAvailable, boolean isPartialRecord);

    /**
     * Whether the invoker can get a different subpartition in the next invocation of {@link
     * #getNextSubpartitionToConsume()}.
     */
    boolean isMoreSubpartitionSwitchable();
}
