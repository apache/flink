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

package org.apache.flink.datastream.api.context;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.watermark.WatermarkManager;
import org.apache.flink.datastream.api.function.ApplyPartitionFunction;

/**
 * This interface represents the context associated with all operations must be applied to all
 * partitions.
 */
@Experimental
public interface NonPartitionedContext<OUT> extends RuntimeContext {
    /**
     * Apply a function to all partitions. For keyed stream, it will apply to all keys. For
     * non-keyed stream, it will apply to single partition.
     */
    void applyToAllPartitions(ApplyPartitionFunction<OUT> applyPartitionFunction) throws Exception;

    WatermarkManager getWatermarkManager();
}
