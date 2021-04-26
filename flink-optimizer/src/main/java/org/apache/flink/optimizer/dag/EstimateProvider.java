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

package org.apache.flink.optimizer.dag;

/**
 * Methods for operators / connections that provide estimated about data size and characteristics.
 */
public interface EstimateProvider {

    /**
     * Gets the estimated output size from this node.
     *
     * @return The estimated output size.
     */
    long getEstimatedOutputSize();

    /**
     * Gets the estimated number of records in the output of this node.
     *
     * @return The estimated number of records.
     */
    long getEstimatedNumRecords();

    /**
     * Gets the estimated number of bytes per record.
     *
     * @return The estimated number of bytes per record.
     */
    float getEstimatedAvgWidthPerOutputRecord();
}
