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

package org.apache.flink.process.api.context;

import org.apache.flink.annotation.Experimental;

import java.util.Optional;

/** This is responsibility for retrieving timestamp related things of process function. */
@Experimental
public interface TimestampManager {
    /**
     * Get the timestamp of current processing record.
     *
     * @return the timestamp of current processed record. If it does not have timestamp, empty will
     *     be returned.
     */
    Optional<Long> getCurrentRecordTimestamp();
}
