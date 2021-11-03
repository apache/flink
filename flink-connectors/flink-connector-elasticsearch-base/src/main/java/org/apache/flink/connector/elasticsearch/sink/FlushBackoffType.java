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

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Used to control whether the sink should retry failed requests at all or with which kind back off
 * strategy.
 */
@PublicEvolving
public enum FlushBackoffType {
    /** After every failure, it waits a configured time until the retries are exhausted. */
    CONSTANT,
    /**
     * After every failure, it waits initially the configured time and increases the waiting time
     * exponentially until the retries are exhausted.
     */
    EXPONENTIAL,
    /** The failure is not retried. */
    NONE,
}
