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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Strategy for handling unknown commit failures in sink operators.
 *
 * <p>This strategy applies to {@link
 * org.apache.flink.api.connector.sink2.Committer.CommitRequest#signalFailedWithUnknownReason}. It
 * does not affect known failures (which are always discarded) or committables that exhaust all
 * retries.
 *
 * @see SinkOptions#COMMITTER_FAILURE_STRATEGY
 */
@PublicEvolving
public enum CommitFailureStrategy {

    /** Fail the job on unknown commit errors (default, current behavior). */
    FAIL,

    /** Log a warning and skip the committable on unknown commit errors. */
    WARN
}
