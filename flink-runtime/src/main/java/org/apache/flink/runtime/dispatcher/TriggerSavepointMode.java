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

package org.apache.flink.runtime.dispatcher;

/**
 * Describes the context of taking a savepoint: Whether it is a savepoint for a running job or
 * whether the job is cancelled, suspended or terminated with a savepoint.
 */
public enum TriggerSavepointMode {
    SAVEPOINT,
    CANCEL_WITH_SAVEPOINT,
    SUSPEND_WITH_SAVEPOINT,
    TERMINATE_WITH_SAVEPOINT;

    /** Whether the operation will result in a globally terminal job status. */
    public boolean isTerminalMode() {
        return this == CANCEL_WITH_SAVEPOINT || this == TERMINATE_WITH_SAVEPOINT;
    }
}
