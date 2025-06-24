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

package org.apache.flink.table.gateway.api.operation;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Status to describe the {@code Operation}. */
@PublicEvolving
public enum OperationStatus {
    /** The operation is newly created. */
    INITIALIZED(false),

    /** Prepare the resources for the operation. */
    PENDING(false),

    /** The operation is running. */
    RUNNING(false),

    /** All the work is finished and ready for the client to fetch the results. */
    FINISHED(true),

    /** Operation has been cancelled. */
    CANCELED(true),

    /** Operation has been closed and all related resources are collected. */
    CLOSED(true),

    /** Some error happens. */
    ERROR(true),

    /** The execution of the operation timeout. */
    TIMEOUT(true);

    private final boolean isTerminalStatus;

    OperationStatus(boolean isTerminalStatus) {
        this.isTerminalStatus = isTerminalStatus;
    }

    public static boolean isValidStatusTransition(
            OperationStatus fromStatus, OperationStatus toStatus) {
        return toOperationStatusSet(fromStatus).contains(toStatus);
    }

    public boolean isTerminalStatus() {
        return isTerminalStatus;
    }

    private static Set<OperationStatus> toOperationStatusSet(OperationStatus fromStatus) {
        switch (fromStatus) {
            case INITIALIZED:
                return new HashSet<>(Arrays.asList(PENDING, CANCELED, CLOSED, TIMEOUT, ERROR));
            case PENDING:
                return new HashSet<>(Arrays.asList(RUNNING, CANCELED, CLOSED, TIMEOUT, ERROR));
            case RUNNING:
                return new HashSet<>(Arrays.asList(FINISHED, CANCELED, CLOSED, TIMEOUT, ERROR));
            case FINISHED:
            case CANCELED:
            case TIMEOUT:
            case ERROR:
                return Collections.singleton(CLOSED);
            case CLOSED:
                return Collections.emptySet();
            default:
                throw new IllegalArgumentException("Unknown from status: " + fromStatus);
        }
    }
}
