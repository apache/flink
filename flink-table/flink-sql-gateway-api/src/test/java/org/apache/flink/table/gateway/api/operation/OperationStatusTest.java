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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.table.gateway.api.operation.OperationStatus.CANCELED;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.CLOSED;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.ERROR;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.FINISHED;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.INITIALIZED;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.PENDING;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.RUNNING;
import static org.apache.flink.table.gateway.api.operation.OperationStatus.TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link OperationStatus}. */
public class OperationStatusTest {

    @Test
    public void testSupportedTransition() {
        for (OperationStatusTransition translation : getSupportedOperationStatusTransition()) {
            assertTrue(OperationStatus.isValidStatusTransition(translation.from, translation.to));
        }
    }

    @Test
    public void testUnsupportedTransition() {
        Set<OperationStatusTransition> supported = getSupportedOperationStatusTransition();
        for (OperationStatus from : OperationStatus.values()) {
            for (OperationStatus to : OperationStatus.values()) {
                if (supported.contains(new OperationStatusTransition(from, to))) {
                    continue;
                }

                assertFalse(OperationStatus.isValidStatusTransition(from, to));
            }
        }
    }

    private Set<OperationStatusTransition> getSupportedOperationStatusTransition() {
        return new HashSet<>(
                Arrays.asList(
                        new OperationStatusTransition(INITIALIZED, PENDING),
                        new OperationStatusTransition(PENDING, RUNNING),
                        new OperationStatusTransition(RUNNING, FINISHED),
                        new OperationStatusTransition(INITIALIZED, CANCELED),
                        new OperationStatusTransition(PENDING, CANCELED),
                        new OperationStatusTransition(RUNNING, CANCELED),
                        new OperationStatusTransition(INITIALIZED, TIMEOUT),
                        new OperationStatusTransition(PENDING, TIMEOUT),
                        new OperationStatusTransition(RUNNING, TIMEOUT),
                        new OperationStatusTransition(INITIALIZED, ERROR),
                        new OperationStatusTransition(PENDING, ERROR),
                        new OperationStatusTransition(RUNNING, ERROR),
                        new OperationStatusTransition(INITIALIZED, CLOSED),
                        new OperationStatusTransition(PENDING, CLOSED),
                        new OperationStatusTransition(RUNNING, CLOSED),
                        new OperationStatusTransition(CANCELED, CLOSED),
                        new OperationStatusTransition(TIMEOUT, CLOSED),
                        new OperationStatusTransition(ERROR, CLOSED),
                        new OperationStatusTransition(FINISHED, CLOSED)));
    }

    private static class OperationStatusTransition {
        OperationStatus from;
        OperationStatus to;

        public OperationStatusTransition(OperationStatus from, OperationStatus to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OperationStatusTransition)) {
                return false;
            }
            OperationStatusTransition that = (OperationStatusTransition) o;
            return from == that.from && to == that.to;
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, to);
        }
    }
}
