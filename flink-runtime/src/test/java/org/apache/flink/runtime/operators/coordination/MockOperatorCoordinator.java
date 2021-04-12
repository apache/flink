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

package org.apache.flink.runtime.operators.coordination;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * An empty interface implementation of the {@link OperatorCoordinator}. If you need a testing stub,
 * use the {@link TestingOperatorCoordinator} instead.
 */
public final class MockOperatorCoordinator implements OperatorCoordinator {

    @Override
    public void start() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        throw new UnsupportedOperationException();
    }
}
