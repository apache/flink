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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;

import java.util.concurrent.CompletableFuture;

/**
 * This context is the interface through which the {@link CheckpointCoordinator} interacts with an
 * {@link OperatorCoordinator} during checkpointing and checkpoint restoring.
 */
public interface OperatorCoordinatorCheckpointContext extends OperatorInfo {

	void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) throws Exception;

	void afterSourceBarrierInjection(long checkpointId);

	void abortCurrentTriggering();

	void checkpointComplete(long checkpointId);

	void resetToCheckpoint(byte[] checkpointData) throws Exception;
}
