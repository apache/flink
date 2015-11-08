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

import org.apache.flink.runtime.jobmanager.RecoveryMode;

import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link CheckpointIDCounter} instances for JobManagers running in {@link RecoveryMode#STANDALONE}.
 *
 * <p>Simple wrapper of an {@link AtomicLong}. This is sufficient, because job managers are not
 * recoverable in this recovery mode.
 */
public class StandaloneCheckpointIDCounter implements CheckpointIDCounter {

	private final AtomicLong checkpointIdCounter = new AtomicLong(1);

	@Override
	public void start() throws Exception {
	}

	@Override
	public void stop() throws Exception {
	}

	@Override
	public long getAndIncrement() throws Exception {
		return checkpointIdCounter.getAndIncrement();
	}
}
