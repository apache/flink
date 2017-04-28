/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.commons.io.IOUtils;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.io.async.AbstractAsyncIOCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract base class for async IO operations of snapshots against a
 * {@link java.util.zip.CheckedOutputStream}. This includes participating in lifecycle management
 * through a {@link CloseableRegistry}.
 */
public abstract class AbstractAsyncSnapshotIOCallable<H extends StateObject>
	extends AbstractAsyncIOCallable<H, CheckpointStreamFactory.CheckpointStateOutputStream> {

	protected final  long checkpointId;
	protected final  long timestamp;

	protected final CheckpointStreamFactory streamFactory;
	protected final CloseableRegistry closeStreamOnCancelRegistry;
	protected final AtomicBoolean open;

	public AbstractAsyncSnapshotIOCallable(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CloseableRegistry closeStreamOnCancelRegistry) {

		this.streamFactory = Preconditions.checkNotNull(streamFactory);
		this.closeStreamOnCancelRegistry = Preconditions.checkNotNull(closeStreamOnCancelRegistry);
		this.checkpointId = checkpointId;
		this.timestamp = timestamp;
		this.open = new AtomicBoolean(false);
	}

	@Override
	public CheckpointStreamFactory.CheckpointStateOutputStream openIOHandle() throws Exception {
		if (checkStreamClosedAndDoTransitionToOpen()) {
			CheckpointStreamFactory.CheckpointStateOutputStream stream =
				streamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);
			try {
				closeStreamOnCancelRegistry.registerClosable(stream);
				return stream;
			} catch (Exception ex) {
				open.set(false);
				throw ex;
			}
		} else {
			throw new IOException("Async snapshot: a checkpoint stream was already opened.");
		}
	}

	@Override
	public void done(boolean canceled) {
		if (checkStreamOpenAndDoTransitionToClose()) {
			CheckpointStreamFactory.CheckpointStateOutputStream stream = getIoHandle();
			if (stream != null) {
				closeStreamOnCancelRegistry.unregisterClosable(stream);
				IOUtils.closeQuietly(stream);
			}
		}
	}

	protected boolean checkStreamClosedAndDoTransitionToOpen() {
		return open.compareAndSet(false, true);
	}

	protected boolean checkStreamOpenAndDoTransitionToClose() {
		return open.compareAndSet(true, false);
	}

	protected StreamStateHandle closeStreamAndGetStateHandle() throws IOException {
		if (checkStreamOpenAndDoTransitionToClose()) {
			final CheckpointStreamFactory.CheckpointStateOutputStream stream = getIoHandle();
			try {
				return stream.closeAndGetHandle();
			} finally {
				closeStreamOnCancelRegistry.unregisterClosable(stream);
			}
		} else {
			throw new IOException("Checkpoint stream already closed.");
		}
	}

}
