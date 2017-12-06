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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FixFileFsStateOutputStream;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Interface that provides access to a CheckpointStateOutputStream and a method to provide the {@link SnapshotResult}.
 * This abstracts from different ways that a result is obtained from checkpoint output streams.
 */
public interface CheckpointStreamWithResultProvider extends Closeable {

	SnapshotResult<KeyedStateHandle> closeAndFinalizeCheckpointStreamResult(
		KeyGroupRangeOffsets kgOffs) throws IOException;

	CheckpointStreamFactory.CheckpointStateOutputStream getCheckpointOutputStream();

	@Override
	default void close() throws IOException {
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = getCheckpointOutputStream();
		if (outputStream != null) {
			outputStream.close();
		}
	}

	/**
	 * Implementation of {@link CheckpointStreamWithResultProvider} that only creates the
	 * primary/remote/jm-owned state.
	 */
	class PrimaryStreamOnly implements CheckpointStreamWithResultProvider {

		private final CheckpointStreamFactory.CheckpointStateOutputStream outputStream;

		public PrimaryStreamOnly(CheckpointStreamFactory.CheckpointStateOutputStream outputStream) {
			this.outputStream = Preconditions.checkNotNull(outputStream);
		}

		@Override
		public SnapshotResult<KeyedStateHandle> closeAndFinalizeCheckpointStreamResult(
			KeyGroupRangeOffsets kgOffs) throws IOException {

			StreamStateHandle streamStateHandle = outputStream.closeAndGetHandle();
			KeyGroupsStateHandle primaryStateHandle = new KeyGroupsStateHandle(kgOffs, streamStateHandle);
			return new SnapshotResult<>(primaryStateHandle, null);
		}

		@Override
		public CheckpointStreamFactory.CheckpointStateOutputStream getCheckpointOutputStream() {
			return outputStream;
		}
	}

	/**
	 * Implementation of {@link CheckpointStreamWithResultProvider} that creates both, the
	 * primary/remote/jm-owned state and the secondary/local/tm-owned state.
	 */
	class PrimaryAndSecondaryStream implements CheckpointStreamWithResultProvider {

		private static final Logger LOG = LoggerFactory.getLogger(PrimaryAndSecondaryStream.class);

		private final DuplicatingCheckpointOutputStream outputStream;

		public PrimaryAndSecondaryStream(DuplicatingCheckpointOutputStream outputStream) {
			this.outputStream = Preconditions.checkNotNull(outputStream);
		}

		@Override
		public SnapshotResult<KeyedStateHandle> closeAndFinalizeCheckpointStreamResult(
			KeyGroupRangeOffsets offsets) throws IOException {

			final StreamStateHandle primaryStreamStateHandle;

			try {
				primaryStreamStateHandle = outputStream.closeAndGetPrimaryHandle();
			} catch (IOException primaryEx) {
				try {
					outputStream.close();
				} catch (IOException closeEx) {
					primaryEx = ExceptionUtils.firstOrSuppressed(closeEx, primaryEx);
				}
				throw primaryEx;
			}

			StreamStateHandle secondaryStreamStateHandle = null;

			try {
				secondaryStreamStateHandle = outputStream.closeAndGetSecondaryHandle();
			} catch (IOException secondaryEx) {
				LOG.warn("Exception from secondary/local checkpoint stream.", secondaryEx);
			}

			if (primaryStreamStateHandle != null) {

				final KeyGroupsStateHandle primaryKeyGroupsStateHandle =
					new KeyGroupsStateHandle(offsets, primaryStreamStateHandle);

				final KeyGroupsStateHandle secondaryKeyGroupsStateHandle = secondaryStreamStateHandle != null ?
					new KeyGroupsStateHandle(offsets, secondaryStreamStateHandle) :
					null;

				return new SnapshotResult<>(primaryKeyGroupsStateHandle, secondaryKeyGroupsStateHandle);
			}

			return null;
		}

		@Override
		public DuplicatingCheckpointOutputStream getCheckpointOutputStream() {
			return outputStream;
		}
	}

	class Factory {

		private static final Logger LOG = LoggerFactory.getLogger(PrimaryAndSecondaryStream.class);

		public CheckpointStreamWithResultProvider create(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory primaryStreamFactory,
			LocalRecoveryDirectoryProvider secondaryStreamDirProvider) throws Exception {

			CheckpointStreamFactory.CheckpointStateOutputStream primaryOut =
				primaryStreamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);

			CheckpointStreamFactory.CheckpointStateOutputStream secondaryOut;

			if (secondaryStreamDirProvider != null) {
				try {
					File currentLocalRecoveryRoot = secondaryStreamDirProvider.nextRootDirectory();
					File outFile = new File(
						currentLocalRecoveryRoot,
						secondaryStreamDirProvider.specificFileForCheckpointId(checkpointId));
					Path outPath = new Path(outFile.toURI());
					secondaryOut = new FixFileFsStateOutputStream(outPath.getFileSystem(), outPath);
					final DuplicatingCheckpointOutputStream effectiveOutStream =
						new DuplicatingCheckpointOutputStream(primaryOut, secondaryOut);

					return new CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream(effectiveOutStream);
				} catch (IOException secondaryEx) {
					LOG.warn("Exception when opening secondary/local checkpoint output stream. " +
						"Continue only with the primary stream.", secondaryEx);
				}
			}

			return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
		}
	}
}
