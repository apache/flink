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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.Snapshotable;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.Disposable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ThrowingSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * This class implements the logic that creates (and potentially restores) a state backend. The restore logic
 * considers multiple, prioritized options of snapshots to restore from, where all of the options should recreate
 * the same state for the backend. When we fail to restore from the snapshot with the highest priority (typically
 * the "fastest" to restore), we fallback to the next snapshot with the next highest priority. We also take care
 * of cleaning up from failed restore attempts. We only reattempt when the problem occurs during the restore call
 * and will only stop after all snapshot alternatives are exhausted and all failed.
 *
 * @param <T> type of the restored backend.
 * @param <S> type of the supplied snapshots from which the backend restores.
 */
public class BackendRestorerProcedure<
	T extends Closeable & Disposable & Snapshotable<?, StateObjectCollection<S>>,
	S extends StateObject> {

	/** Logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(BackendRestorerProcedure.class);

	/** Factory for new, fresh backends without state. */
	private final ThrowingSupplier<T, Exception> instanceSupplier;

	/** This registry is used so that recovery can participate in the task lifecycle, i.e. can be canceled. */
	private final CloseableRegistry backendCloseableRegistry;

	/**
	 * Creates a new backend restorer using the given backend supplier and the closeable registry.
	 *
	 * @param instanceSupplier factory function for new, empty backend instances.
	 * @param backendCloseableRegistry registry to allow participation in task lifecycle, e.g. react to cancel.
	 */
	public BackendRestorerProcedure(
		@Nonnull ThrowingSupplier<T, Exception> instanceSupplier,
		@Nonnull CloseableRegistry backendCloseableRegistry) {

		this.instanceSupplier = Preconditions.checkNotNull(instanceSupplier);
		this.backendCloseableRegistry = Preconditions.checkNotNull(backendCloseableRegistry);
	}

	/**
	 * Creates a new state backend and restores it from the provided set of state snapshot alternatives.
	 *
	 * @param restoreOptions iterator over a prioritized set of state snapshot alternatives for recovery.
	 * @return the created (and restored) state backend.
	 * @throws Exception if the backend could not be created or restored.
	 */
	public @Nonnull T createAndRestore(@Nonnull Iterator<StateObjectCollection<S>> restoreOptions) throws Exception {

		StateObjectCollection<S> restoreState = null;

		boolean retry;

		while (true) {

			retry = false;

			// checks if there is any state to restore or if this going to be a fresh backend...
			if (restoreOptions.hasNext()) {
				restoreState = restoreOptions.next();
			}

			// create a new, empty backend.
			final T backendInstance = instanceSupplier.get();

			try {
				// register the backend with the registry to participate in task lifecycle w.r.t. cancellation.
				backendCloseableRegistry.registerCloseable(backendInstance);

				// if something goes wrong after this point, we can blame it on recovery and should retry if there are
				// still snapshot alternatives.
				retry = restoreOptions.hasNext();

				// attempt to restore from snapshot (or null if no state was checkpointed).
				backendInstance.restore(restoreState);

				return backendInstance;
			} catch (Exception ex) {

				// under failure, we need do close...
				if (backendCloseableRegistry.unregisterCloseable(backendInstance)) {
					try {
						backendInstance.close();
					} catch (IOException closeEx) {
						ex = ExceptionUtils.firstOrSuppressed(closeEx, ex);
					}
				}

				// ... and dispose, e.g. to release native resources.
				try {
					backendInstance.dispose();
				} catch (Exception disposeEx) {
					ex = ExceptionUtils.firstOrSuppressed(disposeEx, ex);
				}

				// if we see no point in a retry, this is the time to report an exception...
				if (retry) {
					//...otherwise we just log that there was a problem.
					LOG.warn("Exception while restoring backend, will retry with another snapshot replica.", ex);
				} else {
					// assume there is no more retry option for the problem.
					throw ex;
				}
			}
		}
	}
}
