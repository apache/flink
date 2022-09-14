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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Helper methods related to {@link DefaultCompletedCheckpointStore}. */
public class DefaultCompletedCheckpointStoreUtils {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultCompletedCheckpointStoreUtils.class);

    private DefaultCompletedCheckpointStoreUtils() {
        // No-op.
    }

    /**
     * Extracts maximum number of retained checkpoints configuration from the passed {@link
     * Configuration}. The default value is used as a fallback if the passed value is a value larger
     * than {@code 0}.
     *
     * @param config The configuration that is accessed.
     * @param logger The {@link Logger} used for exposing the warning if the configured value is
     *     invalid.
     * @return The maximum number of retained checkpoints based on the passed {@code Configuration}.
     */
    public static int getMaximumNumberOfRetainedCheckpoints(Configuration config, Logger logger) {
        final int maxNumberOfCheckpointsToRetain =
                config.getInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

        if (maxNumberOfCheckpointsToRetain <= 0) {
            // warning and use 1 as the default value if the setting in
            // state.checkpoints.max-retained-checkpoints is not greater than 0.
            logger.warn(
                    "The setting for '{} : {}' is invalid. Using default value of {}",
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
                    maxNumberOfCheckpointsToRetain,
                    CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

            return CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
        }

        return maxNumberOfCheckpointsToRetain;
    }

    /**
     * Fetch all {@link CompletedCheckpoint completed checkpoints} from an {@link StateHandleStore
     * external store}. This method is intended for retrieving an initial state of {@link
     * DefaultCompletedCheckpointStore}.
     *
     * @param checkpointStateHandleStore Completed checkpoints in external store.
     * @param completedCheckpointStoreUtil Utilities for completed checkpoint store.
     * @param <R> Type of {@link ResourceVersion}
     * @return Immutable collection of {@link CompletedCheckpoint completed checkpoints}.
     * @throws Exception If we're not able to fetch checkpoints for some reason.
     */
    public static <R extends ResourceVersion<R>>
            Collection<CompletedCheckpoint> retrieveCompletedCheckpoints(
                    StateHandleStore<CompletedCheckpoint, R> checkpointStateHandleStore,
                    CheckpointStoreUtil completedCheckpointStoreUtil)
                    throws Exception {

        LOG.info("Recovering checkpoints from {}.", checkpointStateHandleStore);

        // Get all there is first.
        final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> initialCheckpoints =
                checkpointStateHandleStore.getAllAndLock();

        // Sort checkpoints by name.
        initialCheckpoints.sort(Comparator.comparing(o -> o.f1));

        final int numberOfInitialCheckpoints = initialCheckpoints.size();

        LOG.info(
                "Found {} checkpoints in {}.",
                numberOfInitialCheckpoints,
                checkpointStateHandleStore);
        final List<CompletedCheckpoint> retrievedCheckpoints =
                new ArrayList<>(numberOfInitialCheckpoints);
        LOG.info("Trying to fetch {} checkpoints from storage.", numberOfInitialCheckpoints);

        for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> checkpointStateHandle :
                initialCheckpoints) {
            retrievedCheckpoints.add(
                    checkNotNull(
                            retrieveCompletedCheckpoint(
                                    completedCheckpointStoreUtil, checkpointStateHandle)));
        }
        return Collections.unmodifiableList(retrievedCheckpoints);
    }

    private static CompletedCheckpoint retrieveCompletedCheckpoint(
            CheckpointStoreUtil completedCheckpointStoreUtil,
            Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandle)
            throws FlinkException {
        final long checkpointId = completedCheckpointStoreUtil.nameToCheckpointID(stateHandle.f1);
        LOG.info("Trying to retrieve checkpoint {}.", checkpointId);
        try {
            return stateHandle.f0.retrieveState();
        } catch (ClassNotFoundException exception) {
            throw new FlinkException(
                    String.format(
                            "Could not retrieve checkpoint %d from state handle under %s. This indicates that you are trying to recover from state written by an older Flink version which is not compatible. Try cleaning the state handle store.",
                            checkpointId, stateHandle.f1),
                    exception);
        } catch (IOException exception) {
            throw new FlinkException(
                    String.format(
                            "Could not retrieve checkpoint %d from state handle under %s. This indicates that the retrieved state handle is broken. Try cleaning the state handle store.",
                            checkpointId, stateHandle.f1),
                    exception);
        }
    }
}
