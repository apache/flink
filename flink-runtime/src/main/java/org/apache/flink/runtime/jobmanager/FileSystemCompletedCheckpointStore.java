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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.filesystem.FileSystemStorageHelper;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link HighAvailabilityMode#FILESYSTEM}.
 *
 * This implementation support maintaing checkpoints for a singele job. It stores
 * checkpoints on a file system and therefore relies on the high availability of the file system.
 * During recovery, the latest checkpoint is read from filesystem. If there is more than one,
 * only the latest one is used and older ones are discarded (even if the maximum number
 * of retained checkpoints is greater than one).
 */

public class FileSystemCompletedCheckpointStore implements CompletedCheckpointStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCompletedCheckpointStore.class);

    /** The maximum number of checkpoints to retain (at least 1). */
    private final int maxNumberOfCheckpointsToRetain;

    /** Storage **/
    private final FileSystemStorageHelper<CompletedCheckpoint> checkpointStorage;

    /**
     * Local copy of the completed checkpoints. This is restored from filestore
     * when recovering and is maintained in parallel to the state in during normal
     * operations.
     */
    private final ArrayDeque<File> completedCheckpoints;


    private final Executor executor;

    /**
     * Creates a {@link FileSystemCompletedCheckpointStore} instance.
     *
     * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
     *                                       least 1). Adding more checkpoints than this results
     *                                       in older checkpoints being discarded. On recovery,
     *                                       we will only start with a single checkpoint.
     * @param checkpointStorage              Completed checkpoints in file system
     * @param executor                       to execute blocking calls
     */

    public FileSystemCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain,
            FileSystemStorageHelper<CompletedCheckpoint> checkpointStorage,
            Executor executor) {

        checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");

        this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;

        this.checkpointStorage = checkpointStorage;

        this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);

        this.executor = checkNotNull(executor);
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return false;
    }

    /**
     * Gets the latest checkpoint from ZooKeeper and removes all others.
     *
     * <p><strong>Important</strong>: Even if there are more than one checkpoint in ZooKeeper,
     * this will only recover the latest and discard the others. Otherwise, there is no guarantee
     * that the history of checkpoints is consistent.
     */

    @Override
    public void recover() throws Exception {
        LOG.info("Recovering checkpoints.");

        // Get all there is first
        List<File> checkpointfiles = checkpointStorage.getFiles();
        LOG.info("Get " + checkpointfiles.size() + " checkpoint files");
        Collections.sort(checkpointfiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                long n1 = extractNumber(o1.getName());
                long n2 = extractNumber(o2.getName());
                return (int) (n1 - n2);
            }

            private long extractNumber(String name) {
                long nmbr = 0;
                try {
                    int s = name.indexOf('_') + 1;
                    int e = name.lastIndexOf('.');
                    String number = name.substring(s, e);
                    nmbr = Long.parseLong(number);
                } catch (Exception e) { }
                return nmbr;
            }
        });

        int numberOfInitialCheckpoints = checkpointfiles.size();
        LOG.info("Found {} checkpoints.", numberOfInitialCheckpoints);

        // Clear internal queue first and store existing files
        completedCheckpoints.clear();
        if(checkpointfiles != null) {
            for (File f : checkpointfiles) {
                completedCheckpoints.addLast(f);
            }
        }
    }

    /**
     * Synchronously writes the new checkpoints to ZooKeeper and asynchronously removes older ones.
     *
     * @param checkpoint Completed checkpoint to add.
     */

    @Override
    public void addCheckpoint(final CompletedCheckpoint checkpoint) throws Exception {
        checkNotNull(checkpoint, "Checkpoint");

        // Now add the new one. If it fails, we don't want to loose existing data.
        Long id = checkpoint.getCheckpointID();
        LOG.info("Adding checkpoint " + id);
        try {
           File file = checkpointStorage.store(checkpoint, "chk_" + id.toString());
           completedCheckpoints.addLast(file);

           // Everything worked, let's remove a previous checkpoint if necessary.
           while (completedCheckpoints.size() > maxNumberOfCheckpointsToRetain) {
               final File completedCheckpoint = completedCheckpoints.removeFirst();
               completedCheckpoint.delete();
           }
        }
        catch(Throwable t){
            LOG.warn("Could not discard completed checkpoint {}.", checkpoint.getCheckpointID(), t);
        }

        LOG.debug("Added {}.", checkpoint);
    }

    /**
     * Get checkpoint from File.
     *
     * @param file  File where checkpoint is stored.
     */

    private CompletedCheckpoint checkpointFromFile(File file){
        try {
            FSDataInputStream inputStream = checkpointStorage.getInputStream(file);
            CompletedCheckpoint chk = InstantiationUtil.deserializeObject(inputStream, ClassLoader.getSystemClassLoader());
            inputStream.close();
            LOG.info("Restored checkpoint from file " + file.getPath());
            return chk;
        }
        catch (Throwable t){
            LOG.warn("Could not read checkpoint {}.", file.getName(), t);
            return null;
        }
    }

    /**
     * Get latest checkpoint.
	 * This method is removed in 1.9, but is used in 1.8
     *
     */

    public CompletedCheckpoint getLatestCheckpoint() {
        if (completedCheckpoints.isEmpty()) {
            return null;
        }
        else {
            return checkpointFromFile(completedCheckpoints.peekLast());
        }
    }

    /**
     * Get all available checkpoints.
     */

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
        List<File> files = new ArrayList<>(completedCheckpoints);
        List<CompletedCheckpoint> checkpoints = new ArrayList<>();
        for(File f : files) {
            checkpoints.add(checkpointFromFile(f));
        }
        return checkpoints;
    }

    /**
     * Get amount of available checkpoints.
     */

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return completedCheckpoints.size();
    }

    /**
     * Get amount of checkpoints to keep.
     */

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return maxNumberOfCheckpointsToRetain;
    }

    /**
     * Shutdown
     */

    @Override
    public void shutdown(JobStatus jobStatus) throws Exception {
        if (jobStatus.isGloballyTerminalState()) {
            LOG.info("Shutting down");
            checkpointStorage.cleanup();
            completedCheckpoints.clear();
        } else {
            LOG.info("Suspending");

            // Clear the local handles, but don't remove any state
            completedCheckpoints.clear();
        }
    }
}
