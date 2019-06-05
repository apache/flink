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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.highavailability.filesystem.FileSystemStorageHelper;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SubmittedJobGraph} instances for jobs running in {@link HighAvailabilityMode#FILESYSTEM}.
 *
 * This implementation support maintaing job graphs across the cluster. It stores
 * job graphs on a file system and therefore relies on the high availability of the file system.
 * It also effectively maintains the list of running jobs.
 */
public class FileSystemSubmittedJobGraphStore implements SubmittedJobGraphStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemSubmittedJobGraphStore.class);

    /** Lock to synchronize with the {@link SubmittedJobGraphListener}. */
    private final Object cacheLock = new Object();

    /** Map of current graph files */
    Map<String, File> graphs = new HashMap<>();

    /** Storage helper. */
    private final FileSystemStorageHelper<SubmittedJobGraph> stateStorage;

    /** Flag indicating whether this instance is running. */
    private boolean isRunning;

    /**
     * FileSystemSubmittedJobGraphStore constructor backed by JobGraphStore
     *
     * @param stateStorage storage implementation for current job graphs
     * @throws Exception
     */
    public FileSystemSubmittedJobGraphStore(FileSystemStorageHelper<SubmittedJobGraph> stateStorage) throws Exception {

        checkNotNull(stateStorage, "State storage");

        this.stateStorage = stateStorage;
    }

    /**
     * Start FileSystemSubmittedJobGraphStore
     *
     * @param jobGraphListener  listener for job graphs
     * @throws Exception
     */

    @Override
    public void start(SubmittedJobGraphListener jobGraphListener) throws Exception {
        synchronized (cacheLock) {
            if (!isRunning) {
                isRunning = true;
            }
        }
    }

    /**
     * Stop FileSystemSubmittedJobGraphStore
     *
     * @throws Exception
     */

    @Override
    public void stop() {
        synchronized (cacheLock) {
            if (isRunning) {
                isRunning = false;
            }
        }
    }

    /**
     * Save job graph
     *
     * @param jobGraph      job graph to save
     * @throws Exception
     */

    @Override
    public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
        checkNotNull(jobGraph, "Job graph");
        String hexJobId = jobGraph.getJobId().toHexString();

        synchronized (cacheLock) {
            verifyIsRunning();

            try {
                if(graphs.containsKey(hexJobId)) {
                    graphs.get(hexJobId).delete();
                }
                File file = stateStorage.store(jobGraph, hexJobId, "jobgraph");
                graphs.put(hexJobId, file);
            } catch (Exception e) {
                throw new RuntimeException("Storing job graph " + hexJobId + " failed", e);
            }
        }

        LOG.info("Added job graph {}.", jobGraph);
    }

    /**
     * Removes the {@link SubmittedJobGraph} with the given {@link JobID} if it exists.
     *
     * @param jobId      job ID
     * @throws Exception
     */

    @Override
    public void removeJobGraph(JobID jobId) throws Exception {
        checkNotNull(jobId, "Job ID");
        String hexJobId = jobId.toHexString();

        synchronized (cacheLock) {
            try {
                graphs.remove(hexJobId);
                stateStorage.removeJobId(hexJobId);

            } catch (Exception e) {
                LOG.info("Removing job graph " + jobId + " failed.", e);
            }
        }

        LOG.info("Removed job graph {} .", jobId);
    }

    /**
     * Releases the locks on the graph for specified {@link JobID}.
     *
     * Releasing the locks allows that another instance can delete the job from
     * the {@link SubmittedJobGraphStore}.
     *
     * @param jobId specifying the job to release the locks for
     * @throws Exception if the locks cannot be released
    */

    @Override
    public void releaseJobGraph(JobID jobId) throws Exception {
        // Nothing to do here
    }

    /**
     * Get list of existing job ids
     *
     * @return Collection<JobID>
     * @throws Exception
     */

    @Override
    public Collection<JobID> getJobIds() throws Exception {
        List<String> ids = stateStorage.getJobIDs();
        List<JobID> jobIds = new ArrayList<>(ids.size());
        for(String id : ids) {
            jobIds.add(JobID.fromHexString(id));
        }
        LOG.info("Retrieving all stored job ids. Found " + jobIds.size());
        return jobIds;
    }

    /**
     * Recover job graph
     *
     * @param  jobId                Job ID
     * @return JobGraph
     * @throws Exception
     */

    @Override
    public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
        checkNotNull(jobId, "Job ID");
        String hexJobId = jobId.toHexString();

        synchronized (cacheLock) {
            verifyIsRunning();

            List<File> grFiles = stateStorage.getFilesPerJob(hexJobId);
            if (grFiles.size() == 0) {
                return null;
            }
            try {
                FSDataInputStream inputStream = stateStorage.getInputStream(grFiles.get(0));
                SubmittedJobGraph jobGraph = InstantiationUtil.deserializeObject(inputStream, ClassLoader.getSystemClassLoader());
                inputStream.close();
                graphs.put(hexJobId, grFiles.get(0));

                LOG.info("Recovered {}.", hexJobId);

                return jobGraph;
            }
            catch(Throwable t){
                LOG.info("Error Recovered grapth for job ID " + hexJobId + " error " + t);
                return null;
            }
        }
    }

    /**
     * Verifies that the state is running.
     */
    private void verifyIsRunning() {
        checkState(isRunning, "Not running. Forgot to call start()?");
    }
}