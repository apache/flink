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

import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.highavailability.filesystem.FileSystemStorageHelper;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * An implementation of the {@link FileSystemCheckpointIDCounter} using file system.
 *
 * This implementation support maintaing ID for a singele job. It stores
 * checkpoint IDs on a file system and therefore relies on the high
 * availability of the file system.
 *
 */

public class FileSystemCheckpointIDCounter implements CheckpointIDCounter {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCheckpointIDCounter.class);
    private final FileSystemStorageHelper<Integer> counterStorage;

    /* Local counter */
    private long checkpointID;
    private final Object startStopLock = new Object();
    private boolean isStarted;

    /* Previous counter */
    private File lastID = null;

    /**
     * Creates a new FileSystemCheckpointIDCounter class .
     *
     * @param counterStorage    		Underlying storage
     */

    public FileSystemCheckpointIDCounter(FileSystemStorageHelper<Integer> counterStorage) {
        this.counterStorage = counterStorage;
    }

    /**
     * Starts FileSystemCheckpointIDCounter.
     */
    public void start() throws Exception {
        Object var1 = this.startStopLock;
        synchronized(this.startStopLock) {
            List<File> counters = counterStorage.getFiles();
            if(counters.size() == 0){
                checkpointID = 0;
                lastID = null;
            }
            else{
                checkpointID = Long.parseLong(counters.get(0).getName()) + 1;
                lastID = counters.get(0);
            }
            if (!this.isStarted) {
                this.isStarted = true;
            }
        }
    }

    /**
     * Shutsdown FileSystemCheckpointIDCounter.
     */
    public void shutdown(JobStatus jobStatus) throws Exception {
        Object var2 = this.startStopLock;
        synchronized(this.startStopLock) {
            if (this.isStarted) {
                LOG.info("Shutting down.");
                counterStorage.cleanup();
                this.isStarted = false;
            }

        }
    }

    /**
     * Get next counter.
     *
     * @return  counter
     */
    public long getAndIncrement() throws Exception {
        saveAndIncrementCount();
        return checkpointID - 1;
    }

    /**
     * Resets counter.
     *
     * @param newId         New ID
     */
    public void setCount(long newId) throws Exception {
        checkpointID = newId;
        counterStorage.cleanup();
        lastID = counterStorage.store(0, Long.toString(checkpointID));
    }

    /**
     * Internal method to save counter.
     */

    private void saveAndIncrementCount(){
        try {
            File result = counterStorage.store(0, Long.toString(checkpointID));
            if (lastID != null) {
				lastID.delete();
			}
            lastID = result;
        }
        catch (Throwable t){}
        checkpointID ++;
    }
}
