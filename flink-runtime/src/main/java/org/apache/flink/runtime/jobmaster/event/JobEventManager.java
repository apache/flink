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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The manager for recording and replaying {@link JobEvent}. */
public class JobEventManager {

    private static final Logger LOG = LoggerFactory.getLogger(JobEventManager.class);

    private final JobEventStore jobEventStore;

    private boolean replaying = false;

    private boolean running = false;

    public JobEventManager(JobEventStore store) {
        this.jobEventStore = checkNotNull(store);
    }

    /** Start the job event manager. */
    public void start() throws Exception {
        if (!running) {
            jobEventStore.start();
            running = true;
        }
    }

    /**
     * Stop the job event manager.
     *
     * <p>NOTE: This method maybe invoked multiply times.
     */
    public void stop(boolean clear) {
        if (running) {
            jobEventStore.stop(clear);
            running = false;
        }
    }

    /**
     * Write a job event asynchronously.
     *
     * @param event The job event that will be recorded.
     * @param cutBlock whether start a new event block after write this event.
     */
    public void writeEvent(JobEvent event, boolean cutBlock) {
        checkState(running);
        jobEventStore.writeEvent(event, cutBlock);
    }

    /**
     * Replay all job events that have been record.
     *
     * @param replayHandler handler which will process the job event.
     * @return <code>true</code> if replay successfully, <code>false</code> otherwise.
     */
    public boolean replay(JobEventReplayHandler replayHandler) {
        checkState(running);
        try {
            replaying = true;
            replayHandler.startReplay();

            JobEvent event;
            while ((event = jobEventStore.readEvent()) != null) {
                replayHandler.replayOneEvent(event);
            }

            replayHandler.finalizeReplay();
        } catch (Throwable throwable) {
            LOG.warn("Replay job event failed.", throwable);
            return false;
        } finally {
            replaying = false;
        }

        return true;
    }

    /**
     * Returns whether the store is empty.
     *
     * @return false if the store contains any job events, true otherwise.
     */
    public boolean hasJobEvents() throws Exception {
        return !jobEventStore.isEmpty();
    }

    @VisibleForTesting
    boolean isRunning() {
        return running;
    }
}
