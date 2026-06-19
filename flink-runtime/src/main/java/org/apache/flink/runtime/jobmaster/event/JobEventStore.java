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

/** A store for recording the {@link JobEvent}. */
public interface JobEventStore {

    /** Start the store. This method should be called before any other operations. */
    void start() throws Exception;

    /**
     * Stop the store.
     *
     * @param clearEventLogs Whether to clear the job events that have been recorded in the store.
     */
    void stop(boolean clearEventLogs);

    /**
     * Write a job event.
     *
     * @param jobEvent The job event that will be recorded.
     * @param cutBlock If set to true, the current output file will be closed after writing this
     *     event, and a new output file will be created for subsequent events. This parameter
     *     effectively controls the segmentation of event data into separate files.
     */
    void writeEvent(JobEvent jobEvent, boolean cutBlock);

    /**
     * Read a job event.
     *
     * @return job event.
     */
    JobEvent readEvent() throws Exception;

    /**
     * Returns whether the store is empty.
     *
     * @return false if the store contains any job events, true otherwise.
     */
    boolean isEmpty() throws Exception;
}
