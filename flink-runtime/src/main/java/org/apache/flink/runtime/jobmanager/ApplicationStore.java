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

import org.apache.flink.api.common.ApplicationID;

import java.util.Collection;
import java.util.Optional;

/** {@link ApplicationStoreEntry} instances for recovery. */
public interface ApplicationStore extends ApplicationWriter {

    /** Starts the {@link ApplicationStore} service. */
    void start() throws Exception;

    /** Stops the {@link ApplicationStore} service. */
    void stop() throws Exception;

    /**
     * Returns the {@link ApplicationStoreEntry} with the given {@link ApplicationID} or {@link
     * Optional#empty()} if no application was registered.
     */
    Optional<ApplicationStoreEntry> recoverApplication(ApplicationID applicationId)
            throws Exception;

    /**
     * Get all application ids of submitted applications to the submitted application store.
     *
     * @return Collection of submitted application ids
     * @throws Exception if the operation fails
     */
    Collection<ApplicationID> getApplicationIds() throws Exception;
}
