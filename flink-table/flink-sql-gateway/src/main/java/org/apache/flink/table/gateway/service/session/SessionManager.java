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

package org.apache.flink.table.gateway.service.session;

import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.context.DefaultContext;

/** Mange the lifecycle of the {@link Session}. */
public interface SessionManager {

    /** Create the {@link SessionManager} with the default configuration. */
    static SessionManager create(DefaultContext defaultContext) {
        return new SessionManagerImpl(defaultContext);
    }

    /** Start the {@link SessionManager} and do the required initialization. */
    void start();

    /** Destroy the {@link SessionManager} and releases used resources. */
    void stop();

    /**
     * Get the {@link Session} with the identifier.
     *
     * @param sessionHandle identifier of the session.
     * @return registered session.
     */
    Session getSession(SessionHandle sessionHandle) throws SqlGatewayException;

    /**
     * Register a new {@link Session}.
     *
     * @param environment the initialization environment.
     * @return created session.
     */
    Session openSession(SessionEnvironment environment) throws SqlGatewayException;

    /**
     * Close the session with the specified identifier and releases the resources used by the
     * session.
     *
     * @param sessionHandle the identifier of the session.
     */
    void closeSession(SessionHandle sessionHandle) throws SqlGatewayException;
}
