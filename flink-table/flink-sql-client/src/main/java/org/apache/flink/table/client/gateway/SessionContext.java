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

package org.apache.flink.table.client.gateway;

import org.apache.flink.table.client.config.Environment;

import java.util.Objects;

/**
 * Context describing a session, it's mainly used for user to open a new session in the backend. If
 * client request to open a new session, the backend {@link Executor} will maintain a {@link
 * org.apache.flink.table.client.gateway.local.ExecutionContext} for it, and each interaction the
 * client need to attach the session id.
 */
public class SessionContext {

    private final String sessionId;
    private final Environment sessionEnv;

    public SessionContext(String sessionId, Environment sessionEnv) {
        this.sessionId = sessionId;
        this.sessionEnv = sessionEnv;
    }

    public String getSessionId() {
        return this.sessionId;
    }

    public Environment getSessionEnv() {
        return this.sessionEnv;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SessionContext)) {
            return false;
        }
        SessionContext context = (SessionContext) o;
        return Objects.equals(sessionId, context.sessionId)
                && Objects.equals(sessionEnv, context.sessionEnv);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, sessionEnv);
    }
}
