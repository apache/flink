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

package org.apache.flink.table.gateway.rest.message.session;

import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.table.gateway.api.session.SessionHandle;

import java.util.UUID;

/** {@link MessagePathParameter} that parse the {@link SessionHandle}. */
public class SessionHandleIdPathParameter extends MessagePathParameter<SessionHandle> {

    public static final String KEY = "session_handle";

    public SessionHandleIdPathParameter() {
        super(KEY);
    }

    @Override
    protected SessionHandle convertFromString(String sessionHandleId) {
        return new SessionHandle(UUID.fromString(sessionHandleId));
    }

    @Override
    protected String convertToString(SessionHandle sessionHandle) {
        return sessionHandle.getIdentifier().toString();
    }

    @Override
    public String getDescription() {
        return "The SessionHandle that identifies a session.";
    }
}
