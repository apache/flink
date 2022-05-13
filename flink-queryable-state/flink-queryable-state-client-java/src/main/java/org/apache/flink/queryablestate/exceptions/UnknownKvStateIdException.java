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

package org.apache.flink.queryablestate.exceptions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.network.BadRequestException;
import org.apache.flink.util.Preconditions;

/** Thrown if no KvState with the given ID cannot found by the server handler. */
@Internal
public class UnknownKvStateIdException extends BadRequestException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param serverName the name of the server that threw the exception.
     * @param kvStateId the state id for which no state was found.
     */
    public UnknownKvStateIdException(String serverName, KvStateID kvStateId) {
        super(
                serverName,
                "No registered state with ID " + Preconditions.checkNotNull(kvStateId) + '.');
    }
}
