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

package org.apache.flink.runtime.rpc.pekko.exceptions;

import org.apache.flink.runtime.rpc.exceptions.RpcRuntimeException;

/**
 * Exception which indicates that the {@link org.apache.flink.runtime.rpc.pekko.PekkoRpcActor} has
 * received an unknown message type.
 */
public class UnknownMessageException extends RpcRuntimeException {

    private static final long serialVersionUID = 1691338049911020814L;

    public UnknownMessageException(String message) {
        super(message);
    }

    public UnknownMessageException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownMessageException(Throwable cause) {
        super(cause);
    }
}
