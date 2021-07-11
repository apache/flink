/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.exceptions;

/**
 * Exception which indicates that the specified recipient is unreachable. This can either mean that
 * the recipient has been terminated or that the remote RpcService is currently not reachable.
 */
public class RecipientUnreachableException extends RpcException {
    public RecipientUnreachableException(String sender, String recipient, String message) {
        super(
                String.format(
                        "Could not send message [%s] from sender [%s] to recipient [%s], because "
                                + "the recipient is unreachable. This can either mean that the "
                                + "recipient has been terminated or that the remote RpcService "
                                + "is currently not reachable.",
                        message, sender, recipient));
    }
}
