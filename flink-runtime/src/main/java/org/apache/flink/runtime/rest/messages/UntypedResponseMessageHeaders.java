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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;

/**
 * Message headers for a web handler request.
 *
 * @param <R> type of the request
 * @param <M> type of the message message parameters
 */
public interface UntypedResponseMessageHeaders<R extends RequestBody, M extends MessageParameters>
        extends RestHandlerSpecification {
    /**
     * Returns the class of the request message.
     *
     * @return class of the request message
     */
    Class<R> getRequestClass();

    /**
     * Returns a new {@link MessageParameters} object.
     *
     * @return new message parameters object
     */
    M getUnresolvedMessageParameters();

    /**
     * Returns whether this header allows file uploads.
     *
     * @return whether this header allows file uploads
     */
    default boolean acceptsFileUploads() {
        return false;
    }
}
