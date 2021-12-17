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

package org.apache.flink.runtime.rest.handler.async;

import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;

import java.util.Collection;
import java.util.Collections;

/**
 * Message headers for the status polling of an asynchronous operation.
 *
 * @param <V> type of the operation result
 * @param <M> type of the message parameters
 */
public abstract class AsynchronousOperationStatusMessageHeaders<V, M extends MessageParameters>
        implements MessageHeaders<EmptyRequestBody, AsynchronousOperationResult<V>, M> {

    /**
     * Returns the class of the value wrapped in the {@link AsynchronousOperationResult}.
     *
     * @return value class
     */
    public abstract Class<V> getValueClass();

    @Override
    public Class<AsynchronousOperationResult<V>> getResponseClass() {
        return (Class<AsynchronousOperationResult<V>>) (Class<?>) AsynchronousOperationResult.class;
    }

    @Override
    public Collection<Class<?>> getResponseTypeParameters() {
        return Collections.singleton(getValueClass());
    }
}
