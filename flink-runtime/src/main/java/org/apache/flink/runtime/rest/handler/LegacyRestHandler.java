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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import java.util.concurrent.CompletableFuture;

/**
 * Interface which Flink's legacy REST handler have to implement in order to be usable
 * via the {@link LegacyRestHandlerAdapter}.
 *
 * @param <T> type of the gateway
 * @param <R> type of the REST response
 */
public interface LegacyRestHandler<T extends RestfulGateway, R extends ResponseBody, M extends MessageParameters> {

	CompletableFuture<R> handleRequest(HandlerRequest<EmptyRequestBody, M> request, T gateway);
}
