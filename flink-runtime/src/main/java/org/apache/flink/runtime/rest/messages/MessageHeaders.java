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

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/**
 * This class links {@link RequestBody}s to {@link ResponseBody}s types and contains meta-data required for their http headers.
 *
 * <p>Implementations must be state-less.
 *
 * @param <R> request message type
 * @param <P> response message type
 * @param <M> message parameters type
 */
public interface MessageHeaders<R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends UntypedResponseMessageHeaders<R, M> {

	/**
	 * Returns the class of the response message.
	 *
	 * @return class of the response message
	 */
	Class<P> getResponseClass();

	/**
	 * Returns the http status code for the response.
	 *
	 * @return http status code of the response
	 */
	HttpResponseStatus getResponseStatusCode();

	/**
	 * Returns the collection of type parameters for the response type.
	 *
	 * @return Collection of type parameters for the response type
	 */
	default Collection<Class<?>> getResponseTypeParameters() {
		return Collections.emptyList();
	}

	/**
	 * Returns the description for this header.
	 *
	 * @return description for the header
	 */
	String getDescription();
}
