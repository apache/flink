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

package org.apache.flink.runtime.net.http;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCounted;

/**
 * The default {@link RestRequest} implementation.
 */
public class DefaultRestRequest<T> extends DefaultHttpRequest implements RestRequest<T> {

	private final T content;

	private final ReferenceCounted contentAsReferenceCounted;

	public DefaultRestRequest(HttpVersion httpVersion, HttpMethod method, String uri) {
		this(httpVersion, method, uri, null);
	}

	public DefaultRestRequest(HttpVersion httpVersion, HttpMethod method, String uri, T content) {
		this(httpVersion, method, uri, content, true);
	}

	public DefaultRestRequest(HttpVersion httpVersion, HttpMethod method, String uri,
		T content, boolean validateHeaders) {
		super(httpVersion, method, uri, validateHeaders);
		this.content = content;
		contentAsReferenceCounted = (content instanceof ReferenceCounted) ? (ReferenceCounted) content : null;
	}

	@Override
	public T content() {
		return content;
	}

	@Override
	public int refCnt() {
		return (contentAsReferenceCounted == null)? 0 : contentAsReferenceCounted.refCnt();
	}

	@Override
	public boolean release() {
		return (contentAsReferenceCounted == null)? true : contentAsReferenceCounted.release();
	}

	@Override
	public boolean release(int decrement) {
		return (contentAsReferenceCounted == null)? true : contentAsReferenceCounted.release(decrement);
	}

	@Override
	public ReferenceCounted retain() {
		if (contentAsReferenceCounted != null) {
			contentAsReferenceCounted.retain();
		}
		return this;
	}

	@Override
	public ReferenceCounted retain(int increment) {
		if (contentAsReferenceCounted != null) {
			contentAsReferenceCounted.retain(increment);
		}
		return this;
	}
}
