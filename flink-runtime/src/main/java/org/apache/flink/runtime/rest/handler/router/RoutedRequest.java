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

package org.apache.flink.runtime.rest.handler.router;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCounted;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class for handling {@link HttpRequest} with associated {@link RouteResult}.
 */
public class RoutedRequest<T> implements ReferenceCounted {
	private final RouteResult<T> result;
	private final HttpRequest request;

	private final Optional<ReferenceCounted> requestAsReferenceCounted;
	private final QueryStringDecoder queryStringDecoder;

	public RoutedRequest(RouteResult<T> result, HttpRequest request) {
		this.result = checkNotNull(result);
		this.request = checkNotNull(request);
		this.requestAsReferenceCounted = Optional.ofNullable((request instanceof ReferenceCounted) ? (ReferenceCounted) request : null);
		this.queryStringDecoder = new QueryStringDecoder(request.uri());
	}

	public RouteResult<T> getRouteResult() {
		return result;
	}

	public HttpRequest getRequest() {
		return request;
	}

	public String getPath() {
		return queryStringDecoder.path();
	}

	@Override
	public int refCnt() {
		return requestAsReferenceCounted.map(ReferenceCounted::refCnt).orElse(0);
	}

	@Override
	public boolean release() {
		return requestAsReferenceCounted.map(ReferenceCounted::release).orElse(true);
	}

	@Override
	public boolean release(int arg0) {
		return requestAsReferenceCounted.map(referenceCounted -> referenceCounted.release(arg0)).orElse(true);
	}

	@Override
	public ReferenceCounted retain() {
		requestAsReferenceCounted.ifPresent(ReferenceCounted::retain);
		return this;
	}

	@Override
	public ReferenceCounted retain(int arg0) {
		requestAsReferenceCounted.ifPresent(referenceCounted -> referenceCounted.retain(arg0));
		return this;
	}

	@Override
	public ReferenceCounted touch() {
		requestAsReferenceCounted.ifPresent(ReferenceCountUtil::touch);
		return this;
	}

	@Override
	public ReferenceCounted touch(Object hint) {
		requestAsReferenceCounted.ifPresent(referenceCounted -> ReferenceCountUtil.touch(referenceCounted, hint));
		return this;
	}
}
