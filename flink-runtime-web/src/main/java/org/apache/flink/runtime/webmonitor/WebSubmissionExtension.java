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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarListHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarListHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHeaders;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Container for the web submission handlers.
 */
public class WebSubmissionExtension implements WebMonitorExtension {

	private final ArrayList<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> webSubmissionHandlers;

	public WebSubmissionExtension(
			Configuration configuration,
			CompletableFuture<String> restAddressFuture,
			GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
			Map<String, String> responseHeaders,
			Path jarDir,
			Executor executor,
			Time timeout) throws Exception {

		webSubmissionHandlers = new ArrayList<>(5);

		final JarUploadHandler jarUploadHandler = new JarUploadHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JarUploadHeaders.getInstance(),
			jarDir,
			executor);

		final JarListHandler jarListHandler = new JarListHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JarListHeaders.getInstance(),
			jarDir.toFile(),
			executor);

		final JarRunHandler jarRunHandler = new JarRunHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JarRunHeaders.getInstance(),
			jarDir,
			configuration,
			executor);

		final JarDeleteHandler jarDeleteHandler = new JarDeleteHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JarDeleteHeaders.getInstance(),
			jarDir,
			executor);

		final JarPlanHandler jarPlanHandler = new JarPlanHandler(
			restAddressFuture,
			leaderRetriever,
			timeout,
			responseHeaders,
			JarPlanHeaders.getInstance(),
			jarDir,
			configuration,
			executor
		);

		webSubmissionHandlers.add(Tuple2.of(JarUploadHeaders.getInstance(), jarUploadHandler));
		webSubmissionHandlers.add(Tuple2.of(JarListHeaders.getInstance(), jarListHandler));
		webSubmissionHandlers.add(Tuple2.of(JarRunHeaders.getInstance(), jarRunHandler));
		webSubmissionHandlers.add(Tuple2.of(JarDeleteHeaders.getInstance(), jarDeleteHandler));
		webSubmissionHandlers.add(Tuple2.of(JarPlanHeaders.getInstance(), jarPlanHandler));
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> getHandlers() {
		return webSubmissionHandlers;
	}
}
