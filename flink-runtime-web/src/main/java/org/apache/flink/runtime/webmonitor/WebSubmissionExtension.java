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
import org.apache.flink.runtime.webmonitor.handlers.ArtifactDeleteHandler;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactDeleteHeaders;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactListHandler;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactListHeaders;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactPlanGetHeaders;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactPlanPostHeaders;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactRunHandler;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactUploadHandler;
import org.apache.flink.runtime.webmonitor.handlers.ArtifactUploadHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarListHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarListHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanGetHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanPostHeaders;
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
			GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
			Map<String, String> responseHeaders,
			CompletableFuture<String> localAddressFuture,
			Path artifactDir,
			Executor executor,
			Time timeout) throws Exception {

		webSubmissionHandlers = new ArrayList<>(12);

		final JarUploadHandler jarUploadHandler = new JarUploadHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			JarUploadHeaders.getInstance(),
			artifactDir,
			executor);

		final JarListHandler jarListHandler = new JarListHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			JarListHeaders.getInstance(),
			localAddressFuture,
			artifactDir.toFile(),
			executor);

		final JarRunHandler jarRunHandler = new JarRunHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			JarRunHeaders.getInstance(),
			artifactDir,
			configuration,
			executor);

		final JarDeleteHandler jarDeleteHandler = new JarDeleteHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			JarDeleteHeaders.getInstance(),
			artifactDir,
			executor);

		final JarPlanHandler jarPlanHandler = new JarPlanHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			JarPlanGetHeaders.getInstance(),
			artifactDir,
			configuration,
			executor
		);

		final JarPlanHandler postJarPlanHandler = new JarPlanHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			JarPlanPostHeaders.getInstance(),
			artifactDir,
			configuration,
			executor
		);

		final ArtifactUploadHandler artifactUploadHandler = new ArtifactUploadHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			ArtifactUploadHeaders.getInstance(),
			artifactDir,
			executor);

		final ArtifactListHandler artifactListHandler = new ArtifactListHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			ArtifactListHeaders.getInstance(),
			localAddressFuture,
			artifactDir.toFile(),
			executor);

		final ArtifactRunHandler artifactRunHandler = new ArtifactRunHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			ArtifactRunHeaders.getInstance(),
			artifactDir,
			configuration,
			executor);

		final ArtifactDeleteHandler artifactDeleteHandler = new ArtifactDeleteHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			ArtifactDeleteHeaders.getInstance(),
			artifactDir,
			executor);

		final ArtifactPlanHandler artifactPlanHandler = new ArtifactPlanHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			ArtifactPlanGetHeaders.getInstance(),
			artifactDir,
			configuration,
			executor
		);

		final ArtifactPlanHandler postArtifactPlanHandler = new ArtifactPlanHandler(
			leaderRetriever,
			timeout,
			responseHeaders,
			ArtifactPlanPostHeaders.getInstance(),
			artifactDir,
			configuration,
			executor
		);

		webSubmissionHandlers.add(Tuple2.of(JarUploadHeaders.getInstance(), jarUploadHandler));
		webSubmissionHandlers.add(Tuple2.of(JarListHeaders.getInstance(), jarListHandler));
		webSubmissionHandlers.add(Tuple2.of(JarRunHeaders.getInstance(), jarRunHandler));
		webSubmissionHandlers.add(Tuple2.of(JarDeleteHeaders.getInstance(), jarDeleteHandler));
		webSubmissionHandlers.add(Tuple2.of(JarPlanGetHeaders.getInstance(), jarPlanHandler));
		webSubmissionHandlers.add(Tuple2.of(JarPlanGetHeaders.getInstance(), postJarPlanHandler));

		webSubmissionHandlers.add(Tuple2.of(ArtifactUploadHeaders.getInstance(), artifactUploadHandler));
		webSubmissionHandlers.add(Tuple2.of(ArtifactListHeaders.getInstance(), artifactListHandler));
		webSubmissionHandlers.add(Tuple2.of(ArtifactRunHeaders.getInstance(), artifactRunHandler));
		webSubmissionHandlers.add(Tuple2.of(ArtifactDeleteHeaders.getInstance(), artifactDeleteHandler));
		webSubmissionHandlers.add(Tuple2.of(ArtifactPlanGetHeaders.getInstance(), artifactPlanHandler));
		webSubmissionHandlers.add(Tuple2.of(ArtifactPlanGetHeaders.getInstance(), postArtifactPlanHandler));
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
