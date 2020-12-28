/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.deployment.application.DetachedApplicationRunner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Test setup for all jar-submission related handlers. */
public class JarHandlers {

    final JarUploadHandler uploadHandler;
    final JarListHandler listHandler;
    final JarPlanHandler planHandler;
    final JarRunHandler runHandler;
    final JarDeleteHandler deleteHandler;

    JarHandlers(final Path jarDir, final TestingDispatcherGateway restfulGateway) {
        final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever =
                () -> CompletableFuture.completedFuture(restfulGateway);
        final Time timeout = Time.seconds(10);
        final Map<String, String> responseHeaders = Collections.emptyMap();
        final Executor executor = TestingUtils.defaultExecutor();

        uploadHandler =
                new JarUploadHandler(
                        gatewayRetriever,
                        timeout,
                        responseHeaders,
                        JarUploadHeaders.getInstance(),
                        jarDir,
                        executor);

        listHandler =
                new JarListHandler(
                        gatewayRetriever,
                        timeout,
                        responseHeaders,
                        JarListHeaders.getInstance(),
                        CompletableFuture.completedFuture("shazam://localhost:12345"),
                        jarDir.toFile(),
                        new Configuration(),
                        executor);

        planHandler =
                new JarPlanHandler(
                        gatewayRetriever,
                        timeout,
                        responseHeaders,
                        JarPlanGetHeaders.getInstance(),
                        jarDir,
                        new Configuration(),
                        executor);

        runHandler =
                new JarRunHandler(
                        gatewayRetriever,
                        timeout,
                        responseHeaders,
                        JarRunHeaders.getInstance(),
                        jarDir,
                        new Configuration(),
                        executor,
                        () -> new DetachedApplicationRunner(true));

        deleteHandler =
                new JarDeleteHandler(
                        gatewayRetriever,
                        timeout,
                        responseHeaders,
                        JarDeleteHeaders.getInstance(),
                        jarDir,
                        executor);
    }

    public static String uploadJar(
            JarUploadHandler handler, Path jar, RestfulGateway restfulGateway) throws Exception {
        HandlerRequest<EmptyRequestBody, EmptyMessageParameters> uploadRequest =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        EmptyMessageParameters.getInstance(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.singletonList(jar.toFile()));
        final JarUploadResponseBody uploadResponse =
                handler.handleRequest(uploadRequest, restfulGateway).get();
        return uploadResponse.getFilename();
    }

    public static JarListInfo listJars(JarListHandler handler, RestfulGateway restfulGateway)
            throws Exception {
        HandlerRequest<EmptyRequestBody, EmptyMessageParameters> listRequest =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(), EmptyMessageParameters.getInstance());
        return handler.handleRequest(listRequest, restfulGateway).get();
    }

    public static JobPlanInfo showPlan(
            JarPlanHandler handler, String jarName, RestfulGateway restfulGateway)
            throws Exception {
        JarPlanMessageParameters planParameters =
                JarPlanGetHeaders.getInstance().getUnresolvedMessageParameters();
        HandlerRequest<JarPlanRequestBody, JarPlanMessageParameters> planRequest =
                new HandlerRequest<>(
                        new JarPlanRequestBody(),
                        planParameters,
                        Collections.singletonMap(
                                planParameters.jarIdPathParameter.getKey(), jarName),
                        Collections.emptyMap(),
                        Collections.emptyList());
        return handler.handleRequest(planRequest, restfulGateway).get();
    }

    public static JarRunResponseBody runJar(
            JarRunHandler handler, String jarName, DispatcherGateway restfulGateway)
            throws Exception {
        final JarRunMessageParameters runParameters =
                JarRunHeaders.getInstance().getUnresolvedMessageParameters();
        HandlerRequest<JarRunRequestBody, JarRunMessageParameters> runRequest =
                new HandlerRequest<>(
                        new JarRunRequestBody(),
                        runParameters,
                        Collections.singletonMap(
                                runParameters.jarIdPathParameter.getKey(), jarName),
                        Collections.emptyMap(),
                        Collections.emptyList());
        return handler.handleRequest(runRequest, restfulGateway).get();
    }

    public static void deleteJar(
            JarDeleteHandler handler, String jarName, RestfulGateway restfulGateway)
            throws Exception {
        JarDeleteMessageParameters deleteParameters =
                JarDeleteHeaders.getInstance().getUnresolvedMessageParameters();
        HandlerRequest<EmptyRequestBody, JarDeleteMessageParameters> deleteRequest =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(),
                        deleteParameters,
                        Collections.singletonMap(
                                deleteParameters.jarIdPathParameter.getKey(), jarName),
                        Collections.emptyMap(),
                        Collections.emptyList());
        handler.handleRequest(deleteRequest, restfulGateway).get();
    }
}
