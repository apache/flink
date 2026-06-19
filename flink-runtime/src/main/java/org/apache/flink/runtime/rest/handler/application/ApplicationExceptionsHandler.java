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

package org.apache.flink.runtime.rest.handler.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.ApplicationExceptionsInfoWithHistory;
import org.apache.flink.runtime.rest.messages.ApplicationIDPathParameter;
import org.apache.flink.runtime.rest.messages.ApplicationMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ApplicationJsonArchivist;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler returning the exception history for the specified application. */
public class ApplicationExceptionsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                ApplicationExceptionsInfoWithHistory,
                ApplicationMessageParameters>
        implements ApplicationJsonArchivist {

    public ApplicationExceptionsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            EmptyRequestBody,
                            ApplicationExceptionsInfoWithHistory,
                            ApplicationMessageParameters>
                    messageHeaders) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
    }

    @Override
    public CompletableFuture<ApplicationExceptionsInfoWithHistory> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway) {
        ApplicationID applicationId = request.getPathParameter(ApplicationIDPathParameter.class);

        return gateway.requestApplication(applicationId, timeout)
                .thenApply(
                        archivedApplication ->
                                ApplicationExceptionsInfoWithHistory
                                        .fromApplicationExceptionHistory(
                                                archivedApplication.getExceptionHistory()));
    }

    @Override
    public Collection<ArchivedJson> archiveApplicationWithPath(
            ArchivedApplication archivedApplication) throws IOException {
        String path =
                getMessageHeaders()
                        .getTargetRestEndpointURL()
                        .replace(
                                ':' + ApplicationIDPathParameter.KEY,
                                archivedApplication.getApplicationId().toHexString());
        return Collections.singleton(
                new ArchivedJson(
                        path,
                        ApplicationExceptionsInfoWithHistory.fromApplicationExceptionHistory(
                                archivedApplication.getExceptionHistory())));
    }
}
