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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.client.deployment.application.PackagedProgramApplication;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.handlers.utils.JarHandlerUtils.JarHandlerContext;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.fromRequestBodyOrQueryParameter;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.getQueryParameter;
import static org.apache.flink.shaded.guava33.com.google.common.base.Strings.emptyToNull;

/** Handler to submit applications uploaded via the Web UI. */
public class JarRunApplicationHandler
        extends AbstractRestHandler<
                DispatcherGateway,
                JarRunApplicationRequestBody,
                JarRunApplicationResponseBody,
                JarRunApplicationMessageParameters> {

    private final Path jarDir;

    private final Configuration configuration;

    public JarRunApplicationHandler(
            final GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            final Duration timeout,
            final Map<String, String> responseHeaders,
            final MessageHeaders<
                            JarRunApplicationRequestBody,
                            JarRunApplicationResponseBody,
                            JarRunApplicationMessageParameters>
                    messageHeaders,
            final Path jarDir,
            final Configuration configuration) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);

        this.jarDir = requireNonNull(jarDir);
        this.configuration = requireNonNull(configuration);
    }

    @Override
    @VisibleForTesting
    public CompletableFuture<JarRunApplicationResponseBody> handleRequest(
            @Nonnull final HandlerRequest<JarRunApplicationRequestBody> request,
            @Nonnull final DispatcherGateway gateway)
            throws RestHandlerException {

        final Configuration effectiveConfiguration = new Configuration(configuration);
        effectiveConfiguration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);

        final JarHandlerContext context = JarHandlerContext.fromRequest(request, jarDir, log);
        context.applyToConfiguration(effectiveConfiguration, request);
        SavepointRestoreSettings.toConfiguration(
                getSavepointRestoreSettings(request, effectiveConfiguration),
                effectiveConfiguration);

        final PackagedProgram program = context.toPackagedProgram(effectiveConfiguration);

        ApplicationID applicationId = context.getApplicationId().orElse(ApplicationID.generate());
        PackagedProgramApplication application =
                new PackagedProgramApplication(
                        applicationId, program, effectiveConfiguration, false, true, false, false);

        return gateway.submitApplication(application, timeout)
                .handle(
                        (acknowledge, throwable) -> {
                            if (throwable != null) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "Could not submit application.",
                                                HttpResponseStatus.BAD_REQUEST,
                                                throwable));
                            }
                            return new JarRunApplicationResponseBody(applicationId);
                        });
    }

    private SavepointRestoreSettings getSavepointRestoreSettings(
            final @Nonnull HandlerRequest<JarRunApplicationRequestBody> request,
            final Configuration effectiveConfiguration)
            throws RestHandlerException {

        final JarRunApplicationRequestBody requestBody = request.getRequestBody();

        final boolean allowNonRestoredState =
                fromRequestBodyOrQueryParameter(
                        requestBody.getAllowNonRestoredState().orElse(null),
                        () -> getQueryParameter(request, AllowNonRestoredStateQueryParameter.class),
                        effectiveConfiguration.get(
                                StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE),
                        log);
        final String savepointPath =
                fromRequestBodyOrQueryParameter(
                        emptyToNull(requestBody.getSavepointPath().orElse(null)),
                        () ->
                                emptyToNull(
                                        getQueryParameter(
                                                request, SavepointPathQueryParameter.class)),
                        effectiveConfiguration.get(StateRecoveryOptions.SAVEPOINT_PATH),
                        log);
        final RecoveryClaimMode recoveryClaimMode =
                requestBody
                        .getRecoveryClaimMode()
                        .orElse(effectiveConfiguration.get(StateRecoveryOptions.RESTORE_MODE));
        if (recoveryClaimMode.equals(RecoveryClaimMode.LEGACY)) {
            log.warn(
                    "The {} restore mode is deprecated, please use {} or {} mode instead.",
                    RecoveryClaimMode.LEGACY,
                    RecoveryClaimMode.CLAIM,
                    RecoveryClaimMode.NO_CLAIM);
        }
        final SavepointRestoreSettings savepointRestoreSettings;
        if (savepointPath != null) {
            savepointRestoreSettings =
                    SavepointRestoreSettings.forPath(
                            savepointPath, allowNonRestoredState, recoveryClaimMode);
        } else {
            savepointRestoreSettings = SavepointRestoreSettings.none();
        }
        return savepointRestoreSettings;
    }
}
