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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ProfilingFileNamePathParameter;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.cluster.ProfilingFileMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.io.File;
import java.util.Map;

/** Rest handler which serves the profiler result file from JobManager. */
public class JobManagerProfilingFileHandler
        extends AbstractJobManagerFileHandler<ProfilingFileMessageParameters> {

    private final String profilingResultDir;

    public JobManagerProfilingFileHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            UntypedResponseMessageHeaders<EmptyRequestBody, ProfilingFileMessageParameters>
                    messageHeaders,
            Configuration configs) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);

        this.profilingResultDir = configs.get(RestOptions.PROFILING_RESULT_DIR);
    }

    @Override
    protected File getFile(HandlerRequest<EmptyRequestBody> handlerRequest) {
        if (profilingResultDir == null) {
            return null;
        }
        String filename =
                new File(handlerRequest.getPathParameter(ProfilingFileNamePathParameter.class))
                        .getName();
        return new File(profilingResultDir, filename);
    }
}
