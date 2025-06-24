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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Test;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

/** Tests for {@link RestClient} that rely on external connections. */
public class RestClientITCase extends TestLogger {

    @Test
    public void testHttpsConnectionWithDefaultCerts() throws Exception {
        final Configuration config = new Configuration();
        final URL httpsUrl = new URL("https://raw.githubusercontent.com");
        TestUrlMessageHeaders testUrlMessageHeaders =
                new TestUrlMessageHeaders(
                        "apache/flink/master/flink-runtime-web/web-dashboard/package.json");
        try (final RestClient restClient =
                RestClient.forUrl(config, Executors.directExecutor(), httpsUrl)) {
            restClient
                    .sendRequest(
                            httpsUrl.getHost(),
                            443,
                            testUrlMessageHeaders,
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance())
                    .get(60, TimeUnit.SECONDS);
        }
    }

    private static class TestUrlMessageHeaders
            implements RuntimeMessageHeaders<
                    EmptyRequestBody, GenericResponseBody, EmptyMessageParameters> {

        private final String endpointUrl;

        private final Collection<RestAPIVersion<EmptyRestAPIVersion>> supportedVersions =
                Collections.singleton(new EmptyRestAPIVersion());

        TestUrlMessageHeaders(String endpointUrl) {
            this.endpointUrl = endpointUrl;
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.GET;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return endpointUrl;
        }

        @Override
        public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
            return supportedVersions;
        }

        @Override
        public Class<GenericResponseBody> getResponseClass() {
            return GenericResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }

        private static class EmptyRestAPIVersion implements RestAPIVersion<EmptyRestAPIVersion> {

            @Override
            public String getURLVersionPrefix() {
                return "";
            }

            @Override
            public boolean isDefaultVersion() {
                return true;
            }

            @Override
            public boolean isStableVersion() {
                return true;
            }

            @Override
            public int compareTo(EmptyRestAPIVersion o) {
                return 0;
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private static class GenericResponseBody extends LinkedHashMap implements ResponseBody {}
}
