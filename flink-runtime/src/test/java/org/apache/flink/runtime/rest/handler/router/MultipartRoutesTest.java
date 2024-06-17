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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MultipartRoutesTest {

    @Test
    public void testRoutePatternMatching() {
        MultipartRoutes.Builder builder = new MultipartRoutes.Builder();
        builder.addPostRoute("/jobs");
        builder.addPostRoute("/jobs/:jobid/stop");
        builder.addPostRoute("/jobs/:jobid/savepoints/:savepointid/delete");

        builder.addFileUploadRoute("/jobs");
        builder.addFileUploadRoute("/jobs/:jobid/stop");
        builder.addFileUploadRoute("/jobs/:jobid/savepoints/:savepointid/delete");

        MultipartRoutes routes = builder.build();

        assertThat(routes.isPostRoute("/jobs")).isTrue();
        assertThat(routes.isPostRoute("/jobs?q1=p1&q2=p2")).isTrue();
        assertThat(routes.isPostRoute("/jobs/abc")).isFalse();
        assertThat(routes.isPostRoute("/jobs/abc/stop")).isTrue();
        assertThat(routes.isPostRoute("/jobs/abc/savepoints/def/delete")).isTrue();

        assertThat(routes.isFileUploadRoute("/jobs")).isTrue();
        assertThat(routes.isFileUploadRoute("/jobs?q1=p1&q2=p2")).isTrue();
        assertThat(routes.isFileUploadRoute("/jobs/abc")).isFalse();
        assertThat(routes.isFileUploadRoute("/jobs/abc/stop")).isTrue();
        assertThat(routes.isFileUploadRoute("/jobs/abc/savepoints/def/delete")).isTrue();
    }
}
