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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for the WebMonitorUtils. */
public class WebMonitorUtilsTest extends TestLogger {

    /** Tests dynamically loading of handlers such as {@link JarUploadHandler}. */
    @Test
    public void testLoadWebSubmissionExtension() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, "localhost");
        final WebMonitorExtension webMonitorExtension =
                WebMonitorUtils.loadWebSubmissionExtension(
                        CompletableFuture::new,
                        Time.seconds(10),
                        Collections.emptyMap(),
                        CompletableFuture.completedFuture("localhost:12345"),
                        Paths.get("/tmp"),
                        Executors.directExecutor(),
                        configuration);

        assertThat(webMonitorExtension, is(not(nullValue())));
    }
}
