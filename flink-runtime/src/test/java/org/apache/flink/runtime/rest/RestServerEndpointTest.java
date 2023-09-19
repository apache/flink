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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.helpers.NOPLogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test cases for the {@link RestServerEndpoint}. */
class RestServerEndpointTest {

    /** Tests that the REST handler URLs are properly sorted. */
    @Test
    void testRestHandlerUrlSorting() {
        final int numberHandlers = 5;

        final List<String> handlerUrls = new ArrayList<>(numberHandlers);

        handlerUrls.add("/jobs/overview");
        handlerUrls.add("/jobs/:jobid");
        handlerUrls.add("/jobs");
        handlerUrls.add("/:*");
        handlerUrls.add("/jobs/:jobid/config");

        final List<String> expected = new ArrayList<>(numberHandlers);

        expected.add("/jobs");
        expected.add("/jobs/overview");
        expected.add("/jobs/:jobid");
        expected.add("/jobs/:jobid/config");
        expected.add("/:*");

        Collections.sort(
                handlerUrls,
                new RestServerEndpoint.RestHandlerUrlComparator.CaseInsensitiveOrderComparator());

        assertThat(handlerUrls).isEqualTo(expected);
    }

    @Test
    void testCreateUploadDir(@TempDir File file) throws Exception {
        final Path testUploadDir = file.toPath().resolve("testUploadDir");
        assertThat(Files.exists(testUploadDir)).isFalse();
        RestServerEndpoint.createUploadDir(testUploadDir, NOPLogger.NOP_LOGGER, true);
        assertThat(Files.exists(testUploadDir)).isTrue();
    }

    @Test
    void testCreateUploadDirFails(@TempDir File file) throws Exception {
        assertThat(file.setWritable(false));

        final Path testUploadDir = file.toPath().resolve("testUploadDir");
        assertThat(Files.exists(testUploadDir)).isFalse();

        assertThatThrownBy(
                        () ->
                                RestServerEndpoint.createUploadDir(
                                        testUploadDir, NOPLogger.NOP_LOGGER, true))
                .isInstanceOf(IOException.class);
    }
}
