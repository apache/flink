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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * ClusterEntryPointExceptionUtilsTest checks whether the OOM message enrichment works as expected.
 */
public class ClusterEntryPointExceptionUtilsTest extends TestLogger {

    @Test
    public void testDirectMemoryOOMHandling() {
        OutOfMemoryError error = new OutOfMemoryError("Direct buffer memory");
        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(error);

        assertThat(
                error.getMessage(),
                is(ClusterEntryPointExceptionUtils.JM_DIRECT_OOM_ERROR_MESSAGE));
    }

    @Test
    public void testMetaspaceOOMHandling() {
        OutOfMemoryError error = new OutOfMemoryError("Metaspace");
        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(error);

        assertThat(
                error.getMessage(),
                is(ClusterEntryPointExceptionUtils.JM_METASPACE_OOM_ERROR_MESSAGE));
    }

    @Test
    public void testHeapSpaceOOMHandling() {
        OutOfMemoryError error = new OutOfMemoryError("Java heap space");
        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(error);

        assertThat(
                error.getMessage(),
                is(ClusterEntryPointExceptionUtils.JM_HEAP_SPACE_OOM_ERROR_MESSAGE));
    }

    @Test
    public void testAnyOtherOOMHandling() {
        String message = "Any other message won't be changed.";
        OutOfMemoryError error = new OutOfMemoryError(message);
        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(error);

        assertThat(error.getMessage(), is(message));
    }
}
