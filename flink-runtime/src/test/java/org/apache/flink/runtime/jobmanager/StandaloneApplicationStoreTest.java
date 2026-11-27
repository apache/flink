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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link StandaloneApplicationStore}. */
class StandaloneApplicationStoreTest {

    /** Tests that all operations work and don't change the state. */
    @Test
    void testNoOps() throws Exception {
        StandaloneApplicationStore applicationStore = new StandaloneApplicationStore();
        applicationStore.start();

        ApplicationID applicationId = new ApplicationID();
        ApplicationStoreEntry applicationStoreEntry =
                TestingApplicationStoreEntry.newBuilder().setApplicationId(applicationId).build();

        assertThat(applicationStore.getApplicationIds()).isEmpty();

        applicationStore.putApplication(applicationStoreEntry);
        assertThat(applicationStore.getApplicationIds()).isEmpty();

        applicationStore.globalCleanupAsync(applicationId, Executors.directExecutor()).join();
        assertThat(applicationStore.getApplicationIds()).isEmpty();

        assertThat(applicationStore.recoverApplication(applicationId)).isEmpty();

        applicationStore.stop();
    }
}
