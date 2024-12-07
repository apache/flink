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

package org.apache.flink.runtime.rest.handler.legacy.messages;

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for the {@link ClusterOverviewWithVersion}. */
@ExtendWith(NoOpTestExtension.class)
class ClusterOverviewWithVersionTest
        extends RestResponseMarshallingTestBase<ClusterOverviewWithVersion> {

    @Override
    protected Class<ClusterOverviewWithVersion> getTestResponseClass() {
        return ClusterOverviewWithVersion.class;
    }

    @Override
    protected ClusterOverviewWithVersion getTestResponseInstance() {
        return new ClusterOverviewWithVersion(2, 6, 3, 1, 3, 7, 4, 2, 0, "version", "commit");
    }
}
