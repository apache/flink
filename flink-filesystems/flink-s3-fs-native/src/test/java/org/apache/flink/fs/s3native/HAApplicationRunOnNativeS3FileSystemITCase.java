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

package org.apache.flink.fs.s3native;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Base ITCase tests for HA Application on Native s3. */
class HAApplicationRunOnNativeS3FileSystemITCase extends AbstractNativeS3HAApplicationRunITCase {

    // AbstractHAApplicationRunITCase already registers its own extension at @Order(1), so this
    // one must run after it.
    @RegisterExtension
    @Order(2)
    private static final SeaweedFsNativeS3HAClusterExtension CLUSTER_EXTENSION =
            new SeaweedFsNativeS3HAClusterExtension(
                    AbstractNativeS3HAApplicationRunITCase::createConfiguration);

    @Override
    SeaweedFsNativeS3TestContainer getSeaweedFsContainer() {
        return CLUSTER_EXTENSION.getContainer();
    }
}
