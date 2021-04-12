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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.apache.flink.configuration.ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR;

/** Test {@link UserLibOverlay}. */
public class UserLibOverlayTest extends ContainerOverlayTestBase {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testConfigure() throws Exception {
        final File userLibFolder = tempFolder.newFolder(DEFAULT_FLINK_USR_LIB_DIR);

        final Path[] files =
                createPaths(
                        tempFolder.getRoot(),
                        "usrlib/job_a.jar",
                        "usrlib/lib/dep1.jar",
                        "usrlib/lib/dep2.jar");

        final ContainerSpecification containerSpecification = new ContainerSpecification();
        final UserLibOverlay overlay =
                UserLibOverlay.newBuilder().setUsrLibDirectory(userLibFolder).build();
        overlay.configure(containerSpecification);

        for (Path file : files) {
            checkArtifact(
                    containerSpecification,
                    new Path(FlinkDistributionOverlay.TARGET_ROOT, file.toString()));
        }
    }
}
