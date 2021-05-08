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
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ContainerOverlayTestBase {

    private Map<String, String> originalEnvironment;

    @Before
    public void before() {
        originalEnvironment = new HashMap<>(System.getenv());
    }

    @After
    public void after() {
        CommonTestUtils.setEnv(originalEnvironment, true);
    }

    /**
     * Create an empty file for each given path.
     *
     * @param root the root folder in which to create the files.
     * @param paths the relative paths to create.
     */
    protected static Path[] createPaths(File root, String... paths) throws Exception {
        Path[] files = new Path[paths.length];
        for (int i = 0; i < paths.length; i++) {
            File file = root.toPath().resolve(paths[i]).toFile();
            file.getParentFile().mkdirs();
            file.createNewFile();
            files[i] = new Path(paths[i]);
        }
        return files;
    }

    /** Check that an artifact exists for the given remote path. */
    protected static ContainerSpecification.Artifact checkArtifact(
            ContainerSpecification spec, Path remotePath) {
        for (ContainerSpecification.Artifact artifact : spec.getArtifacts()) {
            if (remotePath.equals(artifact.dest)) {
                return artifact;
            }
        }
        throw new AssertionError("no such artifact (" + remotePath + ")");
    }
}
