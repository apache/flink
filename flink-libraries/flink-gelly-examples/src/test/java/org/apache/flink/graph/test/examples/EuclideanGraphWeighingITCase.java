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

package org.apache.flink.graph.test.examples;

import org.apache.flink.graph.examples.EuclideanGraphWeighing;
import org.apache.flink.graph.examples.data.EuclideanGraphData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

/** Tests for {@link EuclideanGraphWeighing}. */
@RunWith(Parameterized.class)
public class EuclideanGraphWeighingITCase extends MultipleProgramsTestBase {

    private String verticesPath;

    private String edgesPath;

    private String resultPath;

    private String expected;

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    public EuclideanGraphWeighingITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Before
    public void before() throws Exception {
        resultPath = tempFolder.newFile().toURI().toString();
        File verticesFile = tempFolder.newFile();
        FileUtils.writeFileUtf8(verticesFile, EuclideanGraphData.VERTICES);

        File edgesFile = tempFolder.newFile();
        FileUtils.writeFileUtf8(edgesFile, EuclideanGraphData.EDGES);

        verticesPath = verticesFile.toURI().toString();
        edgesPath = edgesFile.toURI().toString();
    }

    @Test
    public void testGraphWeightingWeighing() throws Exception {
        EuclideanGraphWeighing.main(new String[] {verticesPath, edgesPath, resultPath});
        expected = EuclideanGraphData.RESULTED_WEIGHTED_EDGES;
    }

    @After
    public void after() throws Exception {
        TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath);
    }
}
