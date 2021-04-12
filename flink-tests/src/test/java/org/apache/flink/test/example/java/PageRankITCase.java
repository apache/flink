/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.java;

import org.apache.flink.examples.java.graph.PageRank;
import org.apache.flink.test.testdata.PageRankData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.UUID;

/** Test for {@link PageRank}. */
@RunWith(Parameterized.class)
public class PageRankITCase extends MultipleProgramsTestBase {

    public PageRankITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String verticesPath;
    private String edgesPath;
    private String resultPath;
    private String expected;

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        final File folder = tempFolder.newFolder();
        final File resultFile = new File(folder, UUID.randomUUID().toString());
        resultPath = resultFile.toURI().toString();

        File verticesFile = tempFolder.newFile();
        FileUtils.writeFileUtf8(verticesFile, PageRankData.VERTICES);

        File edgesFile = tempFolder.newFile();
        FileUtils.writeFileUtf8(edgesFile, PageRankData.EDGES);

        verticesPath = verticesFile.toURI().toString();
        edgesPath = edgesFile.toURI().toString();
    }

    @After
    public void after() throws Exception {
        compareKeyValuePairsWithDelta(expected, resultPath, " ", 0.01);
    }

    @Test
    public void testPageRankSmallNumberOfIterations() throws Exception {
        PageRank.main(
                new String[] {
                    "--pages", verticesPath,
                    "--links", edgesPath,
                    "--output", resultPath,
                    "--numPages", PageRankData.NUM_VERTICES + "",
                    "--iterations", "3"
                });
        expected = PageRankData.RANKS_AFTER_3_ITERATIONS;
    }

    @Test
    public void testPageRankWithConvergenceCriterion() throws Exception {
        PageRank.main(
                new String[] {
                    "--pages", verticesPath,
                    "--links", edgesPath,
                    "--output", resultPath,
                    "--numPages", PageRankData.NUM_VERTICES + "",
                    "--vertices", "1000"
                });
        expected = PageRankData.RANKS_AFTER_EPSILON_0_0001_CONVERGENCE;
    }
}
