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

package org.apache.flink.tez.test;

import org.apache.flink.test.testdata.PageRankData;
import org.apache.flink.tez.examples.PageRankBasicStep;

public class PageRankBasicStepITCase extends TezProgramTestBase {

    private String verticesPath;
    private String edgesPath;
    private String resultPath;
    private String expectedResult;

    public static final String RANKS_AFTER_1_ITERATION = "1 0.2\n" +
            "2 0.25666666666666665\n" +
            "3 0.1716666666666667\n" +
            "4 0.1716666666666667\n" +
            "5 0.2";

    @Override
    protected void preSubmit() throws Exception {
        resultPath = getTempDirPath("result");
        verticesPath = createTempFile("vertices.txt", PageRankData.VERTICES);
        edgesPath = createTempFile("edges.txt", PageRankData.EDGES);
    }

    @Override
    protected void testProgram() throws Exception {
        PageRankBasicStep.main(new String[]{verticesPath, edgesPath, resultPath, PageRankData.NUM_VERTICES+"", "-1"});
        expectedResult = RANKS_AFTER_1_ITERATION;
    }

    @Override
    protected void postSubmit() throws Exception {
        compareKeyValuePairsWithDelta(expectedResult, resultPath, " ", 0.001);
    }
}
