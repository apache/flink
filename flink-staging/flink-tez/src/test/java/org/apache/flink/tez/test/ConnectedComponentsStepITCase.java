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

import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.tez.examples.ConnectedComponentsStep;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Pattern;

/*
 * Note: This does not test whether the program computes one step of the
 * Weakly Connected Components program correctly. It only tests whether
 * the program assigns a wrong component to a vertex.
 */

public class ConnectedComponentsStepITCase extends TezProgramTestBase {

    private static final long SEED = 0xBADC0FFEEBEEFL;

    private static final int NUM_VERTICES = 1000;

    private static final int NUM_EDGES = 10000;


    private String verticesPath;
    private String edgesPath;
    private String resultPath;


    @Override
    protected void preSubmit() throws Exception {
        verticesPath = createTempFile("vertices.txt", ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
        edgesPath = createTempFile("edges.txt", ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
        resultPath = getTempFilePath("results");
    }

    @Override
    protected void testProgram() throws Exception {
        ConnectedComponentsStep.main(verticesPath, edgesPath, resultPath, "100");
    }

    @Override
    protected void postSubmit() throws Exception {
        for (BufferedReader reader : getResultReader(resultPath)) {
            checkOddEvenResult(reader);
        }
    }

    private static void checkOddEvenResult(BufferedReader result) throws IOException {
        Pattern split = Pattern.compile(" ");
        String line;
        while ((line = result.readLine()) != null) {
            String[] res = split.split(line);
            Assert.assertEquals("Malformed result: Wrong number of tokens in line.", 2, res.length);
            try {
                int vertex = Integer.parseInt(res[0]);
                int component = Integer.parseInt(res[1]);
                Assert.assertTrue(((vertex % 2) == (component % 2)));
            } catch (NumberFormatException e) {
                Assert.fail("Malformed result.");
            }
        }
    }
}
