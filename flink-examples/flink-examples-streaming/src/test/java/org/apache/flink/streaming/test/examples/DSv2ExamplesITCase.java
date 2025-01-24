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

package org.apache.flink.streaming.test.examples;

import org.apache.flink.streaming.examples.dsv2.eventtime.CountNewsClicks;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Objects;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;

/** Integration test for DataStream API V2 examples. */
class DSv2ExamplesITCase extends AbstractTestBase {

    @Test
    void testWordCount() throws Exception {
        final String textPath = createTempFile("text.txt", WordCountData.TEXT);
        final String resultPath = getTempDirPath("result");

        org.apache.flink.streaming.examples.dsv2.wordcount.WordCount.main(
                new String[] {
                    "--input", textPath,
                    "--output", resultPath
                });

        compareResultsByLinesInMemory(WordCountData.STREAMING_COUNTS_AS_TUPLES, resultPath);
    }

    @Test
    void testJoin() throws Exception {
        final String resultPath = getTempDirPath("result");

        org.apache.flink.streaming.examples.dsv2.join.Join.main(
                new String[] {"--output", resultPath});

        compareResultsByLinesInMemory(
                loadFileContent("datas/dsv2/join/JoinResult.csv"), resultPath);
    }

    @Test
    void testCountProductSalesWindowing() throws Exception {
        final String resultPath = getTempDirPath("result");

        org.apache.flink.streaming.examples.dsv2.windowing.CountProductSalesWindowing.main(
                new String[] {"--output", resultPath});

        compareResultsByLinesInMemory(
                loadFileContent("datas/dsv2/windowing/CountProductSalesWindowingResult.csv"),
                resultPath);
    }

    @Test
    void testCountNewsClicks() throws Exception {
        final String resultPath = getTempDirPath("result");

        CountNewsClicks.main(new String[] {"--output", resultPath});

        compareResultsByLinesInMemory(
                loadFileContent("datas/dsv2/eventtime/CountNewsClicksResult.csv"), resultPath);
    }

    /**
     * Load the content from the specified file path for testing purposes.
     *
     * @return the file content
     */
    private static String loadFileContent(String relativeFilePathInResources) throws IOException {
        URL url =
                DSv2ExamplesITCase.class.getClassLoader().getResource(relativeFilePathInResources);
        Objects.requireNonNull(url);

        StringBuilder contentBuilder = new StringBuilder();
        BufferedReader br = new BufferedReader(new FileReader(url.getPath()));
        String line;
        while ((line = br.readLine()) != null) {
            contentBuilder.append(line).append("\n");
        }

        return contentBuilder.toString();
    }
}
