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

package org.apache.flink.test.distributedcache;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Test the distributed cache. */
public class DistributedCacheTest extends AbstractTestBase {

    public static final String DATA =
            "machen\n" + "zeit\n" + "heerscharen\n" + "keiner\n" + "meine\n";

    // ------------------------------------------------------------------------

    @Test
    public void testParseCachedFilesFromStringAndBack() {
        List<String> cachedFilesStringList =
                Arrays.asList(
                        "{path: /path/to/file1, name: file1, executable: 'true'}",
                        "{path: /path/to/file2, name: file2, executable: 'false'}");

        List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFilesList =
                Arrays.asList(
                        Tuple2.of(
                                "file1",
                                new DistributedCache.DistributedCacheEntry("/path/to/file1", true)),
                        Tuple2.of(
                                "file2",
                                new DistributedCache.DistributedCacheEntry(
                                        "/path/to/file2", false)));

        List<Tuple2<String, DistributedCache.DistributedCacheEntry>> parsedCachedFiles =
                DistributedCache.parseCachedFilesFromString(cachedFilesStringList);
        assertThat(parsedCachedFiles, containsInAnyOrder(cachedFilesList.toArray()));

        List<String> parsedCachedFileStrings =
                DistributedCache.parseStringFromCachedFiles(cachedFilesList);
        assertThat(parsedCachedFileStrings, containsInAnyOrder(cachedFilesStringList.toArray()));
    }

    @Test
    public void testStreamingDistributedCache() throws Exception {
        String textPath = createTempFile("count.txt", DATA);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile(textPath, "cache_test");
        env.readTextFile(textPath).flatMap(new WordChecker());
        env.execute();
    }

    @Test
    public void testBatchDistributedCache() throws Exception {
        String textPath = createTempFile("count.txt", DATA);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile(textPath, "cache_test");
        env.readTextFile(textPath).flatMap(new WordChecker()).count();
    }

    private static class WordChecker extends RichFlatMapFunction<String, Tuple1<String>> {
        private static final long serialVersionUID = 1L;

        private final List<String> wordList = new ArrayList<>();

        @Override
        public void open(OpenContext openContext) throws IOException {
            File file = getRuntimeContext().getDistributedCache().getFile("cache_test");
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String tempString;
                while ((tempString = reader.readLine()) != null) {
                    wordList.add(tempString);
                }
            }
        }

        @Override
        public void flatMap(String word, Collector<Tuple1<String>> out) throws Exception {
            assertTrue(
                    "Unexpected word in stream! wordFromStream: "
                            + word
                            + ", shouldBeOneOf: "
                            + wordList.toString(),
                    wordList.contains(word));

            out.collect(new Tuple1<>(word));
        }
    }
}
