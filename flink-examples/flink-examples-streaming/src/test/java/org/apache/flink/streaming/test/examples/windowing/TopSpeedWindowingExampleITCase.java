/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.examples.windowing;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.examples.windowing.TopSpeedWindowing;
import org.apache.flink.streaming.examples.windowing.util.TopSpeedWindowingExampleData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;

/** Tests for {@link TopSpeedWindowing}. */
public class TopSpeedWindowingExampleITCase extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Test
    public void testTopSpeedWindowingExampleITCase() throws Exception {
        File inputFile = temporaryFolder.newFile();
        FileUtils.writeFileUtf8(inputFile, TopSpeedWindowingExampleData.CAR_DATA);

        final String resultPath = temporaryFolder.newFolder().toURI().toString();

        TopSpeedWindowing.main(
                new String[] {"--input", inputFile.getAbsolutePath(), "--output", resultPath});

        compareResultsByLinesInMemory(TopSpeedWindowingExampleData.TOP_SPEEDS, resultPath);
    }
}
