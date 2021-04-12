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

package org.apache.flink.streaming.test.scala.examples.windowing;

import org.apache.flink.streaming.examples.windowing.util.TopSpeedWindowingExampleData;
import org.apache.flink.streaming.scala.examples.windowing.TopSpeedWindowing;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

/** Tests for {@link TopSpeedWindowing}. */
public class TopSpeedWindowingExampleITCase extends AbstractTestBase {

    @Test
    public void testProgram() throws Exception {
        String textPath = createTempFile("text.txt", TopSpeedWindowingExampleData.CAR_DATA);
        String resultPath = getTempDirPath("result");

        TopSpeedWindowing.main(
                new String[] {
                    "--input", textPath,
                    "--output", resultPath
                });

        compareResultsByLinesInMemory(
                TopSpeedWindowingExampleData.TOP_CASE_CLASS_SPEEDS, resultPath);
    }
}
