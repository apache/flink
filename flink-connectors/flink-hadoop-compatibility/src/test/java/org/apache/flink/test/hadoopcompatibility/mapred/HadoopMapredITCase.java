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

package org.apache.flink.test.hadoopcompatibility.mapred;

import org.apache.flink.test.hadoopcompatibility.mapred.example.HadoopMapredCompatWordCount;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.util.OperatingSystem;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;
import static org.assertj.core.api.Assumptions.assumeThat;

/** IT cases for mapred. */
@ExtendWith(ParameterizedTestExtension.class)
public class HadoopMapredITCase extends JavaProgramTestBase {

    protected String textPath;
    protected String resultPath;

    @BeforeEach
    public void checkOperatingSystem() {
        // FLINK-5164 - see https://wiki.apache.org/hadoop/WindowsProblems
        assumeThat(OperatingSystem.isWindows())
                .as("This test can't run successfully on Windows.")
                .isFalse();
    }

    @Override
    protected void preSubmit() throws Exception {
        textPath = createTempFile("text.txt", WordCountData.TEXT);
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath, new String[] {".", "_"});
    }

    @Override
    protected void testProgram() throws Exception {
        HadoopMapredCompatWordCount.main(new String[] {textPath, resultPath});
    }
}
