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

package org.apache.flink.hdfstests;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperatorFactory;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

/** Utility class that contains common methods for testing. */
public class Utils {

    public static <OUT>
            OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, OUT>
                    createContinuousFileProcessingTestHarness(FileInputFormat<OUT> inputFormat)
                            throws Exception {

        return createContinuousFileProcessingTestHarness(
                inputFormat, TypeExtractor.getInputFormatTypes(inputFormat), null);
    }

    public static <OUT>
            OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, OUT>
                    createContinuousFileProcessingTestHarness(
                            FileInputFormat<OUT> inputFormat,
                            TypeInformation<OUT> outTypeInfo,
                            ExecutionConfig executionConfig)
                            throws Exception {

        OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, OUT> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new ContinuousFileReaderOperatorFactory<>(inputFormat));
        testHarness
                .getOperatorFactory()
                .setOutputType(
                        outTypeInfo,
                        executionConfig == null
                                ? testHarness.getExecutionConfig()
                                : executionConfig);

        return testHarness;
    }
}
