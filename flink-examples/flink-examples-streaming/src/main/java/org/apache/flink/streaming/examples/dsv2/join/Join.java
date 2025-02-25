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

package org.apache.flink.streaming.examples.dsv2.join;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.ParameterTool;

import java.time.Duration;
import java.util.Objects;

/**
 * Example illustrating a join between two data streams. The example works on two input streams with
 * pairs (name, grade) and (name, salary) respectively. It joins the streams based on "name".
 *
 * <p>The example uses a built-in sample data as inputs, which can be found in the resources
 * directory.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li><code>--output &lt;path&gt;</code>The output directory where the Job will write the
 *       results. If no output path is provided, the Job will print the results to <code>stdout
 *       </code>.
 * </ul>
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>Usage of Join extension in DataStream API V2
 * </ul>
 *
 * <p>Please note that if you intend to run this example in an IDE, you must first add the following
 * VM options: "--add-opens=java.base/java.util=ALL-UNNAMED". This is necessary because the module
 * system in JDK 17+ restricts some reflection operations.
 *
 * <p>Please note that the DataStream API V2 is a new set of APIs, to gradually replace the original
 * DataStream API. It is currently in the experimental stage and is not fully available for
 * production.
 */
public class Join {

    /** POJO class for grades. */
    public static class GradePojo {
        public int grade;
        public String name;
    }

    /** POJO class for salaries. */
    public static class SalaryPojo {
        public String name;
        public long salary;
    }

    /** POJO class for grade and salary. */
    public static class GradeAndSalaryPojo {
        public String name;
        public int grade;
        public long salary;

        public GradeAndSalaryPojo(String name, int grade, long salary) {
            this.name = name;
            this.grade = grade;
            this.salary = salary;
        }

        public String toString() {
            return String.format("%s,%d,%d", this.name, this.grade, this.salary);
        }
    }

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final boolean fileOutput = params.has("output");

        // obtain execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // create the data sources for both grades and salaries
        // We provide CSV files to represent the sample input data, which can be found in the
        // resources directory.
        String gradesInputFilePath =
                Objects.requireNonNull(
                                Join.class
                                        .getClassLoader()
                                        .getResource("datas/dsv2/join/JoinGrades.csv"))
                        .getPath();
        // create a FileSource with CSV format to read the input data
        FileSource<GradePojo> gradesFileSource =
                FileSource.forRecordStreamFormat(
                                CsvReaderFormat.forPojo(GradePojo.class),
                                new Path(gradesInputFilePath))
                        .build();
        NonKeyedPartitionStream<GradePojo> grades =
                env.fromSource(new WrappedSource<>(gradesFileSource), "grade source");

        String salariesInputFilePath =
                Objects.requireNonNull(
                                Join.class
                                        .getClassLoader()
                                        .getResource("datas/dsv2/join/JoinSalaries.csv"))
                        .getPath();
        FileSource<SalaryPojo> salariesFileSource =
                FileSource.forRecordStreamFormat(
                                CsvReaderFormat.forPojo(SalaryPojo.class),
                                new Path(salariesInputFilePath))
                        .build();
        NonKeyedPartitionStream<SalaryPojo> salaries =
                env.fromSource(new WrappedSource<>(salariesFileSource), "salary source");

        // joining two DataStreams
        NonKeyedPartitionStream<GradeAndSalaryPojo> joinedStream =
                BuiltinFuncs.join(
                        grades,
                        new GradeKeySelector(),
                        salaries,
                        new SalaryKeySelector(),
                        new JoinGradeAndSalaryFunction());

        if (fileOutput) {
            // write joined results to file
            joinedStream
                    .toSink(
                            new WrappedSink<>(
                                    FileSink.<GradeAndSalaryPojo>forRowFormat(
                                                    new Path(params.get("output")),
                                                    new SimpleStringEncoder<>())
                                            .withRollingPolicy(
                                                    DefaultRollingPolicy.builder()
                                                            .withMaxPartSize(
                                                                    MemorySize.ofMebiBytes(1))
                                                            .withRolloverInterval(
                                                                    Duration.ofSeconds(10))
                                                            .build())
                                            .build()))
                    .withName("output");
        } else {
            // Print the results to the STDOUT.
            joinedStream.toSink(new WrappedSink<>(new PrintSink<>())).withName("print-sink");
        }

        // execute program
        env.execute("Join Grade and Salary");
    }

    /** KeySelector of {@link GradePojo}. */
    private static class GradeKeySelector implements KeySelector<GradePojo, String> {

        @Override
        public String getKey(GradePojo value) throws Exception {
            return value.name;
        }
    }

    /** KeySelector of {@link SalaryPojo}. */
    private static class SalaryKeySelector implements KeySelector<SalaryPojo, String> {

        @Override
        public String getKey(SalaryPojo value) throws Exception {
            return value.name;
        }
    }

    /**
     * Join the function of grade (name, grade) and salary (name, salary) to produce an output of
     * (name, grade, salary).
     */
    private static class JoinGradeAndSalaryFunction
            implements JoinFunction<GradePojo, SalaryPojo, GradeAndSalaryPojo> {

        @Override
        public void processRecord(
                GradePojo leftRecord,
                SalaryPojo rightRecord,
                Collector<GradeAndSalaryPojo> output,
                RuntimeContext ctx)
                throws Exception {
            output.collect(
                    new GradeAndSalaryPojo(leftRecord.name, leftRecord.grade, rightRecord.salary));
        }
    }
}
