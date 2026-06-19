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

package org.apache.flink.hadoopcompatibility;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * HadoopInputs is a utility class to use Apache Hadoop InputFormats with Apache Flink.
 *
 * <p>It provides methods to create Flink InputFormat wrappers for Hadoop {@link
 * org.apache.hadoop.mapred.InputFormat} and {@link org.apache.hadoop.mapreduce.InputFormat}.
 *
 * <p>Key value pairs produced by the Hadoop InputFormats are converted into Flink {@link
 * org.apache.flink.api.java.tuple.Tuple2 Tuple2} objects where the first field ({@link
 * org.apache.flink.api.java.tuple.Tuple2#f0 Tuple2.f0}) is the key and the second field ({@link
 * org.apache.flink.api.java.tuple.Tuple2#f1 Tuple2.f1}) is the value.
 */
public final class HadoopInputs {
    // ----------------------------------- Hadoop Input Format
    // ---------------------------------------

    /**
     * Creates a Flink {@link InputFormat} that wraps the given Hadoop {@link
     * org.apache.hadoop.mapred.FileInputFormat}.
     *
     * @return A Flink InputFormat that wraps the Hadoop FileInputFormat.
     */
    public static <K, V> HadoopInputFormat<K, V> readHadoopFile(
            org.apache.hadoop.mapred.FileInputFormat<K, V> mapredInputFormat,
            Class<K> key,
            Class<V> value,
            String inputPath,
            JobConf job) {
        // set input path in JobConf
        org.apache.hadoop.mapred.FileInputFormat.addInputPath(
                job, new org.apache.hadoop.fs.Path(inputPath));
        // return wrapping InputFormat
        return createHadoopInput(mapredInputFormat, key, value, job);
    }

    /**
     * Creates a Flink {@link InputFormat} that wraps the given Hadoop {@link
     * org.apache.hadoop.mapred.FileInputFormat}.
     *
     * @return A Flink InputFormat that wraps the Hadoop FileInputFormat.
     */
    public static <K, V> HadoopInputFormat<K, V> readHadoopFile(
            org.apache.hadoop.mapred.FileInputFormat<K, V> mapredInputFormat,
            Class<K> key,
            Class<V> value,
            String inputPath) {
        return readHadoopFile(mapredInputFormat, key, value, inputPath, new JobConf());
    }

    /**
     * Creates a Flink {@link InputFormat} to read a Hadoop sequence file for the given key and
     * value classes.
     *
     * @return A Flink InputFormat that wraps a Hadoop SequenceFileInputFormat.
     */
    public static <K, V> HadoopInputFormat<K, V> readSequenceFile(
            Class<K> key, Class<V> value, String inputPath) throws IOException {
        return readHadoopFile(
                new org.apache.hadoop.mapred.SequenceFileInputFormat<K, V>(),
                key,
                value,
                inputPath);
    }

    /**
     * Creates a Flink {@link InputFormat} that wraps the given Hadoop {@link
     * org.apache.hadoop.mapred.InputFormat}.
     *
     * @return A Flink InputFormat that wraps the Hadoop InputFormat.
     */
    public static <K, V> HadoopInputFormat<K, V> createHadoopInput(
            org.apache.hadoop.mapred.InputFormat<K, V> mapredInputFormat,
            Class<K> key,
            Class<V> value,
            JobConf job) {
        return new HadoopInputFormat<>(mapredInputFormat, key, value, job);
    }

    /**
     * Creates a Flink {@link InputFormat} that wraps the given Hadoop {@link
     * org.apache.hadoop.mapreduce.lib.input.FileInputFormat}.
     *
     * @return A Flink InputFormat that wraps the Hadoop FileInputFormat.
     */
    public static <K, V>
            org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> readHadoopFile(
                    org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K, V>
                            mapreduceInputFormat,
                    Class<K> key,
                    Class<V> value,
                    String inputPath,
                    Job job)
                    throws IOException {
        // set input path in Job
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(
                job, new org.apache.hadoop.fs.Path(inputPath));
        // return wrapping InputFormat
        return createHadoopInput(mapreduceInputFormat, key, value, job);
    }

    /**
     * Creates a Flink {@link InputFormat} that wraps the given Hadoop {@link
     * org.apache.hadoop.mapreduce.lib.input.FileInputFormat}.
     *
     * @return A Flink InputFormat that wraps the Hadoop FileInputFormat.
     */
    public static <K, V>
            org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> readHadoopFile(
                    org.apache.hadoop.mapreduce.lib.input.FileInputFormat<K, V>
                            mapreduceInputFormat,
                    Class<K> key,
                    Class<V> value,
                    String inputPath)
                    throws IOException {
        return readHadoopFile(mapreduceInputFormat, key, value, inputPath, Job.getInstance());
    }

    /**
     * Creates a Flink {@link InputFormat} that wraps the given Hadoop {@link
     * org.apache.hadoop.mapreduce.InputFormat}.
     *
     * @return A Flink InputFormat that wraps the Hadoop InputFormat.
     */
    public static <K, V>
            org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<K, V> createHadoopInput(
                    org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat,
                    Class<K> key,
                    Class<V> value,
                    Job job) {
        return new org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<>(
                mapreduceInputFormat, key, value, job);
    }
}
