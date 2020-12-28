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

package org.apache.flink.api.java.hadoop.mapred;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;

import java.io.IOException;

/**
 * Wrapper for using HadoopOutputFormats (mapred-variant) with Flink.
 *
 * <p>The IF is returning a {@code Tuple2<K,V>}.
 *
 * @param <K> Type of the key
 * @param <V> Type of the value.
 */
@Public
public class HadoopOutputFormat<K, V> extends HadoopOutputFormatBase<K, V, Tuple2<K, V>> {

    private static final long serialVersionUID = 1L;

    public HadoopOutputFormat(
            org.apache.hadoop.mapred.OutputFormat<K, V> mapredOutputFormat, JobConf job) {
        super(mapredOutputFormat, job);
    }

    public HadoopOutputFormat(
            org.apache.hadoop.mapred.OutputFormat<K, V> mapredOutputFormat,
            Class<OutputCommitter> outputCommitterClass,
            JobConf job) {
        this(mapredOutputFormat, job);
        super.getJobConf().setOutputCommitter(outputCommitterClass);
    }

    @Override
    public void writeRecord(Tuple2<K, V> record) throws IOException {
        this.recordWriter.write(record.f0, record.f1);
    }
}
