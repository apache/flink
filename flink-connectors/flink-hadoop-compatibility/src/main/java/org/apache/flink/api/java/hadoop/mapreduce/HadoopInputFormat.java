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

package org.apache.flink.api.java.hadoop.mapreduce;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * InputFormat implementation allowing to use Hadoop (mapreduce) InputFormats with Flink.
 *
 * @param <K> Key Type
 * @param <V> Value Type
 */
@Public
public class HadoopInputFormat<K, V> extends HadoopInputFormatBase<K, V, Tuple2<K, V>>
        implements ResultTypeQueryable<Tuple2<K, V>> {

    private static final long serialVersionUID = 1L;

    public HadoopInputFormat(
            org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat,
            Class<K> key,
            Class<V> value,
            Job job) {
        super(mapreduceInputFormat, key, value, job);
    }

    public HadoopInputFormat(
            org.apache.hadoop.mapreduce.InputFormat<K, V> mapreduceInputFormat,
            Class<K> key,
            Class<V> value)
            throws IOException {
        super(mapreduceInputFormat, key, value, Job.getInstance());
    }

    @Override
    public Tuple2<K, V> nextRecord(Tuple2<K, V> record) throws IOException {
        if (!this.fetched) {
            fetchNext();
        }
        if (!this.hasNext) {
            return null;
        }
        try {
            record.f0 = recordReader.getCurrentKey();
            record.f1 = recordReader.getCurrentValue();
        } catch (InterruptedException e) {
            throw new IOException("Could not get KeyValue pair.", e);
        }
        this.fetched = false;

        return record;
    }

    @Override
    public TypeInformation<Tuple2<K, V>> getProducedType() {
        return new TupleTypeInfo<Tuple2<K, V>>(
                TypeExtractor.createTypeInfo(keyClass), TypeExtractor.createTypeInfo(valueClass));
    }
}
