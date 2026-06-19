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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Wrapper for using HadoopInputFormats (mapred-variant) with Flink.
 *
 * <p>The IF is returning a {@code Tuple2<K,V>}.
 *
 * @param <K> Type of the key
 * @param <V> Type of the value.
 */
@Public
public class HadoopInputFormat<K, V> extends HadoopInputFormatBase<K, V, Tuple2<K, V>>
        implements ResultTypeQueryable<Tuple2<K, V>> {

    private static final long serialVersionUID = 1L;

    public HadoopInputFormat(
            org.apache.hadoop.mapred.InputFormat<K, V> mapredInputFormat,
            Class<K> key,
            Class<V> value,
            JobConf job) {
        super(mapredInputFormat, key, value, job);
    }

    public HadoopInputFormat(
            org.apache.hadoop.mapred.InputFormat<K, V> mapredInputFormat,
            Class<K> key,
            Class<V> value) {
        super(mapredInputFormat, key, value, new JobConf());
    }

    @Override
    public Tuple2<K, V> nextRecord(Tuple2<K, V> record) throws IOException {
        if (!fetched) {
            fetchNext();
        }
        if (!hasNext) {
            return null;
        }
        record.f0 = key;
        record.f1 = value;
        fetched = false;
        return record;
    }

    @Override
    public TypeInformation<Tuple2<K, V>> getProducedType() {
        return new TupleTypeInfo<>(
                TypeExtractor.createTypeInfo(keyClass), TypeExtractor.createTypeInfo(valueClass));
    }
}
