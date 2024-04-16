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

package org.apache.flink.state.forst.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Before;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link ForStValueState}. */
public class ForStValueStateTest extends ForStStateTestBase {

    private TypeSerializer<Integer> keySerializer;

    private TypeSerializer<String> valueSerializer;

    private int maxParallelism;

    @Before
    public void prepare() throws Exception {
        super.setUp();
        this.keySerializer = IntSerializer.INSTANCE;
        this.valueSerializer = StringSerializer.INSTANCE;
        this.maxParallelism = 128;
    }

    @Test
    public void testPutGetAndClear() throws Exception {
        ColumnFamilyHandle valueStateHandle = createColumnFamilyHandle("valueState");
        ForStValueState<Integer, String> valueState =
                new ForStValueState<>(
                        db,
                        new WriteOptions(),
                        valueStateHandle,
                        keySerializer,
                        valueSerializer,
                        maxParallelism);

        valueState.put(1, "test-1");
        assertThat(valueState.get(1)).isEqualTo("test-1");
        valueState.put(2, "test-2");
        assertThat(valueState.get(2)).isEqualTo("test-2");
        valueState.put(1, "test-3");
        assertThat(valueState.get(1)).isEqualTo("test-3");

        valueState.clear(1);
        assertThat(valueState.get(1)).isNull();
        valueState.clear(2);
        assertThat(valueState.get(2)).isNull();
    }

    @Test
    public void testWriteBatch() throws Exception {
        ColumnFamilyHandle valueStateHandle = createColumnFamilyHandle("write-batch");
        ForStValueState<Integer, String> valueState =
                new ForStValueState<>(
                        db,
                        new WriteOptions(),
                        valueStateHandle,
                        keySerializer,
                        valueSerializer,
                        maxParallelism);

        List<Tuple2<Integer, String>> keyValueData = new ArrayList<>();
        keyValueData.add(Tuple2.of(1, "test-1"));
        keyValueData.add(Tuple2.of(2, "test-2"));
        keyValueData.add(Tuple2.of(3, "test-3"));
        keyValueData.add(Tuple2.of(3, "test-4"));

        valueState.writeBatch(keyValueData);

        assertThat(valueState.get(1)).isEqualTo("test-1");
        assertThat(valueState.get(2)).isEqualTo("test-2");
        assertThat(valueState.get(3)).isEqualTo("test-4");
    }
}
