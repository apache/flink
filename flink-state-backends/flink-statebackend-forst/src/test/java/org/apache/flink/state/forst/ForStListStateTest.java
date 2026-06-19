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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.v2.internal.InternalListState;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForStListState}. */
public class ForStListStateTest extends ForStStateTestBase {

    @Test
    public void testMergeNamespace() throws Exception {
        ListStateDescriptor<Integer> descriptor =
                new ListStateDescriptor<>("testState", IntSerializer.INSTANCE);
        InternalListState<String, Integer, Integer> listState =
                keyedBackend.createState(1, IntSerializer.INSTANCE, descriptor);

        setCurrentContext("test", "test");
        for (int i = 0; i < 10; i++) {
            listState.setCurrentNamespace(i);
            listState.asyncAdd(i);
        }
        drain();

        setCurrentContext("test", "test");
        for (int i = 0; i < 10; i++) {
            listState.setCurrentNamespace(i);
            Iterable<Integer> list = listState.get();
            assertThat(list).containsExactly(i);
        }
        drain();

        // 1~20
        ArrayList<Integer> namespaces = new ArrayList<>();
        for (int i = 1; i < 20; i++) {
            namespaces.add(i);
        }

        setCurrentContext("test", "test");
        listState
                .asyncMergeNamespaces(0, namespaces)
                .thenAccept(
                        (e) -> {
                            listState.setCurrentNamespace(0);
                            Iterable<Integer> list = listState.get();
                            assertThat(list).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

                            for (int i = 1; i < 10; i++) {
                                listState.setCurrentNamespace(i);
                                assertThat(listState.get()).isNullOrEmpty();
                            }
                        });
        drain();

        // test sync method
        setCurrentContext("test", "test");
        for (int i = 10; i < 20; i++) {
            listState.setCurrentNamespace(i);
            listState.add(i);
        }
        drain();

        setCurrentContext("test", "test");
        listState.mergeNamespaces(0, namespaces);
        listState.setCurrentNamespace(0);
        Iterable<Integer> list = listState.get();
        assertThat(list)
                .containsExactly(
                        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);

        for (int i = 1; i < 20; i++) {
            listState.setCurrentNamespace(i);
            assertThat(listState.get()).isNullOrEmpty();
        }
        drain();
    }
}
