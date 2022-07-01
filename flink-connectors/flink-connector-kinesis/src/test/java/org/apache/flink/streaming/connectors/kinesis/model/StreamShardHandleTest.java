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

package org.apache.flink.streaming.connectors.kinesis.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for methods in the {@link StreamShardHandle} class. */
public class StreamShardHandleTest {
    @Test
    public void testCompareShardIds() {
        assertThat(
                        StreamShardHandle.compareShardIds(
                                "shardId-000000000001", "shardId-000000000010"))
                .isLessThan(0);
        assertThat(
                        StreamShardHandle.compareShardIds(
                                "shardId-000000000010", "shardId-000000000010"))
                .isEqualTo(0);
        assertThat(
                        StreamShardHandle.compareShardIds(
                                "shardId-000000000015", "shardId-000000000010"))
                .isGreaterThan(0);
    }
}
