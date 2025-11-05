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

package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordUtils;
import org.apache.flink.table.types.logical.IntType;

/** Test for {@link RowDataKeySerializer}. */
public class NodeSerializerTest extends SerializerTestBase<Node> {

    @Override
    protected TypeSerializer<Node> createSerializer() {
        return new NodeSerializer(new RowDataSerializer(new IntType()));
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<Node> getTypeClass() {
        return Node.class;
    }

    @Override
    protected Node[] getTestData() {
        return new Node[] {
            new Node(StreamRecordUtils.row(1), 1L, null, 2L, 2L, 1L),
            new Node(StreamRecordUtils.row(2), 2L, 1L, 3L, 3L, 2L),
            new Node(StreamRecordUtils.row(3), 3L, 2L, null, null, 3L),
        };
    }
}
