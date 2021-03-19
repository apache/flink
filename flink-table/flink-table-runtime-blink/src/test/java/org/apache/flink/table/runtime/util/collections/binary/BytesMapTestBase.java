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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.VarCharType;

/** Test case for {@link BytesMap}. */
public class BytesMapTestBase {
    protected static final long RANDOM_SEED = 76518743207143L;
    protected static final int PAGE_SIZE = 32 * 1024;
    protected static final int NUM_ENTRIES = 10000;

    protected static final LogicalType[] KEY_TYPES =
            new LogicalType[] {
                new IntType(),
                new VarCharType(VarCharType.MAX_LENGTH),
                new DoubleType(),
                new BigIntType(),
                new BooleanType(),
                new FloatType(),
                new SmallIntType()
            };

    protected int needNumMemSegments(int numEntries, int valLen, int keyLen, int pageSize) {
        return 2 * (valLen + keyLen + 1024 * 3 + 4 + 8 + 8) * numEntries / pageSize;
    }

    protected int rowLength(RowType tpe) {
        return BinaryRowData.calculateFixPartSizeInBytes(tpe.getFieldCount())
                + BytesHashMap.getVariableLength(tpe.getChildren().toArray(new LogicalType[0]));
    }
}
