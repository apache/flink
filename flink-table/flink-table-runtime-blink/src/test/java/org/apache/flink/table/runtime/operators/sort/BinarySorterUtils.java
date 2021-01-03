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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.util.MutableObjectIterator;

/** Utils to test sorter. */
public class BinarySorterUtils {

    /** Mock reader for binary row. */
    public static class MockBinaryRowReader implements MutableObjectIterator<BinaryRowData> {

        private int size;
        private int count;
        private BinaryRowData row;
        private BinaryRowWriter writer;

        public MockBinaryRowReader(int size) {
            this.size = size;
            this.row = new BinaryRowData(2);
            this.writer = new BinaryRowWriter(row);
        }

        @Override
        public BinaryRowData next(BinaryRowData reuse) {
            return next();
        }

        @Override
        public BinaryRowData next() {
            if (count >= size) {
                return null;
            }
            writer.reset();
            writer.writeInt(0, count);
            writer.writeString(1, StringData.fromString(getString(count)));
            writer.complete();
            count++;
            return row;
        }
    }

    public static String getString(int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            builder.append(count);
        }
        return builder.toString();
    }
}
