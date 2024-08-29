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

package org.apache.flink.table.runtime.util;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.MutableObjectIterator;

/** An iterator that returns the Key/Value pairs with identical value a given number of times. */
public final class ConstantsKeyValuePairsIterator implements MutableObjectIterator<BinaryRowData> {

    private final IntValue key;
    private final IntValue value;

    private int numLeft;

    public ConstantsKeyValuePairsIterator(int key, int value, int count) {
        this.key = new IntValue(key);
        this.value = new IntValue(value);
        this.numLeft = count;
    }

    @Override
    public BinaryRowData next(BinaryRowData reuse) {
        if (this.numLeft > 0) {
            this.numLeft--;

            BinaryRowWriter writer = new BinaryRowWriter(reuse);
            writer.writeInt(0, this.key.getValue());
            writer.writeInt(1, this.value.getValue());
            writer.complete();
            return reuse;
        } else {
            return null;
        }
    }

    @Override
    public BinaryRowData next() {
        return next(new BinaryRowData(2));
    }
}
