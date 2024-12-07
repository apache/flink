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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.executiongraph.IndexRange;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.Iterator;

/** A collection of subpartition indexes. */
public class ResultSubpartitionIndexSet extends IndexRange {

    public ResultSubpartitionIndexSet(int index) {
        this(index, index);
    }

    public ResultSubpartitionIndexSet(IndexRange indexRange) {
        this(indexRange.getStartIndex(), indexRange.getEndIndex());
    }

    public ResultSubpartitionIndexSet(int startIndex, int endIndex) {
        super(startIndex, endIndex);
    }

    public boolean contains(int index) {
        return index >= startIndex && index <= endIndex;
    }

    public Iterable<Integer> values() {
        return () ->
                new Iterator<Integer>() {
                    private int index = startIndex;

                    @Override
                    public boolean hasNext() {
                        return index <= endIndex;
                    }

                    @Override
                    public Integer next() {
                        return index++;
                    }
                };
    }

    public void writeTo(ByteBuf target) {
        target.writeInt(startIndex);
        target.writeInt(endIndex);
    }

    public static int getByteBufLength(ResultSubpartitionIndexSet indexSet) {
        return Integer.BYTES * 2;
    }

    public static ResultSubpartitionIndexSet fromByteBuf(ByteBuf source) {
        int startIndex = source.readInt();
        int endIndex = source.readInt();
        return new ResultSubpartitionIndexSet(startIndex, endIndex);
    }
}
