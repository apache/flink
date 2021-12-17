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

package org.apache.flink.runtime.iterative.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.hash.HashPartition;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

/**
 * {@link Iterator} over the build side entries of a {@link HashPartition}.
 *
 * @param <BT>
 */
public class HashPartitionIterator<BT, PT> implements MutableObjectIterator<BT> {

    private final Iterator<HashPartition<BT, PT>> partitions;

    private final TypeSerializer<BT> serializer;

    private HashPartition<BT, PT> currentPartition;

    public HashPartitionIterator(
            Iterator<HashPartition<BT, PT>> partitions, TypeSerializer<BT> serializer) {
        this.partitions = partitions;
        this.serializer = serializer;
        currentPartition = null;
    }

    @Override
    public BT next(BT reuse) throws IOException {
        if (currentPartition == null) {
            if (!partitions.hasNext()) {
                return null;
            }
            currentPartition = partitions.next();
            currentPartition.setReadPosition(0);
        }

        try {
            reuse = serializer.deserialize(reuse, currentPartition);
        } catch (EOFException e) {
            reuse = advanceAndRead(reuse);
        }

        return reuse;
    }

    @Override
    public BT next() throws IOException {
        if (currentPartition == null) {
            if (!partitions.hasNext()) {
                return null;
            }
            currentPartition = partitions.next();
            currentPartition.setReadPosition(0);
        }

        try {
            return serializer.deserialize(currentPartition);
        } catch (EOFException e) {
            return advanceAndRead();
        }
    }

    /* jump to the next partition and continue reading from that */
    private BT advanceAndRead(BT reuse) throws IOException {
        if (!partitions.hasNext()) {
            return null;
        }
        currentPartition = partitions.next();
        currentPartition.setReadPosition(0);

        try {
            reuse = serializer.deserialize(reuse, currentPartition);
        } catch (EOFException e) {
            reuse = advanceAndRead(reuse);
        }
        return reuse;
    }

    /* jump to the next partition and continue reading from that */
    private BT advanceAndRead() throws IOException {
        if (!partitions.hasNext()) {
            return null;
        }
        currentPartition = partitions.next();
        currentPartition.setReadPosition(0);

        try {
            return serializer.deserialize(currentPartition);
        } catch (EOFException e) {
            return advanceAndRead();
        }
    }
}
