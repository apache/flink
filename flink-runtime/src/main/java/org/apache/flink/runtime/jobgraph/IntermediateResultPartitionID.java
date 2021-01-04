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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.topology.ResultID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/** Id identifying {@link IntermediateResultPartition}. */
public class IntermediateResultPartitionID implements ResultID {

    private static final long serialVersionUID = 1L;
    // Represent the number of bytes occupied when writes IntermediateResultPartitionID to the
    // ByteBuf.
    // It is the sum of two long types(lowerPart and upperPart of the intermediateDataSetID) and one
    // int type(partitionNum).
    private static final int BYTEBUF_LEN = 20;

    private final IntermediateDataSetID intermediateDataSetID;
    private final int partitionNum;

    /** Creates an new random intermediate result partition ID for testing. */
    @VisibleForTesting
    public IntermediateResultPartitionID() {
        this.partitionNum = -1;
        this.intermediateDataSetID = new IntermediateDataSetID();
    }

    /**
     * Creates an new intermediate result partition ID with {@link IntermediateDataSetID} and the
     * partitionNum.
     */
    public IntermediateResultPartitionID(
            IntermediateDataSetID intermediateDataSetID, int partitionNum) {
        this.intermediateDataSetID = intermediateDataSetID;
        this.partitionNum = partitionNum;
    }

    public int getPartitionNumber() {
        return partitionNum;
    }

    public IntermediateDataSetID getIntermediateDataSetID() {
        return intermediateDataSetID;
    }

    public void writeTo(ByteBuf buf) {
        intermediateDataSetID.writeTo(buf);
        buf.writeInt(partitionNum);
    }

    public static IntermediateResultPartitionID fromByteBuf(ByteBuf buf) {
        final IntermediateDataSetID intermediateDataSetID = IntermediateDataSetID.fromByteBuf(buf);
        final int partitionNum = buf.readInt();
        return new IntermediateResultPartitionID(intermediateDataSetID, partitionNum);
    }

    public static int getByteBufLength() {
        return BYTEBUF_LEN;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            IntermediateResultPartitionID that = (IntermediateResultPartitionID) obj;
            return that.intermediateDataSetID.equals(this.intermediateDataSetID)
                    && that.partitionNum == this.partitionNum;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.intermediateDataSetID.hashCode() ^ this.partitionNum;
    }

    @Override
    public String toString() {
        return intermediateDataSetID.toString() + "#" + partitionNum;
    }
}
