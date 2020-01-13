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

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;

import javax.transaction.xa.Xid;

import java.security.SecureRandom;

/**
 * Generates {@link Xid} from:
 *
 * <ol>
 *   <li>checkpoint id
 *   <li>subtask index
 *   <li>4 random bytes to provide uniqueness across other jobs and apps (generated at startup using
 *       {@link SecureRandom})
 * </ol>
 *
 * <p>Each {@link SemanticXidGenerator} instance MUST be used for only one Sink (otherwise Xids
 * could collide).
 */
@Internal
class SemanticXidGenerator implements XidGenerator {

    private static final long serialVersionUID = 1L;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final int FORMAT_ID = 201;
    private static final int BQUAL_DYN_PART_LEN =
            Integer.BYTES; // length of the branchQualifier dynamic part, which is task index
    private static final int BQUAL_DYN_PART_POS = 0; // and it's starting position

    private transient byte[] gtridBuffer; // globalTransactionId = checkpoint id (long)
    private transient byte[] bqualBuffer; // branchQualifier = task index + random bytes

    @Override
    public void open() {
        bqualBuffer = new byte[Long.BYTES];
        byte[] bqualStaticPart = getRandomBytes(bqualBuffer.length - BQUAL_DYN_PART_LEN);
        System.arraycopy(
                bqualStaticPart, 0, bqualBuffer, BQUAL_DYN_PART_LEN, bqualStaticPart.length);
        gtridBuffer = new byte[Long.BYTES];
    }

    @Override
    public Xid generateXid(RuntimeContext runtimeContext, long checkpointId) {
        writeNumber(
                runtimeContext.getIndexOfThisSubtask(),
                Integer.BYTES,
                bqualBuffer,
                BQUAL_DYN_PART_POS);
        writeNumber(checkpointId, Long.BYTES, gtridBuffer, 0);
        // relying on arrays copying inside XidImpl constructor
        return new XidImpl(FORMAT_ID, gtridBuffer, bqualBuffer);
    }

    private static void writeNumber(long number, int numberLength, byte[] dst, int dstPos) {
        for (int i = dstPos; i < numberLength; i++) {
            dst[i] = (byte) number;
            number >>= 8;
        }
    }

    private byte[] getRandomBytes(int size) {
        byte[] bytes = new byte[size];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }
}
