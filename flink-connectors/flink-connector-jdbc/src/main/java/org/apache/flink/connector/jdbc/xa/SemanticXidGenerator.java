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
 *   <li>checkpoint id (4 bytes)
 *   <li>subtask index
 *   <li>8 random bytes to provide uniqueness across other jobs and apps (generated at startup using
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

    private transient byte[] gtridBuffer; // globalTransactionId = checkpoint id (long)
    private transient byte[] bqualBuffer; // branchQualifier = task index + random bytes

    @Override
    public void open() {
        bqualBuffer = getRandomBytes(Long.BYTES);
        gtridBuffer = new byte[Long.BYTES];
    }

    @Override
    public Xid generateXid(RuntimeContext runtimeContext, long checkpointId) {
        writeNumber(runtimeContext.getIndexOfThisSubtask(), gtridBuffer, 0);
        // deliberately write only 4 bytes of checkpoint id and rely on random generation
        writeNumber((int) checkpointId, gtridBuffer, Integer.BYTES);
        // relying on arrays copying inside XidImpl constructor
        return new XidImpl(FORMAT_ID, gtridBuffer, bqualBuffer);
    }

    private static void writeNumber(int number, byte[] dst, int dstOffset) {
        for (int i = dstOffset; i < dstOffset + Integer.BYTES; i++) {
            dst[i] = (byte) number;
            number >>>= Byte.SIZE;
        }
    }

    private byte[] getRandomBytes(int size) {
        byte[] bytes = new byte[size];
        SECURE_RANDOM.nextBytes(bytes);
        return bytes;
    }
}
