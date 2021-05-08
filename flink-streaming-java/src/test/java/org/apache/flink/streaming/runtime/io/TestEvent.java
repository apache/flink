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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;

/** A simple task event, used for validation of buffer or event blocking/buffering. */
public class TestEvent extends AbstractEvent {

    private long magicNumber;

    private byte[] payload;

    public TestEvent() {}

    public TestEvent(long magicNumber, byte[] payload) {
        this.magicNumber = magicNumber;
        this.payload = payload;
    }

    // ------------------------------------------------------------------------
    //  Serialization
    // ------------------------------------------------------------------------

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeLong(magicNumber);
        out.writeInt(payload.length);
        out.write(payload);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.magicNumber = in.readLong();
        this.payload = new byte[in.readInt()];
        in.readFully(this.payload);
    }

    // ------------------------------------------------------------------------
    //  Standard utilities
    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Long.valueOf(magicNumber).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj.getClass() == TestEvent.class) {
            TestEvent that = (TestEvent) obj;
            return this.magicNumber == that.magicNumber
                    && Arrays.equals(this.payload, that.payload);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "TestEvent %d (%s)", magicNumber, StringUtils.byteToHexString(payload));
    }
}
