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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.MemorySegment;

/** Utilities related to {@link TypeComparator}. */
@Internal
public class NormalizedKeyUtil {

    public static void putByteNormalizedKey(
            byte value, MemorySegment target, int offset, int numBytes) {
        if (numBytes == 1) {
            // default case, full normalized key. need to explicitly convert to int to
            // avoid false results due to implicit type conversion to int when subtracting
            // the min byte value
            int highByte = value & 0xff;
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
        } else if (numBytes <= 0) {
        } else {
            int highByte = value & 0xff;
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
            for (int i = 1; i < numBytes; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    public static void putCharNormalizedKey(
            char value, MemorySegment target, int offset, int numBytes) {
        // note that the char is an unsigned data type in java and consequently needs
        // no code that transforms the signed representation to an offset representation
        // that is equivalent to unsigned, when compared byte by byte
        if (numBytes == 2) {
            // default case, full normalized key
            target.put(offset, (byte) ((value >>> 8) & 0xff));
            target.put(offset + 1, (byte) ((value) & 0xff));
        } else if (numBytes <= 0) {
        } else if (numBytes == 1) {
            target.put(offset, (byte) ((value >>> 8) & 0xff));
        } else {
            target.put(offset, (byte) ((value >>> 8) & 0xff));
            target.put(offset + 1, (byte) ((value) & 0xff));
            for (int i = 2; i < numBytes; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    public static void putBooleanNormalizedKey(
            boolean value, MemorySegment target, int offset, int numBytes) {
        if (numBytes > 0) {
            target.put(offset, (byte) (value ? 1 : 0));

            for (offset = offset + 1; numBytes > 1; numBytes--) {
                target.put(offset++, (byte) 0);
            }
        }
    }

    public static void putShortNormalizedKey(
            short value, MemorySegment target, int offset, int numBytes) {
        if (numBytes == 2) {
            // default case, full normalized key
            int highByte = ((value >>> 8) & 0xff);
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
            target.put(offset + 1, (byte) ((value) & 0xff));
        } else if (numBytes <= 0) {
        } else if (numBytes == 1) {
            int highByte = ((value >>> 8) & 0xff);
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
        } else {
            int highByte = ((value >>> 8) & 0xff);
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
            target.put(offset + 1, (byte) ((value) & 0xff));
            for (int i = 2; i < numBytes; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    public static void putIntNormalizedKey(
            int iValue, MemorySegment target, int offset, int numBytes) {
        putUnsignedIntegerNormalizedKey(iValue - Integer.MIN_VALUE, target, offset, numBytes);
    }

    public static void putUnsignedIntegerNormalizedKey(
            int value, MemorySegment target, int offset, int numBytes) {
        if (numBytes == 4) {
            // default case, full normalized key
            target.putIntBigEndian(offset, value);
        } else if (numBytes > 0) {
            if (numBytes < 4) {
                for (int i = 0; numBytes > 0; numBytes--, i++) {
                    target.put(offset + i, (byte) (value >>> ((3 - i) << 3)));
                }
            } else {
                target.putIntBigEndian(offset, value);
                for (int i = 4; i < numBytes; i++) {
                    target.put(offset + i, (byte) 0);
                }
            }
        }
    }

    public static void putLongNormalizedKey(
            long lValue, MemorySegment target, int offset, int numBytes) {
        putUnsignedLongNormalizedKey(lValue - Long.MIN_VALUE, target, offset, numBytes);
    }

    public static void putUnsignedLongNormalizedKey(
            long value, MemorySegment target, int offset, int numBytes) {
        if (numBytes == 8) {
            // default case, full normalized key
            target.putLongBigEndian(offset, value);
        } else if (numBytes > 0) {
            if (numBytes < 8) {
                for (int i = 0; numBytes > 0; numBytes--, i++) {
                    target.put(offset + i, (byte) (value >>> ((7 - i) << 3)));
                }
            } else {
                target.putLongBigEndian(offset, value);
                for (int i = 8; i < numBytes; i++) {
                    target.put(offset + i, (byte) 0);
                }
            }
        }
    }
}
