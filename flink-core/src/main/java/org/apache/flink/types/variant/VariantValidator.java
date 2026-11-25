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

package org.apache.flink.types.variant;

import org.apache.flink.annotation.Internal;

import java.math.BigInteger;
import java.util.Arrays;

import static org.apache.flink.types.variant.BinaryVariantUtil.ARRAY;
import static org.apache.flink.types.variant.BinaryVariantUtil.BASIC_TYPE_BITS;
import static org.apache.flink.types.variant.BinaryVariantUtil.BASIC_TYPE_MASK;
import static org.apache.flink.types.variant.BinaryVariantUtil.BINARY;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL16;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL4;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL8;
import static org.apache.flink.types.variant.BinaryVariantUtil.LONG_STR;
import static org.apache.flink.types.variant.BinaryVariantUtil.MALFORMED_VARIANT;
import static org.apache.flink.types.variant.BinaryVariantUtil.MAX_DECIMAL16_PRECISION;
import static org.apache.flink.types.variant.BinaryVariantUtil.MAX_DECIMAL4_PRECISION;
import static org.apache.flink.types.variant.BinaryVariantUtil.MAX_DECIMAL8_PRECISION;
import static org.apache.flink.types.variant.BinaryVariantUtil.OBJECT;
import static org.apache.flink.types.variant.BinaryVariantUtil.SHORT_STR;
import static org.apache.flink.types.variant.BinaryVariantUtil.SIZE_LIMIT;
import static org.apache.flink.types.variant.BinaryVariantUtil.SORTED_STRINGS_MASK;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIME;
import static org.apache.flink.types.variant.BinaryVariantUtil.TYPE_INFO_MASK;
import static org.apache.flink.types.variant.BinaryVariantUtil.U32_SIZE;
import static org.apache.flink.types.variant.BinaryVariantUtil.UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT;
import static org.apache.flink.types.variant.BinaryVariantUtil.VARIANT_CONSTRUCTOR_SIZE_LIMIT;
import static org.apache.flink.types.variant.BinaryVariantUtil.VERSION;
import static org.apache.flink.types.variant.BinaryVariantUtil.VERSION_MASK;
import static org.apache.flink.types.variant.BinaryVariantUtil.arrayOffsetSize;
import static org.apache.flink.types.variant.BinaryVariantUtil.arraySizeBytes;
import static org.apache.flink.types.variant.BinaryVariantUtil.metadataOffsetSize;
import static org.apache.flink.types.variant.BinaryVariantUtil.objectIdSize;
import static org.apache.flink.types.variant.BinaryVariantUtil.objectOffsetSize;
import static org.apache.flink.types.variant.BinaryVariantUtil.objectSizeBytes;
import static org.apache.flink.types.variant.BinaryVariantUtil.primitiveSize;
import static org.apache.flink.types.variant.BinaryVariantUtil.readLong;
import static org.apache.flink.util.SwarUtils.isAscii;
import static org.apache.flink.util.Utf8Utils.firstInvalidUtf8ByteIndex;

/**
 * Checks whether a binary variant follows the Parquet Variant encoding spec.
 *
 * <p>A variant is valid when:
 *
 * <ul>
 *   <li>Its metadata is complete. If its header says the dictionary is sorted, the keys are sorted
 *       and unique.
 *   <li>Every value, at any depth, has a type this version knows, lies within its parent, and holds
 *       a value the getters accept.
 *   <li>The fields of each object are listed in the unsigned byte order of their keys, and no key
 *       appears twice.
 *   <li>Every dictionary key and string is UTF-8.
 * </ul>
 *
 * <p>Bits that the spec reserves are ignored.
 *
 * <p>Values may share bytes as long as the check reads no more bytes of the value than it has: one
 * for each value, plus the bytes of each string. A variant without shared bytes always stays within
 * that bound, because each of its values and strings has bytes of its own. The bound keeps the work
 * linear in the size of the variant.
 *
 * <p>Keys are compared directly until that costs about as much as sorting the dictionary. From then
 * on, the check sorts the dictionary once and compares the ranks of the keys, so many objects with
 * long keys can't make the check quadratic.
 *
 * <p>{@link #isValid} only answers yes or no and builds nothing, so it suits filtering many invalid
 * values. {@link #validate} throws an exception that names the first problem it finds.
 *
 * <p>An instance reuses its buffers between calls, so it must not be shared between threads.
 */
@Internal
public final class VariantValidator {

    private static final long MICROS_PER_DAY = 86_400_000_000L;

    private static final long MAX_DECIMAL4_UNSCALED =
            BigInteger.TEN.pow(MAX_DECIMAL4_PRECISION).longValueExact() - 1;
    private static final long MAX_DECIMAL8_UNSCALED =
            BigInteger.TEN.pow(MAX_DECIMAL8_PRECISION).longValueExact() - 1;

    // 10^38, the smallest unscaled value with more digits than DECIMAL16 allows, in two halves.
    private static final long TEN_POW_38_HIGH = 0x4B3B4CA85A86C47AL;
    private static final long TEN_POW_38_LOW = 0x098A224000000000L;

    // A frame holds the position of an object or array, the index of its next child, and the end
    // that its bytes must fit in.
    private static final int FRAME_SIZE = 3;
    private static final int INITIAL_STACK_SIZE = 32 * FRAME_SIZE;
    private static final int MAX_RETAINED_STACK_SIZE = 1024 * FRAME_SIZE;

    private static final int[] NO_KEYS = new int[0];
    private static final int MAX_RETAINED_KEYS = 1024;

    private int[] stack = new int[INITIAL_STACK_SIZE];

    // The metadata of the current call, and what the check found out about its dictionary.
    private byte[] metadata;
    private int dictionarySize;
    private int keyOffsetSize;
    private int keyStart;
    private boolean keysIncreasing;
    private long keyCompareBudget;
    private boolean keysRanked;

    // Once the keys are ranked, keyRanks holds the rank of each id in key order. Equal keys have
    // the same rank. keyOrder is the buffer the ranking sorts the ids in.
    private int[] keyOrder = NO_KEYS;
    private int[] keyRanks = NO_KEYS;

    // Why the last call returned false: a byte position in the value or a key id, see Problem.
    private Problem problem;
    private int problemAt;

    /** Returns whether the variant follows the spec. Never throws. */
    public boolean isValid(BinaryVariant variant) {
        return isValid(variant.rawValue(), variant.getMetadata(), variant.getPos());
    }

    /**
     * Returns whether {@code value} and {@code metadata} form a variant that follows the spec,
     * including the checks of the {@link BinaryVariant} constructor. Never throws.
     */
    public boolean isValid(byte[] value, byte[] metadata) {
        return isValid(value, metadata, 0);
    }

    /**
     * Checks that the variant follows the spec.
     *
     * @throws VariantTypeException if it doesn't. The message starts with the code that reading the
     *     variant throws, such as {@code MALFORMED_VARIANT}. It goes on with the first problem the
     *     check found and where, as a byte of the variant or a key of the metadata, but no data of
     *     the variant.
     */
    public void validate(BinaryVariant variant) {
        validate(variant.rawValue(), variant.getMetadata(), variant.getPos());
    }

    /**
     * Checks that {@code value} and {@code metadata} form a variant that follows the spec,
     * including the checks of the {@link BinaryVariant} constructor.
     *
     * @throws VariantTypeException if they don't, see {@link #validate(BinaryVariant)}
     */
    public void validate(byte[] value, byte[] metadata) {
        validate(value, metadata, 0);
    }

    private void validate(byte[] value, byte[] metadata, int pos) {
        if (isValid(value, metadata, pos)) {
            return;
        }
        // Positions in the value count from the start of the variant, as in getValue().
        final int at = problem.isInValue ? problemAt - pos : problemAt;
        final int typeId =
                problem == Problem.UNKNOWN_TYPE
                        ? (value[problemAt] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK
                        : 0;
        throw new VariantTypeException(String.format(problem.message, at, typeId));
    }

    private boolean isValid(byte[] value, byte[] metadata, int pos) {
        if (!isInBounds(value, metadata, pos)) {
            return false;
        }
        try {
            return isMetadataValid(metadata) && isValueValid(value, pos);
        } finally {
            this.metadata = null;
            keysRanked = false;
            // A deeply nested variant grows the stack, and a large dictionary the key buffers.
            // Don't keep that memory for later calls.
            if (stack.length > MAX_RETAINED_STACK_SIZE) {
                stack = new int[INITIAL_STACK_SIZE];
            }
            if (keyOrder.length > MAX_RETAINED_KEYS) {
                keyOrder = NO_KEYS;
                keyRanks = NO_KEYS;
            }
        }
    }

    private boolean isInBounds(byte[] value, byte[] metadata, int pos) {
        if (value.length > SIZE_LIMIT || metadata.length > SIZE_LIMIT) {
            return fail(Problem.TOO_LARGE, 0);
        }
        return (pos >= 0 && pos < value.length) || fail(Problem.NO_VALUE, 0);
    }

    /**
     * Returns whether the metadata has a known version and a complete dictionary of UTF-8 keys,
     * sorted and unique if its header says so. Keeps what later checks need about the dictionary.
     */
    private boolean isMetadataValid(byte[] metadata) {
        if (metadata.length == 0 || (metadata[0] & VERSION_MASK) != VERSION) {
            return fail(Problem.METADATA_VERSION, 0);
        }
        final int offsetSize = metadataOffsetSize(metadata);
        if (1 + offsetSize > metadata.length) {
            return fail(Problem.METADATA_TRUNCATED, 0);
        }
        final long size = readUnsigned(metadata, 1, offsetSize);
        final long stringStart = 1 + (size + 2) * offsetSize;
        if (stringStart > metadata.length) {
            return fail(Problem.METADATA_TRUNCATED, 0);
        }
        // The offsets end within the metadata, so the count and every position fit in an int.
        this.metadata = metadata;
        dictionarySize = (int) size;
        keyOffsetSize = offsetSize;
        keyStart = (int) stringStart;
        keysIncreasing = true;
        if (keyOffset(0) != 0) {
            return fail(Problem.FIRST_KEY_OFFSET, 0);
        }
        long start = 0;
        int firstUnsortedKey = 0;
        for (int id = 0; id < dictionarySize; id++) {
            final long end = keyOffset(id + 1);
            if (end < start || stringStart + end > metadata.length) {
                return fail(Problem.KEY_OUT_OF_BOUNDS, id);
            }
            if (!isUtf8(metadata, keyStart + (int) start, (int) (end - start))) {
                return fail(Problem.KEY_NOT_UTF8, id);
            }
            if (id > 0 && keysIncreasing && compareKeys(id - 1, id) >= 0) {
                keysIncreasing = false;
                firstUnsortedKey = id;
            }
            start = end;
        }
        if ((metadata[0] & SORTED_STRINGS_MASK) != 0 && !keysIncreasing) {
            return fail(Problem.KEYS_NOT_SORTED, firstUnsortedKey);
        }
        // Comparing keys directly may cost about as much as sorting them would.
        keyCompareBudget =
                (start + dictionarySize) * (32 - Integer.numberOfLeadingZeros(dictionarySize));
        return true;
    }

    private boolean isValueValid(byte[] value, int rootPos) {
        if (!isContainer(value[rootPos])) {
            return scalarStringSize(value, rootPos, value.length) >= 0;
        }
        // One for each value, plus the bytes of each string. See the class comment.
        final long maxReads = value.length - rootPos;
        long reads = 1;
        int depth = 0;
        int pos = rootPos;
        int limit = value.length;
        int next = 0;
        while (true) {
            final int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
            final boolean isObject = (value[pos] & BASIC_TYPE_MASK) == OBJECT;
            final int sizeBytes = isObject ? objectSizeBytes(typeInfo) : arraySizeBytes(typeInfo);
            final int idSize = isObject ? objectIdSize(typeInfo) : 0;
            final int offsetSize =
                    isObject ? objectOffsetSize(typeInfo) : arrayOffsetSize(typeInfo);
            if (pos + 1L + sizeBytes > limit) {
                return fail(Problem.CONTAINER_OUT_OF_BOUNDS, pos);
            }
            final long count = readUnsigned(value, pos + 1, sizeBytes);
            if (pos + 1L + sizeBytes + count * idSize + (count + 1) * offsetSize > limit) {
                return fail(Problem.CONTAINER_OUT_OF_BOUNDS, pos);
            }
            // The id and offset lists end at or before the limit, so their positions fit in an int.
            final int size = (int) count;
            final int idStart = pos + 1 + sizeBytes;
            final int offsetStart = idStart + size * idSize;
            final int dataStart = offsetStart + (size + 1) * offsetSize;
            final long dataSize = readUnsigned(value, offsetStart + size * offsetSize, offsetSize);
            if (dataStart + dataSize > limit) {
                return fail(Problem.CONTAINER_OUT_OF_BOUNDS, pos);
            }
            final int dataEnd = dataStart + (int) dataSize;

            boolean entered = false;
            while (next < size) {
                if (isObject && !isFieldKeyValid(value, pos, idStart, idSize, next)) {
                    return false;
                }
                final long offset =
                        readUnsigned(value, offsetStart + next * offsetSize, offsetSize);
                next++;
                if (offset >= dataSize) {
                    return fail(Problem.CHILD_OUT_OF_BOUNDS, pos);
                }
                if (++reads > maxReads) {
                    return fail(Problem.SHARED_BYTES, pos);
                }
                final int childPos = dataStart + (int) offset;
                if (!isContainer(value[childPos])) {
                    final long stringSize = scalarStringSize(value, childPos, dataEnd);
                    if (stringSize < 0) {
                        return false;
                    }
                    reads += stringSize;
                    if (reads > maxReads) {
                        return fail(Problem.SHARED_BYTES, pos);
                    }
                    continue;
                }
                // The child's header is checked against dataEnd when the walk enters it.
                push(depth++, pos, next, limit);
                pos = childPos;
                limit = dataEnd;
                next = 0;
                entered = true;
                break;
            }
            if (entered) {
                continue;
            }
            if (depth == 0) {
                return true;
            }
            depth--;
            final int frame = depth * FRAME_SIZE;
            pos = stack[frame];
            next = stack[frame + 1];
            limit = stack[frame + 2];
        }
    }

    private void push(int depth, int pos, int next, int limit) {
        final int frame = depth * FRAME_SIZE;
        if (frame + FRAME_SIZE > stack.length) {
            stack = Arrays.copyOf(stack, stack.length * 2);
        }
        stack[frame] = pos;
        stack[frame + 1] = next;
        stack[frame + 2] = limit;
    }

    /**
     * Returns whether the key of field {@code index} of the object at {@code pos} is in the
     * dictionary and comes after the key of the field before it.
     */
    private boolean isFieldKeyValid(byte[] value, int pos, int idStart, int idSize, int index) {
        final long id = readUnsigned(value, idStart + index * idSize, idSize);
        if (id >= dictionarySize) {
            return fail(Problem.UNKNOWN_KEY, pos);
        }
        if (index == 0) {
            return true;
        }
        final int previous = (int) readUnsigned(value, idStart + (index - 1) * idSize, idSize);
        return isKeyAfter(previous, (int) id) || fail(Problem.FIELDS_NOT_SORTED, pos);
    }

    private boolean isKeyAfter(int previous, int id) {
        if (keysIncreasing) {
            return previous < id;
        }
        if (!keysRanked && keyCompareBudget < 0) {
            rankKeys();
        }
        if (keysRanked) {
            return keyRanks[previous] < keyRanks[id];
        }
        return compareKeys(previous, id) < 0;
    }

    /** Sorts the ids by their keys with a merge sort, then gives equal keys the same rank. */
    private void rankKeys() {
        final int size = dictionarySize;
        if (keyOrder.length < size) {
            keyOrder = new int[size];
            keyRanks = new int[size];
        }
        int[] sorted = keyOrder;
        int[] buffer = keyRanks;
        for (int i = 0; i < size; i++) {
            sorted[i] = i;
        }
        for (int width = 1; width < size; width *= 2) {
            for (int low = 0; low < size; low += 2 * width) {
                merge(
                        sorted,
                        buffer,
                        low,
                        Math.min(low + width, size),
                        Math.min(low + 2 * width, size));
            }
            final int[] merged = buffer;
            buffer = sorted;
            sorted = merged;
        }
        // The buffer is free again and becomes the rank of each id.
        int rank = 0;
        for (int i = 0; i < size; i++) {
            if (i > 0 && compareKeys(sorted[i - 1], sorted[i]) < 0) {
                rank++;
            }
            buffer[sorted[i]] = rank;
        }
        keyOrder = sorted;
        keyRanks = buffer;
        keysRanked = true;
    }

    /** Merges the sorted runs {@code [low, middle)} and {@code [middle, high)} of {@code from}. */
    private void merge(int[] from, int[] to, int low, int middle, int high) {
        int left = low;
        int right = middle;
        for (int i = low; i < high; i++) {
            if (left < middle && (right >= high || compareKeys(from[left], from[right]) <= 0)) {
                to[i] = from[left++];
            } else {
                to[i] = from[right++];
            }
        }
    }

    /**
     * Compares the keys of two ids by their unsigned UTF-8 bytes, and charges the bytes it may read
     * to the comparison budget.
     */
    private int compareKeys(int a, int b) {
        final int aStart = keyStart + (int) keyOffset(a);
        final int aEnd = keyStart + (int) keyOffset(a + 1);
        final int bStart = keyStart + (int) keyOffset(b);
        final int bEnd = keyStart + (int) keyOffset(b + 1);
        keyCompareBudget -= Math.min(aEnd - aStart, bEnd - bStart) + 1;
        return Arrays.compareUnsigned(metadata, aStart, aEnd, metadata, bStart, bEnd);
    }

    private long keyOffset(int index) {
        return readUnsigned(metadata, 1 + (index + 1) * keyOffsetSize, keyOffsetSize);
    }

    /**
     * Returns -1 if the primitive or short string at {@code pos} ends past {@code limit} or holds a
     * value that the getters reject, else the number of bytes of its string, 0 for other types.
     */
    private long scalarStringSize(byte[] value, int pos, int limit) {
        final int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if ((value[pos] & BASIC_TYPE_MASK) == SHORT_STR) {
            return utf8StringSize(value, pos, pos + 1, typeInfo, limit);
        }
        if (typeInfo == BINARY || typeInfo == LONG_STR) {
            if (!fits(pos, 1 + U32_SIZE, limit)) {
                return failScalar(Problem.SCALAR_OUT_OF_BOUNDS, pos);
            }
            final long length = readUnsigned(value, pos + 1, U32_SIZE);
            if (typeInfo == LONG_STR) {
                return utf8StringSize(value, pos, pos + 1 + U32_SIZE, length, limit);
            }
            return fits(pos, 1 + U32_SIZE + length, limit)
                    ? 0
                    : failScalar(Problem.SCALAR_OUT_OF_BOUNDS, pos);
        }
        final int size = primitiveSize(typeInfo);
        if (size < 0) {
            return failScalar(Problem.UNKNOWN_TYPE, pos);
        }
        if (!fits(pos, size, limit)) {
            return failScalar(Problem.SCALAR_OUT_OF_BOUNDS, pos);
        }
        final boolean valid;
        switch (typeInfo) {
            case TIME:
                return isTimeValid(readLong(value, pos + 1, 8))
                        ? 0
                        : failScalar(Problem.TIME_OUT_OF_RANGE, pos);
            case DECIMAL4:
                valid =
                        isScaleValid(value, pos, MAX_DECIMAL4_PRECISION)
                                && Math.abs(readLong(value, pos + 2, 4)) <= MAX_DECIMAL4_UNSCALED;
                break;
            case DECIMAL8:
                valid =
                        isScaleValid(value, pos, MAX_DECIMAL8_PRECISION)
                                && isDecimal8UnscaledValid(readLong(value, pos + 2, 8));
                break;
            case DECIMAL16:
                valid =
                        isScaleValid(value, pos, MAX_DECIMAL16_PRECISION)
                                && isDecimal16UnscaledValid(value, pos + 2);
                break;
            default:
                return 0;
        }
        return valid ? 0 : failScalar(Problem.DECIMAL_OUT_OF_RANGE, pos);
    }

    /**
     * Returns the length of the string at {@code start} that the value at {@code pos} holds, or -1
     * if it ends past {@code limit} or isn't UTF-8.
     */
    private long utf8StringSize(byte[] value, int pos, int start, long length, int limit) {
        if (!fits(start, length, limit)) {
            return failScalar(Problem.SCALAR_OUT_OF_BOUNDS, pos);
        }
        return isUtf8(value, start, (int) length)
                ? length
                : failScalar(Problem.STRING_NOT_UTF8, pos);
    }

    private static boolean isUtf8(byte[] bytes, int start, int length) {
        // Most text is ASCII, which the SWAR scan checks 8 bytes at a time.
        return isAscii(bytes, start, length) || firstInvalidUtf8ByteIndex(bytes, start, length) < 0;
    }

    private static boolean fits(int pos, long size, int limit) {
        return pos + size <= limit;
    }

    private static boolean isTimeValid(long micros) {
        return micros >= 0 && micros < MICROS_PER_DAY;
    }

    private static boolean isScaleValid(byte[] value, int pos, int maxPrecision) {
        return (value[pos + 1] & 0xFF) <= maxPrecision;
    }

    private static boolean isDecimal8UnscaledValid(long unscaled) {
        return unscaled >= -MAX_DECIMAL8_UNSCALED && unscaled <= MAX_DECIMAL8_UNSCALED;
    }

    /**
     * Returns whether the 16-byte little-endian unscaled value at {@code pos} has 38 digits at
     * most.
     */
    private static boolean isDecimal16UnscaledValid(byte[] value, int pos) {
        long low = readLong(value, pos, 8);
        long high = readLong(value, pos + 8, 8);
        if (high < 0) {
            // Negate the two's complement. The carry reaches the high half only when low is 0.
            low = -low;
            high = ~high + (low == 0 ? 1 : 0);
        }
        final int compareHigh = Long.compareUnsigned(high, TEN_POW_38_HIGH);
        return compareHigh < 0
                || (compareHigh == 0 && Long.compareUnsigned(low, TEN_POW_38_LOW) < 0);
    }

    private static boolean isContainer(byte header) {
        final int basicType = header & BASIC_TYPE_MASK;
        return basicType == OBJECT || basicType == ARRAY;
    }

    /**
     * Reads a little-endian unsigned integer of {@code numBytes} bytes. The caller checks that they
     * are in bounds.
     */
    private static long readUnsigned(byte[] bytes, int pos, int numBytes) {
        long result = 0;
        for (int i = 0; i < numBytes; i++) {
            result |= (bytes[pos + i] & 0xFFL) << (8 * i);
        }
        return result;
    }

    /** Records why the variant is invalid and returns false. Builds no message. */
    private boolean fail(Problem problem, int at) {
        this.problem = problem;
        problemAt = at;
        return false;
    }

    private long failScalar(Problem problem, int pos) {
        fail(problem, pos);
        return -1;
    }

    /**
     * A reason why a variant is invalid. The message gets a byte position of the value, or a key id
     * for a problem of the metadata, and the type id for {@link #UNKNOWN_TYPE}.
     */
    private enum Problem {
        TOO_LARGE(
                false,
                VARIANT_CONSTRUCTOR_SIZE_LIMIT,
                "The value or the metadata is larger than 16 MiB."),
        NO_VALUE(false, MALFORMED_VARIANT, "The value ends before the variant starts."),
        METADATA_VERSION(
                false, MALFORMED_VARIANT, "The metadata is empty or its version is not 1."),
        METADATA_TRUNCATED(false, MALFORMED_VARIANT, "The metadata ends before its key offsets."),
        FIRST_KEY_OFFSET(
                false, MALFORMED_VARIANT, "The first key offset of the metadata is not 0."),
        KEY_OUT_OF_BOUNDS(
                false,
                MALFORMED_VARIANT,
                "Key %d of the metadata ends before it starts or past the end "
                        + "of the metadata."),
        KEY_NOT_UTF8(false, MALFORMED_VARIANT, "Key %d of the metadata is not valid UTF-8."),
        KEYS_NOT_SORTED(
                false,
                MALFORMED_VARIANT,
                "The metadata says that its keys are sorted, but key %d "
                        + "doesn't come after the key before it."),
        CONTAINER_OUT_OF_BOUNDS(
                true,
                MALFORMED_VARIANT,
                "The object or array at byte %d ends past its parent or past "
                        + "the end of the value."),
        CHILD_OUT_OF_BOUNDS(
                true,
                MALFORMED_VARIANT,
                "A child of the object or array at byte %d starts past the end " + "of its data."),
        UNKNOWN_KEY(
                true,
                MALFORMED_VARIANT,
                "A field of the object at byte %d refers to a key that the "
                        + "metadata doesn't have."),
        FIELDS_NOT_SORTED(
                true,
                MALFORMED_VARIANT,
                "The fields of the object at byte %d are not sorted by key, or "
                        + "a key appears twice."),
        SHARED_BYTES(
                true,
                MALFORMED_VARIANT,
                "The children of the object or array at byte %d share bytes, "
                        + "so that reading the variant reads more bytes than the value has."),
        SCALAR_OUT_OF_BOUNDS(
                true,
                MALFORMED_VARIANT,
                "The value at byte %d ends past its parent or past the end of " + "the value."),
        UNKNOWN_TYPE(
                true,
                UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT,
                "The value at byte %d has the type id %d, " + "which this version doesn't know."),
        STRING_NOT_UTF8(true, MALFORMED_VARIANT, "The string at byte %d is not valid UTF-8."),
        TIME_OUT_OF_RANGE(true, MALFORMED_VARIANT, "The TIME at byte %d is not within one day."),
        DECIMAL_OUT_OF_RANGE(
                true,
                MALFORMED_VARIANT,
                "The DECIMAL at byte %d has more digits or a larger scale "
                        + "than its width allows.");

        private final boolean isInValue;
        private final String message;

        Problem(boolean isInValue, String code, String reason) {
            this.isInValue = isInValue;
            this.message = code + ": " + reason;
        }
    }
}
