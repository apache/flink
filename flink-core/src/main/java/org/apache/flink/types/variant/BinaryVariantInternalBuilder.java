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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.exc.InputCoercionException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import static org.apache.flink.types.variant.BinaryVariantUtil.ARRAY;
import static org.apache.flink.types.variant.BinaryVariantUtil.BASIC_TYPE_MASK;
import static org.apache.flink.types.variant.BinaryVariantUtil.BINARY;
import static org.apache.flink.types.variant.BinaryVariantUtil.DATE;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL16;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL4;
import static org.apache.flink.types.variant.BinaryVariantUtil.DECIMAL8;
import static org.apache.flink.types.variant.BinaryVariantUtil.DOUBLE;
import static org.apache.flink.types.variant.BinaryVariantUtil.FALSE;
import static org.apache.flink.types.variant.BinaryVariantUtil.FLOAT;
import static org.apache.flink.types.variant.BinaryVariantUtil.INT1;
import static org.apache.flink.types.variant.BinaryVariantUtil.INT2;
import static org.apache.flink.types.variant.BinaryVariantUtil.INT4;
import static org.apache.flink.types.variant.BinaryVariantUtil.INT8;
import static org.apache.flink.types.variant.BinaryVariantUtil.LONG_STR;
import static org.apache.flink.types.variant.BinaryVariantUtil.MAX_DECIMAL16_PRECISION;
import static org.apache.flink.types.variant.BinaryVariantUtil.MAX_DECIMAL4_PRECISION;
import static org.apache.flink.types.variant.BinaryVariantUtil.MAX_DECIMAL8_PRECISION;
import static org.apache.flink.types.variant.BinaryVariantUtil.MAX_SHORT_STR_SIZE;
import static org.apache.flink.types.variant.BinaryVariantUtil.NULL;
import static org.apache.flink.types.variant.BinaryVariantUtil.OBJECT;
import static org.apache.flink.types.variant.BinaryVariantUtil.SIZE_LIMIT;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIMESTAMP;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIMESTAMP_LTZ;
import static org.apache.flink.types.variant.BinaryVariantUtil.TRUE;
import static org.apache.flink.types.variant.BinaryVariantUtil.U16_MAX;
import static org.apache.flink.types.variant.BinaryVariantUtil.U24_MAX;
import static org.apache.flink.types.variant.BinaryVariantUtil.U24_SIZE;
import static org.apache.flink.types.variant.BinaryVariantUtil.U32_SIZE;
import static org.apache.flink.types.variant.BinaryVariantUtil.U8_MAX;
import static org.apache.flink.types.variant.BinaryVariantUtil.VERSION;
import static org.apache.flink.types.variant.BinaryVariantUtil.arrayHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.checkIndex;
import static org.apache.flink.types.variant.BinaryVariantUtil.getMetadataKey;
import static org.apache.flink.types.variant.BinaryVariantUtil.handleArray;
import static org.apache.flink.types.variant.BinaryVariantUtil.handleObject;
import static org.apache.flink.types.variant.BinaryVariantUtil.objectHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.primitiveHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.readUnsigned;
import static org.apache.flink.types.variant.BinaryVariantUtil.shortStrHeader;
import static org.apache.flink.types.variant.BinaryVariantUtil.valueSize;
import static org.apache.flink.types.variant.BinaryVariantUtil.writeLong;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** The internal builder for {@link BinaryVariant}. */
@Internal
public class BinaryVariantInternalBuilder {

    public static final VariantTypeException VARIANT_SIZE_LIMIT_EXCEPTION =
            new VariantTypeException("VARIANT_SIZE_LIMIT");
    public static final VariantTypeException VARIANT_DUPLICATE_KEY_EXCEPTION =
            new VariantTypeException("VARIANT_DUPLICATE_KEY");

    public BinaryVariantInternalBuilder(boolean allowDuplicateKeys) {
        this.allowDuplicateKeys = allowDuplicateKeys;
    }

    /**
     * Parse a JSON string as a Variant value.
     *
     * @throws IOException if any JSON parsing error happens.
     */
    public static BinaryVariant parseJson(String json, boolean allowDuplicateKeys)
            throws IOException {
        try (JsonParser parser = new JsonFactory().createParser(json)) {
            parser.nextToken();
            return parseJson(parser, allowDuplicateKeys);
        }
    }

    /**
     * Similar {@link #parseJson(String, boolean)}, but takes a JSON parser instead of string input.
     */
    private static BinaryVariant parseJson(JsonParser parser, boolean allowDuplicateKeys)
            throws IOException {
        BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(allowDuplicateKeys);
        builder.buildJson(parser);
        return builder.build();
    }

    // Build the variant metadata from `dictionaryKeys` and return the variant result.
    public BinaryVariant build() {
        int numKeys = dictionaryKeys.size();
        // Use long to avoid overflow in accumulating lengths.
        long dictionaryStringSize = 0;
        for (byte[] key : dictionaryKeys) {
            dictionaryStringSize += key.length;
        }
        // Determine the number of bytes required per offset entry.
        // The largest offset is the one-past-the-end value, which is total string size. It's very
        // unlikely that the number of keys could be larger, but incorporate that into the
        // calculation
        // in case of pathological data.
        long maxSize = Math.max(dictionaryStringSize, numKeys);
        if (maxSize > SIZE_LIMIT) {
            throw VARIANT_SIZE_LIMIT_EXCEPTION;
        }
        int offsetSize = getIntegerSize((int) maxSize);

        int offsetStart = 1 + offsetSize;
        int stringStart = offsetStart + (numKeys + 1) * offsetSize;
        long metadataSize = stringStart + dictionaryStringSize;

        if (metadataSize > SIZE_LIMIT) {
            throw VARIANT_SIZE_LIMIT_EXCEPTION;
        }
        byte[] metadata = new byte[(int) metadataSize];
        int headerByte = VERSION | ((offsetSize - 1) << 6);
        writeLong(metadata, 0, headerByte, 1);
        writeLong(metadata, 1, numKeys, offsetSize);
        int currentOffset = 0;
        for (int i = 0; i < numKeys; ++i) {
            writeLong(metadata, offsetStart + i * offsetSize, currentOffset, offsetSize);
            byte[] key = dictionaryKeys.get(i);
            System.arraycopy(key, 0, metadata, stringStart + currentOffset, key.length);
            currentOffset += key.length;
        }
        writeLong(metadata, offsetStart + numKeys * offsetSize, currentOffset, offsetSize);
        return new BinaryVariant(Arrays.copyOfRange(writeBuffer, 0, writePos), metadata);
    }

    public void appendString(String str) {
        byte[] text = str.getBytes(StandardCharsets.UTF_8);
        boolean longStr = text.length > MAX_SHORT_STR_SIZE;
        checkCapacity((longStr ? 1 + U32_SIZE : 1) + text.length);
        if (longStr) {
            writeBuffer[writePos++] = primitiveHeader(LONG_STR);
            writeLong(writeBuffer, writePos, text.length, U32_SIZE);
            writePos += U32_SIZE;
        } else {
            writeBuffer[writePos++] = shortStrHeader(text.length);
        }
        System.arraycopy(text, 0, writeBuffer, writePos, text.length);
        writePos += text.length;
    }

    public void appendNull() {
        checkCapacity(1);
        writeBuffer[writePos++] = primitiveHeader(NULL);
    }

    public void appendBoolean(boolean b) {
        checkCapacity(1);
        writeBuffer[writePos++] = primitiveHeader(b ? TRUE : FALSE);
    }

    public void appendByte(byte b) {
        checkCapacity(1 + 1);
        writeBuffer[writePos++] = primitiveHeader(INT1);
        writeLong(writeBuffer, writePos, b, 1);
        writePos += 1;
    }

    public void appendShort(short s) {
        checkCapacity(1 + 2);
        writeBuffer[writePos++] = primitiveHeader(INT2);
        writeLong(writeBuffer, writePos, s, 2);
        writePos += 2;
    }

    public void appendInt(int i) {
        checkCapacity(1 + 4);
        writeBuffer[writePos++] = primitiveHeader(INT4);
        writeLong(writeBuffer, writePos, i, 4);
        writePos += 4;
    }

    public void appendLong(long l) {
        checkCapacity(1 + 8);
        writeBuffer[writePos++] = primitiveHeader(INT8);
        writeLong(writeBuffer, writePos, l, 8);
        writePos += 8;
    }

    public void appendNumeric(long l) {
        if (l == (byte) l) {
            appendByte((byte) l);
        } else if (l == (short) l) {
            appendShort((short) l);
        } else if (l == (int) l) {
            appendInt((int) l);
        } else {
            appendLong(l);
        }
    }

    public void appendDouble(double d) {
        checkCapacity(1 + 8);
        writeBuffer[writePos++] = primitiveHeader(DOUBLE);
        writeLong(writeBuffer, writePos, Double.doubleToLongBits(d), 8);
        writePos += 8;
    }

    // Append a decimal value to the variant builder. The caller should guarantee that its precision
    // and scale fit into `MAX_DECIMAL16_PRECISION`.
    public void appendDecimal(BigDecimal d) {
        checkCapacity(2 + 16);
        BigInteger unscaled = d.unscaledValue();
        if (d.scale() <= MAX_DECIMAL4_PRECISION && d.precision() <= MAX_DECIMAL4_PRECISION) {
            writeBuffer[writePos++] = primitiveHeader(DECIMAL4);
            writeBuffer[writePos++] = (byte) d.scale();
            writeLong(writeBuffer, writePos, unscaled.intValueExact(), 4);
            writePos += 4;
        } else if (d.scale() <= MAX_DECIMAL8_PRECISION && d.precision() <= MAX_DECIMAL8_PRECISION) {
            writeBuffer[writePos++] = primitiveHeader(DECIMAL8);
            writeBuffer[writePos++] = (byte) d.scale();
            writeLong(writeBuffer, writePos, unscaled.longValueExact(), 8);
            writePos += 8;
        } else {
            assert d.scale() <= MAX_DECIMAL16_PRECISION && d.precision() <= MAX_DECIMAL16_PRECISION;
            writeBuffer[writePos++] = primitiveHeader(DECIMAL16);
            writeBuffer[writePos++] = (byte) d.scale();
            // `toByteArray` returns a big-endian representation. We need to copy it reversely and
            // sign
            // extend it to 16 bytes.
            byte[] bytes = unscaled.toByteArray();
            for (int i = 0; i < bytes.length; ++i) {
                writeBuffer[writePos + i] = bytes[bytes.length - 1 - i];
            }
            byte sign = (byte) (bytes[0] < 0 ? -1 : 0);
            for (int i = bytes.length; i < 16; ++i) {
                writeBuffer[writePos + i] = sign;
            }
            writePos += 16;
        }
    }

    public void appendDate(int daysSinceEpoch) {
        checkCapacity(1 + 4);
        writeBuffer[writePos++] = primitiveHeader(DATE);
        writeLong(writeBuffer, writePos, daysSinceEpoch, 4);
        writePos += 4;
    }

    public void appendTimestampLtz(long microsSinceEpoch) {
        checkCapacity(1 + 8);
        writeBuffer[writePos++] = primitiveHeader(TIMESTAMP_LTZ);
        writeLong(writeBuffer, writePos, microsSinceEpoch, 8);
        writePos += 8;
    }

    public void appendTimestamp(long microsSinceEpoch) {
        checkCapacity(1 + 8);
        writeBuffer[writePos++] = primitiveHeader(TIMESTAMP);
        writeLong(writeBuffer, writePos, microsSinceEpoch, 8);
        writePos += 8;
    }

    public void appendFloat(float f) {
        checkCapacity(1 + 4);
        writeBuffer[writePos++] = primitiveHeader(FLOAT);
        writeLong(writeBuffer, writePos, Float.floatToIntBits(f), 4);
        writePos += 4;
    }

    public void appendBinary(byte[] binary) {
        checkCapacity(1 + U32_SIZE + binary.length);
        writeBuffer[writePos++] = primitiveHeader(BINARY);
        writeLong(writeBuffer, writePos, binary.length, U32_SIZE);
        writePos += U32_SIZE;
        System.arraycopy(binary, 0, writeBuffer, writePos, binary.length);
        writePos += binary.length;
    }

    // Add a key to the variant dictionary. If the key already exists, the dictionary is not
    // modified.
    // In either case, return the id of the key.
    public int addKey(String key) {
        int id;
        if (dictionary.containsKey(key)) {
            id = dictionary.get(key);
        } else {
            id = dictionaryKeys.size();
            dictionary.put(key, id);
            dictionaryKeys.add(key.getBytes(StandardCharsets.UTF_8));
        }
        return id;
    }

    // Return the current write position of the variant builder. It is used together with
    // `finishWritingObject` or `finishWritingArray`.
    public int getWritePos() {
        return writePos;
    }

    // Finish writing a variant object after all of its fields have already been written. The
    // process
    // is as follows:
    // 1. The caller calls `getWritePos` before writing any fields to obtain the `start` parameter.
    // 2. The caller appends all the object fields to the builder. In the meantime, it should
    // maintain
    // the `fields` parameter. Before appending each field, it should append an entry to `fields` to
    // record the offset of the field. The offset is computed as `getWritePos() - start`.
    // 3. The caller calls `finishWritingObject` to finish writing a variant object.
    //
    // This function is responsible to sort the fields by key. If there are duplicate field keys:
    // - when `allowDuplicateKeys` is true, the field with the greatest offset value (the last
    // appended one) is kept.
    // - otherwise, throw an exception.
    public void finishWritingObject(int start, ArrayList<FieldEntry> fields) {
        int size = fields.size();
        Collections.sort(fields);
        int maxId = size == 0 ? 0 : fields.get(0).id;
        if (allowDuplicateKeys) {
            int distinctPos = 0;
            // Maintain a list of distinct keys in-place.
            for (int i = 1; i < size; ++i) {
                maxId = Math.max(maxId, fields.get(i).id);
                if (fields.get(i).id == fields.get(i - 1).id) {
                    // Found a duplicate key. Keep the field with a greater offset.
                    if (fields.get(distinctPos).offset < fields.get(i).offset) {
                        fields.set(
                                distinctPos,
                                fields.get(distinctPos).withNewOffset(fields.get(i).offset));
                    }
                } else {
                    // Found a distinct key. Add the field to the list.
                    ++distinctPos;
                    fields.set(distinctPos, fields.get(i));
                }
            }
            if (distinctPos + 1 < fields.size()) {
                size = distinctPos + 1;
                // Resize `fields` to `size`.
                fields.subList(size, fields.size()).clear();
                // Sort the fields by offsets so that we can move the value data of each field to
                // the new
                // offset without overwriting the fields after it.
                fields.sort(Comparator.comparingInt(f -> f.offset));
                int currentOffset = 0;
                for (int i = 0; i < size; ++i) {
                    int oldOffset = fields.get(i).offset;
                    int fieldSize = valueSize(writeBuffer, start + oldOffset);
                    System.arraycopy(
                            writeBuffer,
                            start + oldOffset,
                            writeBuffer,
                            start + currentOffset,
                            fieldSize);
                    fields.set(i, fields.get(i).withNewOffset(currentOffset));
                    currentOffset += fieldSize;
                }
                writePos = start + currentOffset;
                // Change back to the sort order by field keys to meet the variant spec.
                Collections.sort(fields);
            }
        } else {
            for (int i = 1; i < size; ++i) {
                maxId = Math.max(maxId, fields.get(i).id);
                String key = fields.get(i).key;
                if (key.equals(fields.get(i - 1).key)) {
                    throw VARIANT_DUPLICATE_KEY_EXCEPTION;
                }
            }
        }
        int dataSize = writePos - start;
        boolean largeSize = size > U8_MAX;
        int sizeBytes = largeSize ? U32_SIZE : 1;
        int idSize = getIntegerSize(maxId);
        int offsetSize = getIntegerSize(dataSize);
        // The space for header byte, object size, id list, and offset list.
        int headerSize = 1 + sizeBytes + size * idSize + (size + 1) * offsetSize;
        checkCapacity(headerSize);
        // Shift the just-written field data to make room for the object header section.
        System.arraycopy(writeBuffer, start, writeBuffer, start + headerSize, dataSize);
        writePos += headerSize;
        writeBuffer[start] = objectHeader(largeSize, idSize, offsetSize);
        writeLong(writeBuffer, start + 1, size, sizeBytes);
        int idStart = start + 1 + sizeBytes;
        int offsetStart = idStart + size * idSize;
        for (int i = 0; i < size; ++i) {
            writeLong(writeBuffer, idStart + i * idSize, fields.get(i).id, idSize);
            writeLong(writeBuffer, offsetStart + i * offsetSize, fields.get(i).offset, offsetSize);
        }
        writeLong(writeBuffer, offsetStart + size * offsetSize, dataSize, offsetSize);
    }

    // Finish writing a variant array after all of its elements have already been written. The
    // process
    // is similar to that of `finishWritingObject`.
    public void finishWritingArray(int start, ArrayList<Integer> offsets) {
        int dataSize = writePos - start;
        int size = offsets.size();
        boolean largeSize = size > U8_MAX;
        int sizeBytes = largeSize ? U32_SIZE : 1;
        int offsetSize = getIntegerSize(dataSize);
        // The space for header byte, object size, and offset list.
        int headerSize = 1 + sizeBytes + (size + 1) * offsetSize;
        checkCapacity(headerSize);
        // Shift the just-written field data to make room for the header section.
        System.arraycopy(writeBuffer, start, writeBuffer, start + headerSize, dataSize);
        writePos += headerSize;
        writeBuffer[start] = arrayHeader(largeSize, offsetSize);
        writeLong(writeBuffer, start + 1, size, sizeBytes);
        int offsetStart = start + 1 + sizeBytes;
        for (int i = 0; i < size; ++i) {
            writeLong(writeBuffer, offsetStart + i * offsetSize, offsets.get(i), offsetSize);
        }
        writeLong(writeBuffer, offsetStart + size * offsetSize, dataSize, offsetSize);
    }

    // Append a variant value to the variant builder. We need to insert the keys in the input
    // variant
    // into the current variant dictionary and rebuild it with new field ids. For scalar values in
    // the
    // input variant, we can directly copy the binary slice.
    public void appendVariant(BinaryVariant v) {
        appendVariantImpl(v.getValue(), v.getMetadata(), v.getPos());
    }

    private void appendVariantImpl(byte[] value, byte[] metadata, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        switch (basicType) {
            case OBJECT:
                handleObject(
                        value,
                        pos,
                        (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                            ArrayList<FieldEntry> fields = new ArrayList<>(size);
                            int start = writePos;
                            for (int i = 0; i < size; ++i) {
                                int id = readUnsigned(value, idStart + idSize * i, idSize);
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                int elementPos = dataStart + offset;
                                String key = getMetadataKey(metadata, id);
                                int newId = addKey(key);
                                fields.add(new FieldEntry(key, newId, writePos - start));
                                appendVariantImpl(value, metadata, elementPos);
                            }
                            finishWritingObject(start, fields);
                            return null;
                        });
                break;
            case ARRAY:
                handleArray(
                        value,
                        pos,
                        (size, offsetSize, offsetStart, dataStart) -> {
                            ArrayList<Integer> offsets = new ArrayList<>(size);
                            int start = writePos;
                            for (int i = 0; i < size; ++i) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                int elementPos = dataStart + offset;
                                offsets.add(writePos - start);
                                appendVariantImpl(value, metadata, elementPos);
                            }
                            finishWritingArray(start, offsets);
                            return null;
                        });
                break;
            default:
                shallowAppendVariantImpl(value, pos);
                break;
        }
    }

    private void shallowAppendVariantImpl(byte[] value, int pos) {
        int size = valueSize(value, pos);
        checkIndex(pos + size - 1, value.length);
        checkCapacity(size);
        System.arraycopy(value, pos, writeBuffer, writePos, size);
        writePos += size;
    }

    private void checkCapacity(int additional) {
        int required = writePos + additional;
        if (required > writeBuffer.length) {
            // Allocate a new buffer with a capacity of the next power of 2 of `required`.
            int newCapacity = Integer.highestOneBit(required);
            newCapacity = newCapacity < required ? newCapacity * 2 : newCapacity;
            if (newCapacity > SIZE_LIMIT) {
                throw VARIANT_SIZE_LIMIT_EXCEPTION;
            }
            byte[] newValue = new byte[newCapacity];
            System.arraycopy(writeBuffer, 0, newValue, 0, writePos);
            writeBuffer = newValue;
        }
    }

    // Temporarily store the information of a field. We need to collect all fields in an JSON
    // object,
    // sort them by their keys, and build the variant object in sorted order.
    public static final class FieldEntry implements Comparable<FieldEntry> {
        final String key;
        final int id;
        final int offset;

        public FieldEntry(String key, int id, int offset) {
            this.key = key;
            this.id = id;
            this.offset = offset;
        }

        FieldEntry withNewOffset(int newOffset) {
            return new FieldEntry(key, id, newOffset);
        }

        @Override
        public int compareTo(FieldEntry other) {
            return key.compareTo(other.key);
        }
    }

    private void buildJson(JsonParser parser) throws IOException {
        JsonToken token = parser.currentToken();
        if (token == null) {
            throw new JsonParseException(parser, "Unexpected null token");
        }
        switch (token) {
            case START_OBJECT:
                {
                    ArrayList<FieldEntry> fields = new ArrayList<>();
                    int start = writePos;
                    while (parser.nextToken() != JsonToken.END_OBJECT) {
                        String key = parser.currentName();
                        parser.nextToken();
                        int id = addKey(key);
                        fields.add(new FieldEntry(key, id, writePos - start));
                        buildJson(parser);
                    }
                    finishWritingObject(start, fields);
                    break;
                }
            case START_ARRAY:
                {
                    ArrayList<Integer> offsets = new ArrayList<>();
                    int start = writePos;
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        offsets.add(writePos - start);
                        buildJson(parser);
                    }
                    finishWritingArray(start, offsets);
                    break;
                }
            case VALUE_STRING:
                appendString(parser.getText());
                break;
            case VALUE_NUMBER_INT:
                try {
                    appendNumeric(parser.getLongValue());
                } catch (InputCoercionException ignored) {
                    // If the value doesn't fit any integer type, parse it as decimal or floating
                    // instead.
                    parseFloatingPoint(parser);
                }
                break;
            case VALUE_NUMBER_FLOAT:
                parseFloatingPoint(parser);
                break;
            case VALUE_TRUE:
                appendBoolean(true);
                break;
            case VALUE_FALSE:
                appendBoolean(false);
                break;
            case VALUE_NULL:
                appendNull();
                break;
            default:
                throw new JsonParseException(parser, "Unexpected token " + token);
        }
    }

    // Choose the smallest unsigned integer type that can store `value`. It must be within
    // `[0, U24_MAX]`.
    private int getIntegerSize(int value) {
        assert value >= 0 && value <= U24_MAX;
        if (value <= U8_MAX) {
            return 1;
        }
        if (value <= U16_MAX) {
            return 2;
        }
        return U24_SIZE;
    }

    private void parseFloatingPoint(JsonParser parser) throws IOException {
        if (!tryParseDecimal(parser.getText())) {
            appendDouble(parser.getDoubleValue());
        }
    }

    // Try to parse a JSON number as a decimal. Return whether the parsing succeeds. The input must
    // only use the decimal format (an integer value with an optional '.' in it) and must not use
    // scientific notation. It also must fit into the precision limitation of decimal types.
    private boolean tryParseDecimal(String input) {
        for (int i = 0; i < input.length(); ++i) {
            char ch = input.charAt(i);
            if (ch != '-' && ch != '.' && !(ch >= '0' && ch <= '9')) {
                return false;
            }
        }
        BigDecimal d = new BigDecimal(input);
        if (d.scale() <= MAX_DECIMAL16_PRECISION && d.precision() <= MAX_DECIMAL16_PRECISION) {
            appendDecimal(d);
            return true;
        }
        return false;
    }

    // The write buffer in building the variant value. Its first `writePos` bytes has been written.
    private byte[] writeBuffer = new byte[128];
    private int writePos = 0;
    // Map keys to a monotonically increasing id.
    private final HashMap<String, Integer> dictionary = new HashMap<>();
    // Store all keys in `dictionary` in the order of id.
    private final ArrayList<byte[]> dictionaryKeys = new ArrayList<>();
    private final boolean allowDuplicateKeys;
}
