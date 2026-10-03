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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.flink.types.variant.BinaryVariantUtil.BINARY_SEARCH_THRESHOLD;
import static org.apache.flink.types.variant.BinaryVariantUtil.SIZE_LIMIT;
import static org.apache.flink.types.variant.BinaryVariantUtil.VERSION;
import static org.apache.flink.types.variant.BinaryVariantUtil.VERSION_MASK;
import static org.apache.flink.types.variant.BinaryVariantUtil.checkIndex;
import static org.apache.flink.types.variant.BinaryVariantUtil.getMetadataKey;
import static org.apache.flink.types.variant.BinaryVariantUtil.handleArray;
import static org.apache.flink.types.variant.BinaryVariantUtil.handleObject;
import static org.apache.flink.types.variant.BinaryVariantUtil.malformedVariant;
import static org.apache.flink.types.variant.BinaryVariantUtil.microsToInstant;
import static org.apache.flink.types.variant.BinaryVariantUtil.nanosToInstant;
import static org.apache.flink.types.variant.BinaryVariantUtil.readUnsigned;
import static org.apache.flink.types.variant.BinaryVariantUtil.valueSize;
import static org.apache.flink.types.variant.BinaryVariantUtil.variantConstructorSizeLimit;

/**
 * A data structure that represents a semi-structured value. It consists of two binary values: value
 * and metadata. The value encodes types and values, but not field names. The metadata currently
 * contains a version flag and a list of field names. We can extend/modify the detailed binary
 * format given the version flag.
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Variant
 *     Binary Encoding</a> for the detail layout of the data structure.
 */
@Internal
public final class BinaryVariant implements Variant {

    private static final long serialVersionUID = 1L;

    private final byte[] value;
    private final byte[] metadata;
    // The variant value doesn't use the whole `value` binary, but starts from its `pos` index and
    // spans a size of `valueSize(value, pos)`. This design avoids frequent copies of the value
    // binary when reading a sub-variant in the array/object element.
    private final int pos;

    public BinaryVariant(byte[] value, byte[] metadata) {
        this(value, metadata, 0);
    }

    private BinaryVariant(byte[] value, byte[] metadata, int pos) {
        this.value = value;
        this.metadata = metadata;
        this.pos = pos;
        // There is currently only one allowed version.
        if (metadata.length < 1 || (metadata[0] & VERSION_MASK) != VERSION) {
            throw malformedVariant();
        }
        // Don't attempt to use a Variant larger than 16 MiB. We'll never produce one, and it risks
        // memory instability.
        if (metadata.length > SIZE_LIMIT || value.length > SIZE_LIMIT) {
            throw variantConstructorSizeLimit();
        }
    }

    @Override
    public boolean isPrimitive() {
        return !isArray() && !isObject();
    }

    @Override
    public boolean isArray() {
        return getType() == Type.ARRAY;
    }

    @Override
    public boolean isObject() {
        return getType() == Type.OBJECT;
    }

    @Override
    public boolean isNull() {
        return getType() == Type.NULL;
    }

    @Override
    public Type getType() {
        return BinaryVariantUtil.getType(value, pos);
    }

    @Override
    public boolean getBoolean() throws VariantTypeException {
        checkType(Type.BOOLEAN, getType());
        return BinaryVariantUtil.getBoolean(value, pos);
    }

    @Override
    public byte getByte() throws VariantTypeException {
        checkType(Type.TINYINT, getType());
        return (byte) BinaryVariantUtil.getLong(value, pos);
    }

    @Override
    public short getShort() throws VariantTypeException {
        checkType(Type.SMALLINT, getType());
        return (short) BinaryVariantUtil.getLong(value, pos);
    }

    @Override
    public int getInt() throws VariantTypeException {
        checkType(Type.INT, getType());
        return (int) BinaryVariantUtil.getLong(value, pos);
    }

    @Override
    public long getLong() throws VariantTypeException {
        checkType(Type.BIGINT, getType());
        return BinaryVariantUtil.getLong(value, pos);
    }

    @Override
    public float getFloat() throws VariantTypeException {
        checkType(Type.FLOAT, getType());
        return BinaryVariantUtil.getFloat(value, pos);
    }

    @Override
    public BigDecimal getDecimal() throws VariantTypeException {
        checkType(Type.DECIMAL, getType());
        return BinaryVariantUtil.getDecimal(value, pos);
    }

    @Override
    public double getDouble() throws VariantTypeException {
        checkType(Type.DOUBLE, getType());
        return BinaryVariantUtil.getDouble(value, pos);
    }

    @Override
    public String getString() throws VariantTypeException {
        checkType(Type.STRING, getType());
        return BinaryVariantUtil.getString(value, pos);
    }

    @Override
    public LocalDate getDate() throws VariantTypeException {
        checkType(Type.DATE, getType());
        return LocalDate.ofEpochDay(BinaryVariantUtil.getLong(value, pos));
    }

    @Override
    public LocalDateTime getDateTime() throws VariantTypeException {
        Type type = getType();
        Instant instant;
        if (type == Type.TIMESTAMP) {
            instant = microsToInstant(BinaryVariantUtil.getLong(value, pos));
        } else if (type == Type.TIMESTAMP_NS) {
            instant = nanosToInstant(BinaryVariantUtil.getLong(value, pos));
        } else {
            throw new VariantTypeException(
                    String.format(
                            "Expected type %s or %s but got %s",
                            Type.TIMESTAMP, Type.TIMESTAMP_NS, type));
        }
        return instant.atZone(ZoneOffset.UTC).toLocalDateTime();
    }

    @Override
    public Instant getInstant() throws VariantTypeException {
        Type type = getType();
        if (type == Type.TIMESTAMP_LTZ) {
            return microsToInstant(BinaryVariantUtil.getLong(value, pos));
        } else if (type == Type.TIMESTAMP_LTZ_NS) {
            return nanosToInstant(BinaryVariantUtil.getLong(value, pos));
        }
        throw new VariantTypeException(
                String.format(
                        "Expected type %s or %s but got %s",
                        Type.TIMESTAMP_LTZ, Type.TIMESTAMP_LTZ_NS, type));
    }

    @Override
    public LocalTime getTime() throws VariantTypeException {
        checkType(Type.TIME, getType());
        return LocalTime.ofNanoOfDay(BinaryVariantUtil.getLong(value, pos) * 1000);
    }

    @Override
    public byte[] getBytes() throws VariantTypeException {
        checkType(Type.BYTES, getType());
        return BinaryVariantUtil.getBinary(value, pos);
    }

    @Override
    public UUID getUuid() throws VariantTypeException {
        checkType(Type.UUID, getType());
        return BinaryVariantUtil.getUuid(value, pos);
    }

    @Override
    public Object get() throws VariantTypeException {
        switch (getType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return getBoolean();
            case TINYINT:
                return getByte();
            case SMALLINT:
                return getShort();
            case INT:
                return getInt();
            case BIGINT:
                return getLong();
            case FLOAT:
                return getFloat();
            case DOUBLE:
                return getDouble();
            case DECIMAL:
                return getDecimal();
            case STRING:
                return getString();
            case DATE:
                return getDate();
            case TIME:
                return getTime();
            case TIMESTAMP:
            case TIMESTAMP_NS:
                return getDateTime();
            case TIMESTAMP_LTZ:
            case TIMESTAMP_LTZ_NS:
                return getInstant();
            case BYTES:
                return getBytes();
            case UUID:
                return getUuid();
            default:
                throw new VariantTypeException(
                        String.format("Expecting a primitive variant but got %s", getType()));
        }
    }

    @Override
    public <T> T getAs() throws VariantTypeException {
        return (T) get();
    }

    @Override
    public Variant getElement(int index) throws VariantTypeException {
        return getElementAtIndex(index);
    }

    @Override
    public int getArraySize() throws VariantTypeException {
        return handleArray(value, pos, (size, offsetSize, offsetStart, dataStart) -> size);
    }

    @Override
    public Variant getField(String fieldName) throws VariantTypeException {
        return getFieldByKey(fieldName);
    }

    @Override
    public List<String> getFieldNames() throws VariantTypeException {
        return handleObject(
                value,
                pos,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    List<String> fieldNames = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        int id = readUnsigned(value, idStart + idSize * i, idSize);
                        String fieldName = getMetadataKey(metadata, id);
                        fieldNames.add(fieldName);
                    }
                    return fieldNames;
                });
    }

    @Override
    public String toJson() {
        return JsonVariantFormatter.STRICT.format(this);
    }

    public byte[] getValue() {
        if (pos == 0) {
            return value;
        }
        int size = valueSize(value, pos);
        checkIndex(pos + size - 1, value.length);
        return Arrays.copyOfRange(value, pos, pos + size);
    }

    /** Returns the value buffer without copying. This variant starts at {@link #getPos()}. */
    byte[] rawValue() {
        return value;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public int getPos() {
        return pos;
    }

    private void checkType(Type expected, Type actual) {
        if (expected != actual) {
            throw new VariantTypeException(
                    String.format("Expected type %s but got %s", expected, actual));
        }
    }

    // Find the field value whose key is equal to `key`. Return null if the key is not found.
    // It is only legal to call it when `getType()` is `Type.OBJECT`.
    private BinaryVariant getFieldByKey(String key) {
        return handleObject(
                value,
                pos,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    // Use linear search for a short list. Switch to binary search when the length
                    // reaches `BINARY_SEARCH_THRESHOLD`.
                    if (size < BINARY_SEARCH_THRESHOLD) {
                        for (int i = 0; i < size; ++i) {
                            int id = readUnsigned(value, idStart + idSize * i, idSize);
                            if (key.equals(getMetadataKey(metadata, id))) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                return new BinaryVariant(value, metadata, dataStart + offset);
                            }
                        }
                    } else {
                        int low = 0;
                        int high = size - 1;
                        while (low <= high) {
                            // Use unsigned right shift to compute the middle of `low` and `high`.
                            // This is not only a performance optimization, because it can properly
                            // handle the case where `low + high` overflows int.
                            int mid = (low + high) >>> 1;
                            int id = readUnsigned(value, idStart + idSize * mid, idSize);
                            int cmp = getMetadataKey(metadata, id).compareTo(key);
                            if (cmp < 0) {
                                low = mid + 1;
                            } else if (cmp > 0) {
                                high = mid - 1;
                            } else {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * mid, offsetSize);
                                return new BinaryVariant(value, metadata, dataStart + offset);
                            }
                        }
                    }
                    return null;
                });
    }

    // Get the array element at the `index` slot. Return null if `index` is out of the bound of
    // `[0, arraySize())`.
    // It is only legal to call it when `getType()` is `Type.ARRAY`.
    private BinaryVariant getElementAtIndex(int index) {
        return handleArray(
                value,
                pos,
                (size, offsetSize, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        return null;
                    }
                    int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
                    return new BinaryVariant(value, metadata, dataStart + offset);
                });
    }

    @Override
    public String toString() {
        return JsonVariantFormatter.LENIENT.format(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BinaryVariant)) {
            return false;
        }
        BinaryVariant variant = (BinaryVariant) o;
        return getPos() == variant.getPos()
                && Objects.deepEquals(getValue(), variant.getValue())
                && Objects.deepEquals(getMetadata(), variant.getMetadata());
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(value), Arrays.hashCode(metadata), pos);
    }
}
