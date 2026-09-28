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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Base64;

import static org.apache.flink.types.variant.BinaryVariantUtil.TIMESTAMP_FORMATTER;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIMESTAMP_LTZ_FORMATTER;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIME_FORMATTER;
import static org.apache.flink.types.variant.BinaryVariantUtil.getMetadataKey;
import static org.apache.flink.types.variant.BinaryVariantUtil.handleArray;
import static org.apache.flink.types.variant.BinaryVariantUtil.handleObject;
import static org.apache.flink.types.variant.BinaryVariantUtil.microsToInstant;
import static org.apache.flink.types.variant.BinaryVariantUtil.nanosToInstant;
import static org.apache.flink.types.variant.BinaryVariantUtil.readUnsigned;
import static org.apache.flink.types.variant.BinaryVariantUtil.unexpectedType;

/**
 * Renders a {@link BinaryVariant} as JSON.
 *
 * <p>{@link #STRICT} backs {@link Variant#toJson()} and fails on NaN, infinity, and undecodable
 * nodes.
 */
@Internal
public final class JsonVariantFormatter implements VariantFormatter {

    public static final JsonVariantFormatter STRICT = new JsonVariantFormatter();

    private JsonVariantFormatter() {}

    @Override
    public String format(final Variant variant) {
        final BinaryVariant binary = (BinaryVariant) variant;
        final StringBuilder sb = new StringBuilder();
        append(binary.rawValue(), binary.getMetadata(), binary.getPos(), sb);
        return sb.toString();
    }

    private void append(byte[] value, byte[] metadata, int pos, StringBuilder sb) {
        switch (BinaryVariantUtil.getType(value, pos)) {
            case OBJECT:
                handleObject(
                        value,
                        pos,
                        (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                            sb.append('{');
                            for (int i = 0; i < size; ++i) {
                                int id = readUnsigned(value, idStart + idSize * i, idSize);
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                int elementPos = dataStart + offset;
                                if (i != 0) {
                                    sb.append(',');
                                }
                                sb.append(escapeJson(getMetadataKey(metadata, id)));
                                sb.append(':');
                                append(value, metadata, elementPos, sb);
                            }
                            sb.append('}');
                            return null;
                        });
                break;
            case ARRAY:
                handleArray(
                        value,
                        pos,
                        (size, offsetSize, offsetStart, dataStart) -> {
                            sb.append('[');
                            for (int i = 0; i < size; ++i) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                int elementPos = dataStart + offset;
                                if (i != 0) {
                                    sb.append(',');
                                }
                                append(value, metadata, elementPos, sb);
                            }
                            sb.append(']');
                            return null;
                        });
                break;
            case NULL:
                sb.append("null");
                break;
            case BOOLEAN:
                sb.append(BinaryVariantUtil.getBoolean(value, pos));
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                sb.append(BinaryVariantUtil.getLong(value, pos));
                break;
            case STRING:
                sb.append(escapeJson(BinaryVariantUtil.getString(value, pos)));
                break;
            case DOUBLE:
                {
                    final double d = BinaryVariantUtil.getDouble(value, pos);
                    if (Double.isInfinite(d) || Double.isNaN(d)) {
                        throw new VariantTypeException(
                                String.format(
                                        "Non-finite value %s cannot be serialized to JSON.", d));
                    }
                    sb.append(d);
                    break;
                }
            case DECIMAL:
                sb.append(BinaryVariantUtil.getDecimal(value, pos).toPlainString());
                break;
            case DATE:
                appendQuoted(
                        sb,
                        LocalDate.ofEpochDay((int) BinaryVariantUtil.getLong(value, pos))
                                .toString());
                break;
            case TIMESTAMP_LTZ:
                appendQuoted(
                        sb,
                        TIMESTAMP_LTZ_FORMATTER.format(
                                microsToInstant(BinaryVariantUtil.getLong(value, pos))
                                        .atZone(ZoneOffset.UTC)));
                break;
            case TIMESTAMP:
                appendQuoted(
                        sb,
                        TIMESTAMP_FORMATTER.format(
                                microsToInstant(BinaryVariantUtil.getLong(value, pos))
                                        .atZone(ZoneOffset.UTC)));
                break;
            case TIME:
                appendQuoted(
                        sb,
                        TIME_FORMATTER.format(
                                LocalTime.ofNanoOfDay(
                                        BinaryVariantUtil.getLong(value, pos) * 1000)));
                break;
            case TIMESTAMP_LTZ_NS:
                appendQuoted(
                        sb,
                        TIMESTAMP_LTZ_FORMATTER.format(
                                nanosToInstant(BinaryVariantUtil.getLong(value, pos))
                                        .atZone(ZoneOffset.UTC)));
                break;
            case TIMESTAMP_NS:
                appendQuoted(
                        sb,
                        TIMESTAMP_FORMATTER.format(
                                nanosToInstant(BinaryVariantUtil.getLong(value, pos))
                                        .atZone(ZoneOffset.UTC)));
                break;
            case FLOAT:
                {
                    final float f = BinaryVariantUtil.getFloat(value, pos);
                    if (Float.isInfinite(f) || Float.isNaN(f)) {
                        throw new VariantTypeException(
                                String.format(
                                        "Non-finite value %s cannot be serialized to JSON.",
                                        (double) f));
                    }
                    sb.append(f);
                    break;
                }
            case BYTES:
                appendQuoted(
                        sb,
                        Base64.getEncoder()
                                .encodeToString(BinaryVariantUtil.getBinary(value, pos)));
                break;
            case UUID:
                appendQuoted(sb, BinaryVariantUtil.getUuid(value, pos).toString());
                break;
            default:
                throw unexpectedType(BinaryVariantUtil.getType(value, pos));
        }
    }

    // Escape a string so that it can be pasted into JSON structure.
    // For example, if `str` only contains a new-line character, then the result content is "\n"
    // (4 characters).
    private static String escapeJson(String str) {
        try (CharArrayWriter writer = new CharArrayWriter();
                JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
            gen.writeString(str);
            gen.flush();
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void appendQuoted(StringBuilder sb, String str) {
        sb.append('"');
        sb.append(str);
        sb.append('"');
    }
}
