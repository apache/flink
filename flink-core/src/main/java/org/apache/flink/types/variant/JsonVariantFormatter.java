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
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;

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
 * nodes. {@link #LENIENT} backs {@link BinaryVariant#toString()} and never fails: it writes NaN as
 * {@code "NaN"}, a type this version does not know as {@code "<UNKNOWN>"}, and other undecodable
 * data as {@code "<INVALID>"}.
 */
@Internal
public final class JsonVariantFormatter {

    public static final JsonVariantFormatter STRICT = new JsonVariantFormatter(false);

    public static final JsonVariantFormatter LENIENT = new JsonVariantFormatter(true);

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private final boolean lenient;

    private JsonVariantFormatter(final boolean lenient) {
        this.lenient = lenient;
    }

    public String format(final BinaryVariant variant) {
        final StringBuilder sb = new StringBuilder();
        append(variant.rawValue(), variant.getMetadata(), variant.getPos(), sb);
        return sb.toString();
    }

    // Renders the node and its descendants via an explicit work stack, so deep nesting cannot
    // overflow.
    private void append(
            final byte[] value, final byte[] metadata, final int startPos, final StringBuilder sb) {
        final Deque<Object> stack = new ArrayDeque<>();
        stack.push(startPos);
        while (!stack.isEmpty()) {
            final Object top = stack.peek();
            if (top instanceof ContainerFrame) {
                final ContainerFrame frame = (ContainerFrame) top;
                if (frame.index == frame.childPositions.length) {
                    sb.append(frame.closing);
                    stack.pop();
                    continue;
                }
                if (frame.index != 0) {
                    sb.append(',');
                }
                if (frame.keys != null) {
                    sb.append(frame.keys[frame.index]);
                    sb.append(':');
                }
                stack.push(frame.childPositions[frame.index++]);
                continue;
            }
            final int pos = (Integer) top;
            stack.pop();
            final int start = sb.length();
            try {
                appendNode(value, metadata, pos, sb, stack);
            } catch (VariantTypeException e) {
                if (!lenient) {
                    throw e;
                }
                // Drop the node's partial output, such as a dangling key.
                sb.setLength(start);
                appendQuoted(sb, BinaryVariantUtil.undecodableNode(value, pos));
            }
        }
    }

    // Decodes a container's header before writing anything so a malformed one leaves sb untouched
    // for the lenient reset path.
    private void appendNode(
            byte[] value, byte[] metadata, int pos, StringBuilder sb, Deque<Object> stack) {
        switch (BinaryVariantUtil.getType(value, pos)) {
            case OBJECT:
                handleObject(
                        value,
                        pos,
                        (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                            final int[] childPositions = new int[size];
                            final String[] keys = new String[size];
                            for (int i = 0; i < size; ++i) {
                                int id = readUnsigned(value, idStart + idSize * i, idSize);
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                childPositions[i] = dataStart + offset;
                                keys[i] = escapeJson(getMetadataKey(metadata, id));
                            }
                            sb.append('{');
                            stack.push(new ContainerFrame(childPositions, keys, '}'));
                            return null;
                        });
                break;
            case ARRAY:
                handleArray(
                        value,
                        pos,
                        (size, offsetSize, offsetStart, dataStart) -> {
                            final int[] childPositions = new int[size];
                            for (int i = 0; i < size; ++i) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                childPositions[i] = dataStart + offset;
                            }
                            sb.append('[');
                            stack.push(new ContainerFrame(childPositions, null, ']'));
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
                // Reads the 1, 2, 4 or 8 bytes the header declares and widens them to a long.
                sb.append(BinaryVariantUtil.getLong(value, pos));
                break;
            case STRING:
                sb.append(escapeJson(BinaryVariantUtil.getString(value, pos)));
                break;
            case DOUBLE:
                {
                    final double d = BinaryVariantUtil.getDouble(value, pos);
                    if (Double.isFinite(d)) {
                        sb.append(d);
                    } else {
                        appendNonFinite(sb, Double.toString(d), d);
                    }
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
                    if (Float.isFinite(f)) {
                        sb.append(f);
                    } else {
                        appendNonFinite(sb, Float.toString(f), f);
                    }
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

    private static final class ContainerFrame {
        private final int[] childPositions;
        // Quoted "key" tokens for an object, or null for an array.
        private final String[] keys;
        private final char closing;
        private int index;

        private ContainerFrame(int[] childPositions, String[] keys, char closing) {
            this.childPositions = childPositions;
            this.keys = keys;
            this.closing = closing;
        }
    }

    /** JSON has no literal for NaN or infinity. */
    private void appendNonFinite(final StringBuilder sb, final String text, final double number) {
        if (!lenient) {
            throw new VariantTypeException(
                    String.format("Non-finite value '%s' cannot be serialized to JSON.", number));
        }
        appendQuoted(sb, text);
    }

    // Escape a string so that it can be pasted into JSON structure.
    // For example, if `str` only contains a new-line character, then the result content is "\n"
    // (4 characters).
    private static String escapeJson(String str) {
        try (CharArrayWriter writer = new CharArrayWriter();
                JsonGenerator gen = JSON_FACTORY.createGenerator(writer)) {
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
