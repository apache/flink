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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.functions.XmlToVariantParser.XmlElement;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.util.ExceptionUtils;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

/**
 * Implementation of {@code PARSE_XML} and {@code TRY_PARSE_XML} for the generated code.
 *
 * <p>The calls on the same input share the document from {@link #read}, so that each document is
 * read only once, even if the calls differ in {@code force_array} or in their error handling.
 */
@Internal
public final class SqlXmlUtils {

    // A document can be large and contain personal data, so error messages only show its start.
    private static final int MAX_INPUT_LENGTH_IN_ERROR = 100;

    private SqlXmlUtils() {}

    /**
     * Reads the document. An invalid document is returned with its error, so that each call can
     * handle it.
     */
    public static ReadXml read(XmlToVariantParser parser, StringData xml) {
        final String text = xml.toString();
        try {
            return new ReadXml(text, parser.read(text), null);
        } catch (Throwable e) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
            return new ReadXml(text, null, e);
        }
    }

    public static Variant parseXml(ReadXml xml, boolean forceArray) {
        if (xml.error != null) {
            throw parseError(xml, xml.error);
        }
        try {
            return XmlToVariantParser.encode(xml.root, forceArray);
        } catch (Throwable e) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
            throw parseError(xml, e);
        }
    }

    public static @Nullable Variant tryParseXml(ReadXml xml, boolean forceArray) {
        if (xml.error != null) {
            return null;
        }
        try {
            return XmlToVariantParser.encode(xml.root, forceArray);
        } catch (Throwable e) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
            return null;
        }
    }

    private static TableRuntimeException parseError(ReadXml xml, Throwable cause) {
        return new TableRuntimeException(
                String.format(
                        "Failed to parse XML string: %s",
                        StringUtils.abbreviate(xml.text, MAX_INPUT_LENGTH_IN_ERROR)),
                cause);
    }

    /** A document returned by {@link #read}, or the error of an invalid one. */
    public static final class ReadXml {

        private final String text;
        private final @Nullable XmlElement root;
        private final @Nullable Throwable error;

        private ReadXml(String text, @Nullable XmlElement root, @Nullable Throwable error) {
            this.text = text;
            this.root = root;
            this.error = error;
        }
    }
}
