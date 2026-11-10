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

import javax.annotation.Nullable;

import java.util.List;

/**
 * The keys, attribute names, and namespace prefixes of the mapping between XML and variants, and
 * the boolean values of XML Schema, shared by {@link XmlToVariantParser} and {@link
 * VariantToXmlWriter}.
 */
final class XmlVariantNames {

    static final String ATTRIBUTE_PREFIX = "@";
    static final String TEXT_KEY = "$";
    static final String ORDER_KEY = "#";

    static final String XSI_NIL = "xsi:nil";
    static final String XSI_TYPE = "xsi:type";

    private XmlVariantNames() {}

    /**
     * Returns the value of an XML Schema boolean, for an xsi:type of boolean and for xsi:nil, or
     * null if the text isn't one.
     */
    static @Nullable Boolean parseXmlSchemaBoolean(String text) {
        switch (text) {
            case "true":
            case "1":
                return true;
            case "false":
            case "0":
                return false;
            default:
                return null;
        }
    }

    /**
     * The prefixes for XML Schema. {@code PARSE_XML} drops their declarations with the namespaces
     * of XML Schema, and keeps declarations with other namespaces. {@code XML_STRING} uses the
     * prefixes for xsi:nil and xsi:type, and declares them on the root element if needed.
     */
    enum SchemaPrefix {
        XSI("xsi", "http://www.w3.org/2001/XMLSchema-instance"),
        XS("xs", "http://www.w3.org/2001/XMLSchema");

        /** All values, since {@link #values()} copies them on every call. */
        static final List<SchemaPrefix> ALL = List.of(values());

        /** The prefix with its colon, e.g. {@code xsi:}. */
        final String prefix;

        /** The name of the attribute that declares the prefix, e.g. {@code xmlns:xsi}. */
        final String declaration;

        final String namespace;

        SchemaPrefix(String name, String namespace) {
            this.prefix = name + ":";
            this.declaration = "xmlns:" + name;
            this.namespace = namespace;
        }

        /** Whether the attribute declares a schema prefix with the namespace of XML Schema. */
        static boolean isSchemaDeclaration(String attribute, String value) {
            for (SchemaPrefix schemaPrefix : ALL) {
                if (schemaPrefix.declaration.equals(attribute)
                        && schemaPrefix.namespace.equals(value)) {
                    return true;
                }
            }
            return false;
        }
    }
}
