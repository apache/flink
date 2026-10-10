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

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SqlXmlUtils}. */
class SqlXmlUtilsTest {

    private final XmlToVariantParser parser = new XmlToVariantParser();

    @Test
    void testErrorMessageShowsShortInput() {
        assertThatThrownBy(() -> parseXml("<a>"))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage("Failed to parse XML string: <a>");
    }

    @Test
    void testErrorMessageShowsOnlyTheStartOfLongInput() {
        final String xml = "<a>" + "x".repeat(200);
        assertThatThrownBy(() -> parseXml(xml))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage("Failed to parse XML string: " + xml.substring(0, 97) + "...");
    }

    @Test
    void testTryParseXmlReturnsNullForInvalidInput() {
        assertThat(SqlXmlUtils.tryParseXml(read("<a>"), false)).isNull();
    }

    private Object parseXml(String xml) {
        return SqlXmlUtils.parseXml(read(xml), false);
    }

    private SqlXmlUtils.ReadXml read(String xml) {
        return SqlXmlUtils.read(parser, StringData.fromString(xml));
    }
}
