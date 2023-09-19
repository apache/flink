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

package org.apache.flink.runtime.rest.handler.legacy.files;

import org.apache.flink.runtime.rest.handler.util.MimeTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the MIME types map. */
public class MimeTypesTest {

    @Test
    void testCompleteness() {
        try {
            assertThat(MimeTypes.getMimeTypeForExtension("txt")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("htm")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("html")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("css")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("js")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("json")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("png")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("jpg")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("jpeg")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("gif")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("woff")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("woff2")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("otf")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("ttf")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForExtension("eot")).isNotNull();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testFileNameExtraction() {
        try {
            assertThat(MimeTypes.getMimeTypeForFileName("test.txt")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForFileName("t.txt")).isNotNull();
            assertThat(MimeTypes.getMimeTypeForFileName("first.second.third.txt")).isNotNull();

            assertThat(MimeTypes.getMimeTypeForFileName(".txt")).isNull();
            assertThat(MimeTypes.getMimeTypeForFileName("txt")).isNull();
            assertThat(MimeTypes.getMimeTypeForFileName("test.")).isNull();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
