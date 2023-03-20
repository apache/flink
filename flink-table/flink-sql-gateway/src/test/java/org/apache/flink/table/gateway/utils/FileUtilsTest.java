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

package org.apache.flink.table.gateway.utils;

import org.apache.flink.table.gateway.service.utils.FileUtils;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileUtils}. */
public class FileUtilsTest {
    @Test
    public void testParseCatalogSqlList(@TempDir Path tempFolder) throws Exception {
        File file = new File(tempFolder.toFile(), "catalog.sql");
        List<String> catalogSqlList =
                Arrays.asList(
                        "create catalog catalog1 with ('type'='generic_in_memory');",
                        "",
                        "\n",
                        "create catalog catalog2 with\n ('type'='generic_in_memory');",
                        "",
                        "create catalog catalog3 with ('type'='generic_in_memory');");
        IOUtils.writeLines(catalogSqlList, "\n", Files.newOutputStream(file.toPath()), "utf-8");
        String content = FileUtils.readFromURL(FileUtils.localFileToURL(file.getPath()));
        assertThat(FileUtils.parseCatalogSqls(content))
                .containsExactlyInAnyOrder(
                        "create catalog catalog1 with ('type'='generic_in_memory');",
                        "create catalog catalog2 with\n ('type'='generic_in_memory');",
                        "create catalog catalog3 with ('type'='generic_in_memory');");
    }
}
