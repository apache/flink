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

package org.apache.flink.table.gateway.service.utils;

import org.apache.flink.core.fs.Path;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** File utils for gateway and client. */
public class FileUtils {
    private static final String CATALOG_SPLIT_DELIMITER = ";\n";
    private static final String CATALOG_END_DELIMITER = ";";

    /**
     * Convert local file path to url.
     *
     * @param filePath the given local file path
     * @return the file path url
     * @throws Exception the thrown exception
     */
    public static URL localFileToURL(String filePath) throws Exception {
        Path path = new Path(filePath);
        String scheme = path.toUri().getScheme();
        if (scheme != null && !scheme.equals("file")) {
            throw new Exception("Only supports to load files in local.");
        }
        return Path.fromLocalFile(new File(filePath).getAbsoluteFile()).toUri().toURL();
    }

    /**
     * Read content from local file url.
     *
     * @param file the given file url
     * @return the content
     */
    public static String readFromURL(URL file) {
        try {
            return IOUtils.toString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SqlExecutionException(
                    String.format("Fail to read content from the %s.", file.getPath()), e);
        }
    }

    /**
     * Parse catalog sql list from content.
     *
     * @param content the given sql content
     * @return the catalog sql list
     */
    public static List<String> parseCatalogSqls(String content) {
        return Arrays.stream(StringUtils.splitByWholeSeparator(content, CATALOG_SPLIT_DELIMITER))
                .map(String::trim)
                .filter(v -> v.length() > 0)
                .map(
                        v -> {
                            if (v.endsWith(CATALOG_END_DELIMITER)) {
                                return v;
                            } else {
                                return v + CATALOG_END_DELIMITER;
                            }
                        })
                .collect(Collectors.toList());
    }
}
