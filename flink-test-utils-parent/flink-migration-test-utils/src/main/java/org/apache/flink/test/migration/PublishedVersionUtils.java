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

package org.apache.flink.test.migration;

import org.apache.flink.FlinkVersion;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

/** Utilities for update and read the most-recently published flink version. */
public class PublishedVersionUtils {

    private static final String MOST_RECENTLY_PUBLISHED_VERSION_FILE =
            "most_recently_published_version";

    public static FlinkVersion getMostRecentlyPublishedVersion() {
        try (InputStream input =
                        Objects.requireNonNull(
                                PublishedVersionUtils.class.getResourceAsStream(
                                        "/" + MOST_RECENTLY_PUBLISHED_VERSION_FILE),
                                "The most-recently published version file does not exist");
                BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
            String versionName = reader.readLine();
            return FlinkVersion.valueOf(versionName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse the latest published version", e);
        }
    }
}
