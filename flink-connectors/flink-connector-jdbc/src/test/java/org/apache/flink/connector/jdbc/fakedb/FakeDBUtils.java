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

package org.apache.flink.connector.jdbc.fakedb;

/** Utilities and constants for FakeDB. */
public class FakeDBUtils {
    public static final String URL_PREFIX = "jdbc:fake:";

    public static final String TEST_DB_URL = composeDBUrl("test");
    public static final String TEST_DB_INVALID_URL = "jdbc:no-existing-driver:test";

    public static final String DRIVER1_CLASS_NAME =
            "org.apache.flink.connector.jdbc.fakedb.driver.FakeDriver1";
    public static final String DRIVER2_CLASS_NAME =
            "org.apache.flink.connector.jdbc.fakedb.driver.FakeDriver2";
    public static final String DRIVER3_CLASS_NAME =
            "org.apache.flink.connector.jdbc.fakedb.driver.FakeDriver3";

    public static String composeDBUrl(String db) {
        return URL_PREFIX + db;
    }

    public static boolean acceptsUrl(String url) {
        return url.startsWith(URL_PREFIX);
    }
}
