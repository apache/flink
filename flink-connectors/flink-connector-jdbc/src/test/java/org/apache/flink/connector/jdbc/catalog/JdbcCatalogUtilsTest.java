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

package org.apache.flink.connector.jdbc.catalog;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test for {@link JdbcCatalogUtils}. */
public class JdbcCatalogUtilsTest {
    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void testJdbcUrl() {
        JdbcCatalogUtils.validateJdbcUrl("jdbc:postgresql://localhost:5432/");

        JdbcCatalogUtils.validateJdbcUrl("jdbc:postgresql://localhost:5432");
    }

    @Test
    public void testInvalidJdbcUrl() {
        exception.expect(IllegalArgumentException.class);
        JdbcCatalogUtils.validateJdbcUrl("jdbc:postgresql://localhost:5432/db");
    }
}
