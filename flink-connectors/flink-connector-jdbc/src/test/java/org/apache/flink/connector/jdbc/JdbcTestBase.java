/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc;

import org.junit.After;
import org.junit.Before;

/**
 * Base class for JDBC test using DDL from {@link JdbcTestFixture}. It uses create tables before
 * each test and drops afterwards.
 */
public abstract class JdbcTestBase {

    @Before
    public final void before() throws Exception {
        JdbcTestFixture.initSchema(getDbMetadata());
    }

    @After
    public final void after() throws Exception {
        JdbcTestFixture.cleanupData(getDbMetadata().getUrl());
        JdbcTestFixture.cleanUpDatabasesStatic(getDbMetadata());
    }

    protected abstract DbMetadata getDbMetadata();
}
