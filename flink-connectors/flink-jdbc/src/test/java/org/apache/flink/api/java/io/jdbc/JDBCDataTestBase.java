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

package org.apache.flink.api.java.io.jdbc;

import org.junit.Before;

import java.sql.SQLException;

/**
 * Base class for JDBC test using data from {@link JdbcTestFixture}. It uses {@link DerbyDbMetadata} and inserts data before each test.
 */
abstract class JDBCDataTestBase extends JDBCTestBase {
	@Before
	public void initData() throws SQLException {
		JdbcTestFixture.initData(getDbMetadata());
	}

	@Override
	protected DbMetadata getDbMetadata() {
		return JdbcTestFixture.DERBY_EBOOKSHOP_DB;
	}
}
