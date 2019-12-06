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

package org.apache.flink.table.client.gateway.local;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link org.apache.flink.client.deployment.executors.LocalExecutor}.
 */
public class LocalExecutorTest {

	@Test
	public void testInsertIntoSqlPattern() {
		assertTrue(LocalExecutor.INSERT_INTO_SQL_PATTERN.matcher("insert into t1 select * from t2").matches());
		assertTrue(LocalExecutor.INSERT_INTO_SQL_PATTERN.matcher("INSERT INTO t1 select * from t2").matches());
		assertTrue(LocalExecutor.INSERT_INTO_SQL_PATTERN.matcher("INsert Into t1 select * from t2").matches());
		assertFalse(LocalExecutor.INSERT_INTO_SQL_PATTERN.matcher("alter table t1 rename to t2").matches());
		assertFalse(LocalExecutor.INSERT_INTO_SQL_PATTERN.matcher("alter database d1 set ('k1'='v2')").matches());
	}
}
