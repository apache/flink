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

package org.apache.flink.table.api.constraints;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link UniqueConstraint}.
 */
public class UniqueConstraintTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testCreatingPrimaryKey() {
		String keyName = "pk";
		List<String> columns = Collections.singletonList("f0");
		UniqueConstraint primaryKey = UniqueConstraint.primaryKey(keyName, columns);

		assertThat(primaryKey.getType(), is(Constraint.ConstraintType.PRIMARY_KEY));
		assertThat(primaryKey.getName(), is(keyName));
		assertThat(primaryKey.getColumns(), equalTo(columns));
	}
}
