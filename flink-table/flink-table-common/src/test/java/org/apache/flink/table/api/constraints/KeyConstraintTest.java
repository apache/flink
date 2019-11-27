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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.FieldReferenceExpression;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link KeyConstraint}.
 */
public class KeyConstraintTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testCreatingPrimaryKey() {
		String keyName = "pk";
		List<FieldReferenceExpression> columns = Collections.singletonList(
			new FieldReferenceExpression("f0", DataTypes.BIGINT().notNull(), 0, 1));
		KeyConstraint primaryKey = KeyConstraint.primaryKey(keyName, columns);

		assertThat(primaryKey.getType(), is(Constraint.ConstraintType.PRIMARY_KEY));
		assertThat(primaryKey.getName(), is(keyName));
		assertThat(primaryKey.getColumns(), equalTo(columns));
	}

	@Test
	public void testCreatingPrimaryKeyForNullableColumns() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Cannot define PRIMARY KEY constraint on nullable column.");

		KeyConstraint.primaryKey(
			"pk",
			Collections.singletonList(new FieldReferenceExpression("f0", DataTypes.BIGINT(), 0, 1)));
	}
}
