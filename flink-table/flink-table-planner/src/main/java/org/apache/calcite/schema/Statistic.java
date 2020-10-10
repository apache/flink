/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.schema;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.List;

/**
 * Statistics about a {@link Table}.
 *
 * <p>Each of the methods may return {@code null} meaning "not known".</p>
 *
 * <p>Changes:
 *
 * <ul>
 *     <li>Line 61: default collations change from null to empty list.</li>
 * </ul>
 *
 * @see Statistics
 */
public interface Statistic {
	/** Returns the approximate number of rows in the table. */
	default Double getRowCount() {
		return null;
	}

	/** Returns whether the given set of columns is a unique key, or a superset
	 * of a unique key, of the table.
	 */
	default boolean isKey(ImmutableBitSet columns) {
		return false;
	}

	/** Returns a list of unique keys, or null if no key exist. */
	default List<ImmutableBitSet> getKeys() {
		return null;
	}

	/** Returns the collection of referential constraints (foreign-keys)
	 * for this table. */
	default List<RelReferentialConstraint> getReferentialConstraints() {
		return null;
	}

	/** Returns the collections of columns on which this table is sorted. */
	default List<RelCollation> getCollations() {
		return Collections.emptyList();
	}

	/** Returns the distribution of the data in this table. */
	default RelDistribution getDistribution()  {
		return null;
	}
}
