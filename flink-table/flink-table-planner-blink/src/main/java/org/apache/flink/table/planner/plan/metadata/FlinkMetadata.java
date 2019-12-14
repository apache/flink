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

package org.apache.flink.table.planner.plan.metadata;

import org.apache.flink.table.planner.plan.stats.ValueInterval;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.RelModifiedMonotonicity;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Method;

/**
 * Contains the interfaces for several specified metadata of flink.
 */
public abstract class FlinkMetadata {

	/**
	 * Metadata about the interval of given column from a specified relational expression.
	 */
	public interface ColumnInterval extends Metadata {

		Method METHOD = Types.lookupMethod(ColumnInterval.class, "getColumnInterval", int.class);

		MetadataDef<ColumnInterval> DEF = MetadataDef.of(
				ColumnInterval.class,
				ColumnInterval.Handler.class,
				METHOD);

		/**
		 * Returns the interval value of the given column from a specified relational expression.
		 *
		 * @param index the index of the given column in a specified relational expression
		 * @return the interval of the given column.
		 *         Returns null if interval cannot be estimated,
		 *         Returns [[NullValueInterval]] if column values does not contains any value
		 *         except for null.
		 */
		ValueInterval getColumnInterval(int index);

		/**
		 * Handler API.
		 */
		interface Handler extends MetadataHandler<ColumnInterval> {
			ValueInterval getColumnInterval(RelNode r, RelMetadataQuery mq, int index);
		}
	}

	/**
	 * Metadata about the interval of given column under the given filter argument
	 * from a specified relational expression.
	 */
	public interface FilteredColumnInterval extends Metadata {
		Method METHOD = Types.lookupMethod(FilteredColumnInterval.class, "getFilteredColumnInterval", int.class, int.class);

		MetadataDef<FilteredColumnInterval> DEF = MetadataDef.of(
				FilteredColumnInterval.class,
				FilteredColumnInterval.Handler.class,
				METHOD);

		/**
		 * Returns the interval value of the given column under the given filter argument
		 * from a specified relational expression.
		 *
		 * @param columnIndex the index of the given column in a specified relational expression
		 * @param filterArg the index of the filter argument, -1 when no filter argument existed
		 * @return the interval of the given column.
		 * Returns null if interval cannot be estimated,
		 * Returns [[NullValueInterval]] if column values does not contains any value
		 * except for null.
		 */
		ValueInterval getFilteredColumnInterval(int columnIndex, int filterArg);

		/**
		 * Handler API.
		 */
		interface Handler extends MetadataHandler<FilteredColumnInterval> {
			ValueInterval getFilteredColumnInterval(RelNode r, RelMetadataQuery mq, int columnIndex, int filterArg);
		}

	}

	/**
	 * Metadata about the null count of given column from a specified relational expression.
	 */
	public interface ColumnNullCount extends Metadata {

		Method METHOD = Types.lookupMethod(ColumnNullCount.class, "getColumnNullCount", int.class);

		MetadataDef<ColumnNullCount> DEF = MetadataDef.of(
				ColumnNullCount.class,
				ColumnNullCount.Handler.class,
				METHOD);

		/**
		 * Returns the null count of the given column from a specified relational expression.
		 *
		 * @param index the index of the given column in a specified relational expression
		 * @return the null count of the given column if can be estimated, else return null.
		 */
		Double getColumnNullCount(int index);

		/**
		 * Handler API.
		 */
		interface Handler extends MetadataHandler<ColumnNullCount> {
			Double getColumnNullCount(RelNode r, RelMetadataQuery mq, int index);
		}

	}

	/**
	 * Origin null count, looking until source.
	 */
	public interface ColumnOriginNullCount extends Metadata {

		Method METHOD = Types.lookupMethod(ColumnOriginNullCount.class, "getColumnOriginNullCount", int.class);

		MetadataDef<ColumnOriginNullCount> DEF = MetadataDef.of(
				ColumnOriginNullCount.class,
				ColumnOriginNullCount.Handler.class,
				METHOD);

		/**
		 * Returns origin null count of the given column from a specified relational expression.
		 *
		 * @param index the index of the given column in a specified relational expression
		 * @return origin null count of the given column if can be estimated, else return null.
		 */
		Double getColumnOriginNullCount(int index);

		/**
		 * Handler API.
		 */
		interface Handler extends MetadataHandler<ColumnOriginNullCount> {
			Double getColumnOriginNullCount(RelNode r, RelMetadataQuery mq, int index);
		}

	}

	/**
	 * Metadata about the (minimum) unique groups of the given columns from a specified relational expression.
	 */
	public interface UniqueGroups extends Metadata {

		Method METHOD = Types.lookupMethod(UniqueGroups.class, "getUniqueGroups", ImmutableBitSet.class);

		MetadataDef<UniqueGroups> DEF = MetadataDef.of(
				UniqueGroups.class,
				UniqueGroups.Handler.class,
				METHOD);

		/**
		 * Returns the (minimum) unique groups of the given columns from a specified relational expression.
		 *
		 * @param columns the given columns in a specified relational expression.
		 *                The given columns should not be null.
		 * @return the (minimum) unique columns which should be a sub-collection of the given columns,
		 *         and should not be null or empty. If none unique columns can be found, return the given columns.
		 */
		ImmutableBitSet getUniqueGroups(ImmutableBitSet columns);

		/**
		 * Handler API.
		 */
		interface Handler extends MetadataHandler<UniqueGroups> {
			ImmutableBitSet getUniqueGroups(RelNode r, RelMetadataQuery mq, ImmutableBitSet columns);
		}
	}

	/**
	 * Metadata about how a relational expression is distributed.
	 *
	 * <p>If you are an operator consuming a relational expression, which subset
	 * of the rows are you seeing? You might be seeing all of them (BROADCAST
	 * or SINGLETON), only those whose key column values have a particular hash
	 * code (HASH) or only those whose column values have particular values or
	 * ranges of values (RANGE).
	 *
	 * <p>When a relational expression is partitioned, it is often partitioned
	 * among nodes, but it may be partitioned among threads running on the same
	 * node.
	 */
	public interface FlinkDistribution extends Metadata {
		Method METHOD = Types.lookupMethod(FlinkDistribution.class, "flinkDistribution");

		MetadataDef<FlinkDistribution> DEF = MetadataDef.of(
				FlinkDistribution.class,
				FlinkDistribution.Handler.class,
				METHOD);

		/** Determines how the rows are distributed. */
		FlinkRelDistribution flinkDistribution();

		/** Handler API. */
		interface Handler extends MetadataHandler<FlinkDistribution> {
			FlinkRelDistribution flinkDistribution(RelNode r, RelMetadataQuery mq);
		}
	}

	/**
	 * Metadata about the modified property of a RelNode. For example, an aggregate RelNode
	 * contains a max aggregate function whose result value maybe modified increasing.
	 */
	public interface ModifiedMonotonicity extends Metadata {
		Method METHOD = Types.lookupMethod(ModifiedMonotonicity.class, "getRelModifiedMonotonicity");

		MetadataDef<ModifiedMonotonicity> DEF = MetadataDef.of(
				ModifiedMonotonicity.class,
				ModifiedMonotonicity.Handler.class,
				METHOD);

		RelModifiedMonotonicity getRelModifiedMonotonicity();

		/** Handler API. */
		interface Handler extends MetadataHandler<ModifiedMonotonicity> {
			RelModifiedMonotonicity getRelModifiedMonotonicity(RelNode r, RelMetadataQuery mq);
		}
	}

}
