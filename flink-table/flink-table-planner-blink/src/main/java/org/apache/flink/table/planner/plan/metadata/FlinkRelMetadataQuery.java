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
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * A RelMetadataQuery that defines extended metadata handler in Flink,
 * e.g ColumnInterval, ColumnNullCount.
 */
public class FlinkRelMetadataQuery extends RelMetadataQuery {
	// Serves as the handlers prototype of all the FlinkRelMetadataQuery instances.
	private static final Handlers HANDLERS = new Handlers();

	private FlinkMetadata.ColumnInterval.Handler columnIntervalHandler;
	private FlinkMetadata.FilteredColumnInterval.Handler filteredColumnInterval;
	private FlinkMetadata.ColumnNullCount.Handler columnNullCountHandler;
	private FlinkMetadata.ColumnOriginNullCount.Handler columnOriginNullCountHandler;
	private FlinkMetadata.UniqueGroups.Handler uniqueGroupsHandler;
	private FlinkMetadata.FlinkDistribution.Handler distributionHandler;
	private FlinkMetadata.ModifiedMonotonicity.Handler modifiedMonotonicityHandler;

	/**
	 * Returns an instance of FlinkRelMetadataQuery. It ensures that cycles do not
	 * occur while computing metadata.
	 */
	public static FlinkRelMetadataQuery instance() {
		return new FlinkRelMetadataQuery();
	}

	/**
	 * Reuse input metadataQuery instance if it could cast to FlinkRelMetadataQuery class,
	 * or create one if not.
	 *
	 * @param mq metadataQuery which try to reuse
	 * @return a FlinkRelMetadataQuery instance
	 */
	public static FlinkRelMetadataQuery reuseOrCreate(RelMetadataQuery mq) {
		if (mq instanceof FlinkRelMetadataQuery) {
			return (FlinkRelMetadataQuery) mq;
		} else {
			return instance();
		}
	}

	/**
	 * Creates a FlinkRelMetadataQuery instance.
	 */
	private FlinkRelMetadataQuery() {
		this.columnIntervalHandler = HANDLERS.columnIntervalHandler;
		this.filteredColumnInterval = HANDLERS.filteredColumnInterval;
		this.columnNullCountHandler = HANDLERS.columnNullCountHandler;
		this.columnOriginNullCountHandler = HANDLERS.columnOriginNullCountHandler;
		this.uniqueGroupsHandler = HANDLERS.uniqueGroupsHandler;
		this.distributionHandler = HANDLERS.distributionHandler;
		this.modifiedMonotonicityHandler = HANDLERS.modifiedMonotonicityHandler;
	}

	/** Extended handlers. */
	private static class Handlers {
		private FlinkMetadata.ColumnInterval.Handler columnIntervalHandler =
				initialHandler(FlinkMetadata.ColumnInterval.Handler.class);
		private FlinkMetadata.FilteredColumnInterval.Handler filteredColumnInterval =
				initialHandler(FlinkMetadata.FilteredColumnInterval.Handler.class);
		private FlinkMetadata.ColumnNullCount.Handler columnNullCountHandler =
				initialHandler(FlinkMetadata.ColumnNullCount.Handler.class);
		private FlinkMetadata.ColumnOriginNullCount.Handler columnOriginNullCountHandler =
				initialHandler(FlinkMetadata.ColumnOriginNullCount.Handler.class);
		private FlinkMetadata.UniqueGroups.Handler uniqueGroupsHandler =
				initialHandler(FlinkMetadata.UniqueGroups.Handler.class);
		private FlinkMetadata.FlinkDistribution.Handler distributionHandler =
				initialHandler(FlinkMetadata.FlinkDistribution.Handler.class);
		private FlinkMetadata.ModifiedMonotonicity.Handler modifiedMonotonicityHandler =
				initialHandler(FlinkMetadata.ModifiedMonotonicity.Handler.class);
	}

	/**
	 * Returns the {@link FlinkMetadata.ColumnInterval} statistic.
	 *
	 * @param rel the relational expression
	 * @param index the index of the given column
	 * @return the interval of the given column of a specified relational expression.
	 *         Returns null if interval cannot be estimated,
	 *         Returns {@link org.apache.flink.table.planner.plan.stats.EmptyValueInterval}
	 *         if column values does not contains any value except for null.
	 */
	public ValueInterval getColumnInterval(RelNode rel, int index) {
		for (; ; ) {
			try {
				return columnIntervalHandler.getColumnInterval(rel, this, index);
			} catch (JaninoRelMetadataProvider.NoHandler e) {
				columnIntervalHandler = revise(e.relClass, FlinkMetadata.ColumnInterval.DEF);
			}
		}
	}

	/**
	 * Returns the {@link FlinkMetadata.ColumnInterval} of the given column
	 * under the given filter argument.
	 *
	 * @param rel the relational expression
	 * @param columnIndex the index of the given column
	 * @param filterArg the index of the filter argument
	 * @return the interval of the given column of a specified relational expression.
	 *         Returns null if interval cannot be estimated,
	 *         Returns {@link org.apache.flink.table.planner.plan.stats.EmptyValueInterval}
	 *         if column values does not contains any value except for null.
	 */
	public ValueInterval getFilteredColumnInterval(RelNode rel, int columnIndex, int filterArg) {
		for (; ; ) {
			try {
				return filteredColumnInterval.getFilteredColumnInterval(
						rel, this, columnIndex, filterArg);
			} catch (JaninoRelMetadataProvider.NoHandler e) {
				filteredColumnInterval =
						revise(e.relClass, FlinkMetadata.FilteredColumnInterval.DEF);
			}
		}
	}

	/**
	 * Returns the null count of the given column.
	 *
	 * @param rel the relational expression
	 * @param index the index of the given column
	 * @return the null count of the given column if can be estimated, else return null.
	 */
	public Double getColumnNullCount(RelNode rel, int index) {
		for (; ; ) {
			try {
				return columnNullCountHandler.getColumnNullCount(rel, this, index);
			} catch (JaninoRelMetadataProvider.NoHandler e) {
				columnNullCountHandler = revise(e.relClass, FlinkMetadata.ColumnNullCount.DEF);
			}
		}
	}

	/**
	 * Returns origin null count of the given column.
	 *
	 * @param rel the relational expression
	 * @param index the index of the given column
	 * @return the null count of the given column if can be estimated, else return null.
	 */
	public Double getColumnOriginNullCount(RelNode rel, int index) {
		for (; ; ) {
			try {
				return columnOriginNullCountHandler.getColumnOriginNullCount(rel, this, index);
			} catch (JaninoRelMetadataProvider.NoHandler e) {
				columnOriginNullCountHandler =
						revise(e.relClass, FlinkMetadata.ColumnOriginNullCount.DEF);
			}
		}
	}

	/**
	 * Returns the (minimum) unique groups of the given columns.
	 *
	 * @param rel the relational expression
	 * @param columns the given columns in a specified relational expression.
	 *        The given columns should not be null.
	 * @return the (minimum) unique columns which should be a sub-collection of the given columns,
	 *         and should not be null or empty. If none unique columns can be found, return the
	 *         given columns.
	 */
	public ImmutableBitSet getUniqueGroups(RelNode rel, ImmutableBitSet columns) {
		for (; ; ) {
			try {
				Preconditions.checkArgument(columns != null);
				if (columns.isEmpty()) {
					return columns;
				}
				ImmutableBitSet uniqueGroups =
						uniqueGroupsHandler.getUniqueGroups(rel, this, columns);
				Preconditions.checkArgument(uniqueGroups != null && !uniqueGroups.isEmpty());
				Preconditions.checkArgument(columns.contains(uniqueGroups));
				return uniqueGroups;
			} catch (JaninoRelMetadataProvider.NoHandler e) {
				uniqueGroupsHandler = revise(e.relClass, FlinkMetadata.UniqueGroups.DEF);
			}
		}
	}

	/**
	 * Returns the {@link FlinkRelDistribution} statistic.
	 *
	 * @param rel the relational expression
	 * @return description of how the rows in the relational expression are
	 *         physically distributed
	 */
	public FlinkRelDistribution flinkDistribution(RelNode rel) {
		for (; ; ) {
			try {
				return distributionHandler.flinkDistribution(rel, this);
			} catch (JaninoRelMetadataProvider.NoHandler e) {
				distributionHandler = revise(e.relClass, FlinkMetadata.FlinkDistribution.DEF);
			}
		}
	}

	/**
	 * Returns the {@link RelModifiedMonotonicity} statistic.
	 *
	 * @param rel the relational expression
	 * @return the monotonicity for the corresponding RelNode
	 */
	public RelModifiedMonotonicity getRelModifiedMonotonicity(RelNode rel) {
		for (; ; ) {
			try {
				return modifiedMonotonicityHandler.getRelModifiedMonotonicity(rel, this);
			} catch (JaninoRelMetadataProvider.NoHandler e) {
				modifiedMonotonicityHandler =
						revise(e.relClass, FlinkMetadata.ModifiedMonotonicity.DEF);
			}
		}
	}

}
