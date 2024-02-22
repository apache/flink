/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.TableSource;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Flink {@link org.apache.calcite.prepare.Prepare.AbstractPreparingTable} implementation for the
 * purposes of the sql-to-rel conversion and planner.
 */
@Internal
public abstract class FlinkPreparingTableBase extends Prepare.AbstractPreparingTable {
    // ~ Static fields/initializers ---------------------------------------------

    // Default value of rowCount if there is no available stats.
    // Sets a bigger default value to avoid broadcast join.
    private static final double DEFAULT_ROWCOUNT = 1E8;

    // ~ Instance fields --------------------------------------------------------

    @Nullable protected final RelOptSchema relOptSchema;
    protected final RelDataType rowType;
    protected final List<String> names;

    protected FlinkStatistic statistic;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a {@link org.apache.calcite.prepare.Prepare.AbstractPreparingTable} instance.
     *
     * @param relOptSchema The RelOptSchema that this table comes from
     * @param rowType The table row type
     * @param names The table qualified name
     * @param statistic The table statistics
     */
    public FlinkPreparingTableBase(
            @Nullable RelOptSchema relOptSchema,
            RelDataType rowType,
            Iterable<String> names,
            FlinkStatistic statistic) {
        this.relOptSchema = relOptSchema;
        this.rowType = Objects.requireNonNull(rowType);
        this.names = Objects.requireNonNull(ImmutableList.copyOf(names));
        this.statistic = Objects.requireNonNull(statistic);
    }

    // ~ Methods ----------------------------------------------------------------

    /** Returns the statistic of this table. */
    public FlinkStatistic getStatistic() {
        return this.statistic;
    }

    /**
     * Returns the table path in the {@link RelOptSchema}. Different with {@link
     * #getQualifiedName()}, the latter is mainly used for table digest.
     */
    public List<String> getNames() {
        return names;
    }

    @Override
    public List<String> getQualifiedName() {
        return names;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context) {
        return LogicalTableScan.create(context.getCluster(), this, context.getTableHints());
    }

    /**
     * Obtains whether a given column is monotonic.
     *
     * @param columnName Column name
     * @return True if the given column is monotonic
     */
    public SqlMonotonicity getMonotonicity(String columnName) {
        return SqlMonotonicity.NOT_MONOTONIC;
    }

    /**
     * Obtains the access type of the table.
     *
     * @return all access types including SELECT/UPDATE/INSERT/DELETE
     */
    public SqlAccessType getAllowedAccess() {
        return SqlAccessType.ALL;
    }

    @Override
    public boolean supportsModality(SqlModality modality) {
        return false;
    }

    /** We recognize all tables in FLink are temporal as they are changeable. */
    public boolean isTemporal() {
        return true;
    }

    /** Returns an estimate of the number of rows in the table. */
    public double getRowCount() {
        Double rowCnt = getStatistic().getRowCount();
        return rowCnt == null ? DEFAULT_ROWCOUNT : rowCnt;
    }

    /** Returns the type of rows returned by this table. */
    public RelDataType getRowType() {
        return rowType;
    }

    @Override
    public RelOptSchema getRelOptSchema() {
        return relOptSchema;
    }

    /**
     * Returns a description of the physical ordering (or orderings) of the rows returned from this
     * table.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery#collations(RelNode)
     */
    public List<RelCollation> getCollationList() {
        return ImmutableList.of();
    }

    /**
     * Returns a description of the physical distribution of the rows in this table.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery#distribution
     */
    public RelDistribution getDistribution() {
        return null;
    }

    /**
     * Returns whether the given columns are a key or a superset of a unique key of this table.
     *
     * <p>Note: Return true means TRUE. However return false means FALSE or NOT KNOWN. It's better
     * to use {@link org.apache.calcite.rel.metadata.RelMetadataQuery#areRowsUnique} to distinguish
     * FALSE with NOT KNOWN.
     *
     * @param columns Ordinals of key columns
     * @return If the input columns bits represents a unique column set; false if not (or if no
     *     metadata is available)
     */
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

    /**
     * Returns the referential constraints existing for this table. These constraints are
     * represented over other tables using {@link RelReferentialConstraint} nodes.
     */
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return ImmutableList.of();
    }

    @Override
    public <C> C unwrap(Class<C> clazz) {
        if (clazz.isInstance(this)) {
            return clazz.cast(this);
        } else {
            return null;
        }
    }

    /**
     * Obtains whether the ordinal column has a default value, which is not supported now.
     *
     * @param rowType Row type of field
     * @param ordinal Index of the given column
     * @param initializerContext Context for {@link
     *     org.apache.calcite.sql2rel.InitializerExpressionFactory}
     * @return true if the column has a default value
     */
    public boolean columnHasDefaultValue(
            RelDataType rowType, int ordinal, InitializerContext initializerContext) {
        return false;
    }

    /**
     * Generates code for this table, which is not supported now.
     *
     * @param clazz The desired collection class, for example {@link
     *     org.apache.calcite.linq4j.Queryable}
     */
    public Expression getExpression(Class clazz) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected RelOptTable extend(Table extendedTable) {
        throw new RuntimeException("Extending column not supported");
    }

    // ~ Tools ------------------------------------------------------------------

    /** Returns the digest of the {@link TableSource} instance. */
    protected List<String> explainSourceAsString(TableSource<?> ts) {
        String tsDigest = ts.explainSource();
        if (!Strings.isNullOrEmpty(tsDigest)) {
            return ImmutableList.<String>builder()
                    .addAll(Util.skipLast(names))
                    .add(String.format("%s, source: [%s]", Util.last(names), tsDigest))
                    .build();
        } else {
            return names;
        }
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        return statistic.getKeys();
    }

    /** Returns unique keySets of current table. */
    public Optional<Set<ImmutableBitSet>> uniqueKeysSet() {
        Set<? extends Set<String>> uniqueKeys = statistic.getUniqueKeys();
        if (uniqueKeys == null) {
            return Optional.empty();
        } else if (uniqueKeys.size() == 0) {
            return Optional.of(ImmutableSet.of());
        } else {
            ImmutableSet.Builder<ImmutableBitSet> uniqueKeysSetBuilder = ImmutableSet.builder();
            for (Set<String> keys : uniqueKeys) {
                // some columns in original uniqueKeys may not exist in RowType after project push
                // down.
                boolean allUniqueKeysExists =
                        keys.stream().allMatch(f -> rowType.getField(f, false, false) != null);
                // if not all columns in original uniqueKey, skip this uniqueKey
                if (allUniqueKeysExists) {
                    Set<Integer> keysPosition =
                            keys.stream()
                                    .map(f -> rowType.getField(f, false, false).getIndex())
                                    .collect(Collectors.toSet());
                    uniqueKeysSetBuilder.add(ImmutableBitSet.of(keysPosition));
                }
            }
            return Optional.of(uniqueKeysSetBuilder.build());
        }
    }
}
