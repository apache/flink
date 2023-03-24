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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * A {@code LIKE} clause in a {@code CREATE TABLE} statement.
 *
 * <p>It enables to use an existing table descriptor to define a new, adjusted/extended table. Users
 * can control the way particular features of both declarations are merged using {@link
 * MergingStrategy} and {@link FeatureOption}.
 *
 * <p>Example: A DDL like the one below for creating a `derived_table`
 *
 * <pre>{@code
 * CREATE TABLE base_table_1 (
 *     id BIGINT,
 *     name STRING,
 *     tstmp TIMESTAMP,
 *     PRIMARY KEY(id)
 * ) WITH (
 *     ‘connector’: ‘kafka’,
 *     ‘connector.starting-offset’: ‘12345’,
 *     ‘format’: ‘json’
 * )
 *
 * CREATE TEMPORARY TABLE derived_table (
 *     WATERMARK FOR tstmp AS tsmp - INTERVAL '5' SECOND
 * )
 * WITH (
 *     ‘connector.starting-offset’: ‘0’
 * )
 * LIKE base_table (
 *   OVERWRITING OPTIONS,
 *   EXCLUDING CONSTRAINTS
 * )
 * }</pre>
 *
 * <p>is equivalent to:
 *
 * <pre>{@code
 * CREATE TEMPORARY TABLE derived_table (
 *     id BIGINT,
 *     name STRING,
 *     tstmp TIMESTAMP,
 *     WATERMARK FOR tstmp AS tsmp - INTERVAL '5' SECOND
 * ) WITH (
 *     ‘connector’: ‘kafka’,
 *     ‘connector.starting-offset’: ‘0’,
 *     ‘format’: ‘json’
 * )
 * }</pre>
 */
public class SqlTableLike extends SqlCall implements ExtendedSqlNode {
    /**
     * A strategy that describes how the features of the parent source table should be merged with
     * the features of the newly created table.
     *
     * <ul>
     *   <li>EXCLUDING - does not include the given feature of the source table
     *   <li>INCLUDING - includes feature of the source table, fails on duplicate entries, e.g. if
     *       an option with the same key exists in both tables.
     *   <li>OVERWRITING - includes feature of the source table, overwrites duplicate entries of the
     *       source table with properties of the new table, e.g. if an option with the same key
     *       exists in both tables, the one from the current statement will be used.
     * </ul>
     */
    public enum MergingStrategy {
        INCLUDING,
        EXCLUDING,
        OVERWRITING
    }

    /**
     * A feature of a table descriptor that will be merged into the new table. The way how a certain
     * feature will be merged into the final descriptor is controlled with {@link MergingStrategy}.
     *
     * <ul>
     *   <li>ALL - a shortcut to change the default merging strategy if none provided
     *   <li>CONSTRAINTS - constraints such as primary and unique keys
     *   <li>GENERATED - computed columns
     *   <li>METADATA - metadata columns
     *   <li>WATERMARKS - watermark declarations
     *   <li>PARTITIONS - partition of the tables
     *   <li>OPTIONS - connector options that describe connector and format properties
     * </ul>
     *
     * <p>Example:
     *
     * <pre>{@code
     * LIKE `sourceTable` (
     *   INCLUDING ALL
     *   OVERWRITING OPTIONS
     *   EXCLUDING PARTITIONS
     * )
     * }</pre>
     *
     * <p>is equivalent to:
     *
     * <pre>{@code
     * LIKE `sourceTable` (
     *   INCLUDING GENERATED
     *   INCLUDING CONSTRAINTS
     *   OVERWRITING OPTIONS
     *   EXCLUDING PARTITIONS
     * )
     * }</pre>
     */
    public enum FeatureOption {
        ALL,
        CONSTRAINTS,
        GENERATED,
        METADATA,
        OPTIONS,
        PARTITIONS,
        WATERMARKS
    }

    private final SqlIdentifier sourceTable;
    private final List<SqlTableLikeOption> options;

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("LIKE TABLE", SqlKind.OTHER);

    public SqlTableLike(
            SqlParserPos pos, SqlIdentifier sourceTable, List<SqlTableLikeOption> options) {
        super(pos);
        this.sourceTable = sourceTable;
        this.options = options;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return singletonList(sourceTable);
    }

    public SqlIdentifier getSourceTable() {
        return sourceTable;
    }

    public List<SqlTableLikeOption> getOptions() {
        return options;
    }

    private static final Map<FeatureOption, List<MergingStrategy>> invalidCombinations =
            new HashMap<>();

    static {
        invalidCombinations.put(FeatureOption.ALL, singletonList(MergingStrategy.OVERWRITING));
        invalidCombinations.put(
                FeatureOption.PARTITIONS, singletonList(MergingStrategy.OVERWRITING));
        invalidCombinations.put(
                FeatureOption.CONSTRAINTS, singletonList(MergingStrategy.OVERWRITING));
    }

    @Override
    public void validate() throws SqlValidateException {
        long distinctFeatures =
                options.stream().map(SqlTableLikeOption::getFeatureOption).distinct().count();
        if (distinctFeatures != options.size()) {
            throw new SqlValidateException(
                    pos, "Each like option feature can be declared only once.");
        }

        for (SqlTableLikeOption option : options) {
            if (invalidCombinations
                    .getOrDefault(option.featureOption, Collections.emptyList())
                    .contains(option.mergingStrategy)) {
                throw new SqlValidateException(
                        pos,
                        String.format(
                                "Illegal merging strategy '%s' for '%s' option.",
                                option.getMergingStrategy(), option.getFeatureOption()));
            }
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("LIKE");
        sourceTable.unparse(writer, leftPrec, rightPrec);
        if (options == null || options.isEmpty()) {
            return;
        }
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (SqlTableLikeOption option : options) {
            writer.newlineAndIndent();
            writer.print("  ");
            writer.keyword(option.mergingStrategy.toString());
            writer.keyword(option.featureOption.toString());
        }
        writer.newlineAndIndent();
        writer.endList(frame);
    }

    /**
     * A pair of {@link MergingStrategy} and {@link FeatureOption}.
     *
     * @see MergingStrategy
     * @see FeatureOption
     */
    public static class SqlTableLikeOption {
        private final MergingStrategy mergingStrategy;
        private final FeatureOption featureOption;

        public SqlTableLikeOption(MergingStrategy mergingStrategy, FeatureOption featureOption) {
            this.mergingStrategy = mergingStrategy;
            this.featureOption = featureOption;
        }

        public MergingStrategy getMergingStrategy() {
            return mergingStrategy;
        }

        public FeatureOption getFeatureOption() {
            return featureOption;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlTableLikeOption that = (SqlTableLikeOption) o;
            return mergingStrategy == that.mergingStrategy && featureOption == that.featureOption;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mergingStrategy, featureOption);
        }

        @Override
        public String toString() {
            return String.format("%s %s", mergingStrategy, featureOption);
        }
    }
}
