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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;

/**
 * This class holds configuration constants used by Flink's table planner module.
 *
 * <p>NOTE: All option keys in this class must start with "table.optimizer".
 */
@PublicEvolving
public class OptimizerConfigOptions {

    // ------------------------------------------------------------------------
    //  Optimizer Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<String> TABLE_OPTIMIZER_AGG_PHASE_STRATEGY =
            key("table.optimizer.agg-phase-strategy")
                    .stringType()
                    .defaultValue("AUTO")
                    .withDescription(
                            "Strategy for aggregate phase. Only AUTO, TWO_PHASE or ONE_PHASE can be set.\n"
                                    + "AUTO: No special enforcer for aggregate stage. Whether to choose two stage aggregate or one"
                                    + " stage aggregate depends on cost. \n"
                                    + "TWO_PHASE: Enforce to use two stage aggregate which has localAggregate and globalAggregate. "
                                    + "Note that if aggregate call does not support optimize into two phase, we will still use one stage aggregate.\n"
                                    + "ONE_PHASE: Enforce to use one stage aggregate which only has CompleteGlobalAggregate.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Long> TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD =
            key("table.optimizer.join.broadcast-threshold")
                    .longType()
                    .defaultValue(1024 * 1024L)
                    .withDescription(
                            "Configures the maximum size in bytes for a table that will be broadcast to all worker "
                                    + "nodes when performing a join. By setting this value to -1 to disable broadcasting.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED =
            key("table.optimizer.distinct-agg.split.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Tells the optimizer whether to split distinct aggregation "
                                    + "(e.g. COUNT(DISTINCT col), SUM(DISTINCT col)) into two level. "
                                    + "The first aggregation is shuffled by an additional key which is calculated using "
                                    + "the hashcode of distinct_key and number of buckets. This optimization is very useful "
                                    + "when there is data skew in distinct aggregation and gives the ability to scale-up the job. "
                                    + "Default is false.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Integer> TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_BUCKET_NUM =
            key("table.optimizer.distinct-agg.split.bucket-num")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Configure the number of buckets when splitting distinct aggregation. "
                                    + "The number is used in the first level aggregation to calculate a bucket key "
                                    + "'hash_code(distinct_key) % BUCKET_NUM' which is used as an additional group key after splitting.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED =
            key("table.optimizer.reuse-sub-plan-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will try to find out duplicated sub-plans and reuse them.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED =
            key("table.optimizer.reuse-source-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will try to find out duplicated table sources and "
                                    + "reuse them. This works only when "
                                    + TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED.key()
                                    + " is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED =
            key("table.optimizer.source.aggregate-pushdown-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will push down the local aggregates into "
                                    + "the TableSource which implements SupportsAggregatePushDown.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED =
            key("table.optimizer.source.predicate-pushdown-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will push down predicates into the FilterableTableSource. "
                                    + "Default value is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_SOURCE_REPORT_STATISTICS_ENABLED =
            key("table.optimizer.source.report-statistics-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will collect and use the statistics from source connectors"
                                    + " if the source extends from SupportsStatisticReport and the statistics from catalog is UNKNOWN."
                                    + "Default value is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_JOIN_REORDER_ENABLED =
            key("table.optimizer.join-reorder-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enables join reorder in optimizer. Default is disabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer> TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD =
            key("table.optimizer.bushy-join-reorder-threshold")
                    .intType()
                    .defaultValue(12)
                    .withDescription(
                            "The maximum number of joined nodes allowed in the bushy join reorder algorithm, "
                                    + "otherwise the left-deep join reorder algorithm will be used. The search "
                                    + "space of bushy join reorder algorithm will increase with the increase of "
                                    + "this threshold value, so this threshold is not recommended to be set too "
                                    + "large. The default value is 12.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED =
            key("table.optimizer.multiple-input-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will merge the operators with pipelined shuffling "
                                    + "into a multiple input operator to reduce shuffling and improve performance. Default value is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED =
            key("table.optimizer.dynamic-filtering.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will try to push dynamic filtering into scan table source,"
                                    + " the irrelevant partitions or input data will be filtered to reduce scan I/O in runtime.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_RUNTIME_FILTER_ENABLED =
            key("table.optimizer.runtime-filter.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "A flag to enable or disable the runtime filter. "
                                    + "When it is true, the optimizer will try to inject a runtime filter for eligible join.");

    /**
     * The data volume of build side needs to be under this value. If the data volume of build side
     * is too large, the building overhead will be too large, which may lead to a negative impact on
     * job performance.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<MemorySize>
            TABLE_OPTIMIZER_RUNTIME_FILTER_MAX_BUILD_DATA_SIZE =
                    key("table.optimizer.runtime-filter.max-build-data-size")
                            .memoryType()
                            .defaultValue(MemorySize.parse("150m"))
                            .withDescription(
                                    "Max data volume threshold of the runtime filter build side. "
                                            + "Estimated data volume needs to be under this value to try to inject runtime filter.");

    /**
     * The data volume of probe side needs to be over this value. If the data volume on the probe
     * side is too small, the overhead of building runtime filter is not worth it.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<MemorySize>
            TABLE_OPTIMIZER_RUNTIME_FILTER_MIN_PROBE_DATA_SIZE =
                    key("table.optimizer.runtime-filter.min-probe-data-size")
                            .memoryType()
                            .defaultValue(MemorySize.parse("10g"))
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "Min data volume threshold of the runtime filter probe side. "
                                                            + "Estimated data volume needs to be over this value to try to inject runtime filter."
                                                            + "This value should be larger than %s.",
                                                    code(
                                                            TABLE_OPTIMIZER_RUNTIME_FILTER_MAX_BUILD_DATA_SIZE
                                                                    .key()))
                                            .build());

    /**
     * The filtering ratio of runtime filter needs to be over this value. A low filter rate is not
     * worth it to build runtime filter.
     */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Double> TABLE_OPTIMIZER_RUNTIME_FILTER_MIN_FILTER_RATIO =
            key("table.optimizer.runtime-filter.min-filter-ratio")
                    .doubleType()
                    .defaultValue(0.5)
                    .withDescription(
                            "Min filter ratio threshold of the runtime filter. "
                                    + "Estimated filter ratio needs to be over this value to try to inject runtime filter.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<NonDeterministicUpdateStrategy>
            TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY =
                    key("table.optimizer.non-deterministic-update.strategy")
                            .enumType(NonDeterministicUpdateStrategy.class)
                            .defaultValue(NonDeterministicUpdateStrategy.IGNORE)
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "When it is `TRY_RESOLVE`, the optimizer tries to resolve the correctness issue caused by "
                                                            + "'Non-Deterministic Updates' (NDU) in a changelog pipeline. Changelog may contain kinds"
                                                            + " of message types: Insert (I), Delete (D), Update_Before (UB), Update_After (UA)."
                                                            + " There's no NDU problem in an insert only changelog pipeline. For updates, there are"
                                                            + "  three main NDU problems:")
                                            .linebreak()
                                            .text(
                                                    "1. Non-deterministic functions, include scalar, table, aggregate functions, both builtin and custom ones.")
                                            .linebreak()
                                            .text("2. LookupJoin on an evolving source")
                                            .linebreak()
                                            .text(
                                                    "3. Cdc-source carries metadata fields which are system columns, not belongs to the entity data itself.")
                                            .linebreak()
                                            .linebreak()
                                            .text(
                                                    "For the first step, the optimizer automatically enables the materialization for No.2(LookupJoin) if needed,"
                                                            + " and gives the detailed error message for No.1(Non-deterministic functions) and"
                                                            + " No.3(Cdc-source with metadata) which is relatively easier to solve by changing the SQL.")
                                            .linebreak()
                                            .text(
                                                    "Default value is `IGNORE`, the optimizer does no changes.")
                                            .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_SQL2REL_PROJECT_MERGE_ENABLED =
            key("table.optimizer.sql2rel.project-merge.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("table.optimizer.sql-to-rel.project.merge.enabled")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If set to true, it will merge projects when converting SqlNode to RelNode.")
                                    .linebreak()
                                    .text(
                                            "Note: it is not recommended to turn on unless you are aware of possible side effects, "
                                                    + "such as causing the output of certain non-deterministic expressions to not meet expectations(see FLINK-20887).")
                                    .build());

    /** Strategy for handling non-deterministic updates. */
    @PublicEvolving
    public enum NonDeterministicUpdateStrategy {

        /**
         * Try to resolve by planner automatically if exists non-deterministic updates, will raise
         * an error when cannot resolve.
         */
        TRY_RESOLVE,

        /**
         * Do nothing if exists non-deterministic updates, the risk of wrong result still exists.
         */
        IGNORE
    }
}
