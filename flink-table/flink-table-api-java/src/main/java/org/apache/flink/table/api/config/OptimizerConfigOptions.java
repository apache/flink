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
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

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
    public static final ConfigOption<AggregatePhaseStrategy> TABLE_OPTIMIZER_AGG_PHASE_STRATEGY =
            key("table.optimizer.agg-phase-strategy")
                    .enumType(AggregatePhaseStrategy.class)
                    .defaultValue(AggregatePhaseStrategy.AUTO)
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

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_REUSE_SINK_ENABLED =
            key("table.optimizer.reuse-sink-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will try to find out duplicated table sinks and "
                                    + "reuse them. This works only when "
                                    + TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED.key()
                                    + " is true.");

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

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<AdaptiveBroadcastJoinStrategy>
            TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY =
                    key("table.optimizer.adaptive-broadcast-join.strategy")
                            .enumType(AdaptiveBroadcastJoinStrategy.class)
                            .defaultValue(AdaptiveBroadcastJoinStrategy.AUTO)
                            .withDescription(
                                    "Flink will perform broadcast hash join optimization when the runtime "
                                            + "statistics on one side of a join operator is less than the "
                                            + "threshold `table.optimizer.join.broadcast-threshold`. The "
                                            + "value of this configuration option decides when Flink should "
                                            + "perform this optimization. AUTO means Flink will automatically "
                                            + "choose the timing for optimization, RUNTIME_ONLY means broadcast "
                                            + "hash join optimization is only performed at runtime, and NONE "
                                            + "means the optimization is only carried out at compile time.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<AdaptiveSkewedJoinOptimizationStrategy>
            TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY =
                    key("table.optimizer.skewed-join-optimization.strategy")
                            .enumType(AdaptiveSkewedJoinOptimizationStrategy.class)
                            .defaultValue(AdaptiveSkewedJoinOptimizationStrategy.AUTO)
                            .withDescription(
                                    "Flink will handle skew in shuffled joins (sort-merge and hash) "
                                            + "at runtime by splitting data according to the skewed join "
                                            + "key. The value of this configuration determines how Flink performs "
                                            + "this optimization. AUTO means Flink will automatically apply this "
                                            + "optimization, FORCED means Flink will enforce this optimization even "
                                            + "if it introduces extra hash shuffle, and NONE means this optimization "
                                            + "will not be executed.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Double>
            TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_SKEWED_FACTOR =
                    key("table.optimizer.skewed-join-optimization.skewed-factor")
                            .doubleType()
                            .defaultValue(4.0)
                            .withDescription(
                                    "When a join operator instance encounters input data that exceeds N times the median "
                                            + "size of other concurrent join operator instances, it is considered skewed "
                                            + "(where N represents this skewed-factor). In such cases, Flink may automatically "
                                            + "split the skewed data into multiple parts to ensure a more balanced data "
                                            + "distribution, unless the data volume is below the skewed threshold(defined "
                                            + "using table.optimizer.skewed-join-optimization.skewed-threshold).");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<MemorySize>
            TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_SKEWED_THRESHOLD =
                    key("table.optimizer.skewed-join-optimization.skewed-threshold")
                            .memoryType()
                            .defaultValue(MemorySize.ofMebiBytes(256))
                            .withDescription(
                                    "When a join operator instance encounters input data that exceeds N times the median "
                                            + "size of other concurrent join operator instances, it is considered skewed "
                                            + "(where N represents the table.optimizer.skewed-join-optimization.skewed-factor). "
                                            + "In such cases, Flink may automatically split the skewed data into multiple "
                                            + "parts to ensure a more balanced data distribution, unless the data volume "
                                            + "is below this skewed threshold.");

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
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If set to true, it will merge projects when converting SqlNode to RelNode.")
                                    .linebreak()
                                    .text(
                                            "Note: it is not recommended to turn on unless you are aware of possible side effects, "
                                                    + "such as causing the output of certain non-deterministic expressions to not meet expectations(see FLINK-20887).")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED =
            key("table.optimizer.union-all-as-breakpoint-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When true, the optimizer will breakup the graph at union-all node "
                                    + "when it's a breakpoint. When false, the optimizer will skip the union-all node "
                                    + "even it's a breakpoint, and will try find the breakpoint in its inputs.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean>
            TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED =
                    key("table.optimizer.reuse-optimize-block-with-digest-enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "When true, the optimizer will try to find out duplicated sub-plans by "
                                            + "digest to build optimize blocks (a.k.a. common sub-graphs). "
                                            + "Each optimize block will be optimized independently.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED =
            key("table.optimizer.incremental-agg-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When both local aggregation and distinct aggregation splitting "
                                    + "are enabled, a distinct aggregation will be optimized into four aggregations, "
                                    + "i.e., local-agg1, global-agg1, local-agg2, and global-agg2. We can combine global-agg1 "
                                    + "and local-agg2 into a single operator (we call it incremental agg because "
                                    + "it receives incremental accumulators and outputs incremental results). "
                                    + "In this way, we can reduce some state overhead and resources. Default is enabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Integer> TABLE_OPTIMIZER_PTF_MAX_TABLES =
            key("table.optimizer.ptf.max-tables")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "The maximum number of table arguments for a Process Table Function (PTF). In theory, a PTF "
                                    + "can accept an arbitrary number of input tables. In practice, however, each input "
                                    + "requires reserving network buffers, which impacts memory usage. For this reason, "
                                    + "the number of input tables is limited to 20.");

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

    /** Strategies used for {@link #TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY}. */
    @PublicEvolving
    public enum AdaptiveBroadcastJoinStrategy implements DescribedEnum {
        AUTO("auto", text("Flink will automatically choose the timing for optimization")),
        RUNTIME_ONLY(
                "runtime_only",
                text("Broadcast hash join optimization is only performed at runtime.")),
        NONE("none", text("Broadcast hash join optimization is only carried out at compile time."));

        private final String value;

        private final InlineElement description;

        AdaptiveBroadcastJoinStrategy(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Strategies used for {@link #TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY}. */
    @PublicEvolving
    public enum AdaptiveSkewedJoinOptimizationStrategy implements DescribedEnum {
        AUTO("auto", text(" Flink will automatically perform this optimization.")),
        FORCED(
                "forced",
                text(
                        "Flink will perform this optimization even if it introduces extra hash shuffling.")),
        NONE("none", text("Skewed join optimization will not be performed."));

        private final String value;

        private final InlineElement description;

        AdaptiveSkewedJoinOptimizationStrategy(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
