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

package org.apache.flink.table.api.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * This class holds configuration constants used by Flink's table module.
 *
 * <p>NOTE: All option keys in this class must start with "table.exec".
 */
@PublicEvolving
public class ExecutionConfigOptions {

    // ------------------------------------------------------------------------
    //  State Options
    // ------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> IDLE_STATE_RETENTION =
            key("table.exec.state.ttl")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "Specifies a minimum time interval for how long idle state "
                                    + "(i.e. state which was not updated), will be retained. State will never be "
                                    + "cleared until it was idle for less than the minimum time, and will be cleared "
                                    + "at some time after it was idle. Default is never clean-up the state. "
                                    + "NOTE: Cleaning up state requires additional overhead for bookkeeping. "
                                    + "Default value is 0, which means that it will never clean up state.");

    // ------------------------------------------------------------------------
    //  Source Options
    // ------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_SOURCE_IDLE_TIMEOUT =
            key("table.exec.source.idle-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "When a source do not receive any elements for the timeout time, "
                                    + "it will be marked as temporarily idle. This allows downstream "
                                    + "tasks to advance their watermarks without the need to wait for "
                                    + "watermarks from this source while it is idle. "
                                    + "Default value is 0, which means detecting source idleness is not enabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE =
            key("table.exec.source.cdc-events-duplicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Indicates whether the CDC (Change Data Capture) sources "
                                                    + "in the job will produce duplicate change events that requires the "
                                                    + "framework to deduplicate and get consistent result. CDC source refers to the "
                                                    + "source that produces full change events, including INSERT/UPDATE_BEFORE/"
                                                    + "UPDATE_AFTER/DELETE, for example Kafka source with Debezium format. "
                                                    + "The value of this configuration is false by default.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "However, it's a common case that there are duplicate change events. "
                                                    + "Because usually the CDC tools (e.g. Debezium) work in at-least-once delivery "
                                                    + "when failover happens. Thus, in the abnormal situations Debezium may deliver "
                                                    + "duplicate change events to Kafka and Flink will get the duplicate events. "
                                                    + "This may cause Flink query to get wrong results or unexpected exceptions.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Therefore, it is recommended to turn on this configuration if your CDC tool "
                                                    + "is at-least-once delivery. Enabling this configuration requires to define "
                                                    + "PRIMARY KEY on the CDC sources. The primary key will be used to deduplicate "
                                                    + "change events and generate normalized changelog stream at the cost of "
                                                    + "an additional stateful operator.")
                                    .build());

    // ------------------------------------------------------------------------
    //  Sink Options
    // ------------------------------------------------------------------------

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<NotNullEnforcer> TABLE_EXEC_SINK_NOT_NULL_ENFORCER =
            key("table.exec.sink.not-null-enforcer")
                    .enumType(NotNullEnforcer.class)
                    .defaultValue(NotNullEnforcer.ERROR)
                    .withDescription(
                            "Determines how Flink enforces NOT NULL column constraints when inserting null values.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<TypeLengthEnforcer> TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER =
            key("table.exec.sink.type-length-enforcer")
                    .enumType(TypeLengthEnforcer.class)
                    .defaultValue(TypeLengthEnforcer.IGNORE)
                    .withDescription(
                            "Determines whether values for columns with CHAR(<length>)/VARCHAR(<length>)"
                                    + "/BINARY(<length>)/VARBINARY(<length>) types will be trimmed or padded "
                                    + "(only for CHAR(<length>)/BINARY(<length>)), so that their length "
                                    + "will match the one defined by the length of their respective "
                                    + "CHAR/VARCHAR/BINARY/VARBINARY column type.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<UpsertMaterialize> TABLE_EXEC_SINK_UPSERT_MATERIALIZE =
            key("table.exec.sink.upsert-materialize")
                    .enumType(UpsertMaterialize.class)
                    .defaultValue(UpsertMaterialize.AUTO)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Because of the disorder of ChangeLog data caused by Shuffle in distributed system, "
                                                    + "the data received by Sink may not be the order of global upsert. "
                                                    + "So add upsert materialize operator before upsert sink. It receives the "
                                                    + "upstream changelog records and generate an upsert view for the downstream.")
                                    .linebreak()
                                    .text(
                                            "By default, the materialize operator will be added when a distributed disorder "
                                                    + "occurs on unique keys. You can also choose no materialization(NONE) "
                                                    + "or force materialization(FORCE).")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<SinkKeyedShuffle> TABLE_EXEC_SINK_KEYED_SHUFFLE =
            key("table.exec.sink.keyed-shuffle")
                    .enumType(SinkKeyedShuffle.class)
                    .defaultValue(SinkKeyedShuffle.AUTO)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "In order to minimize the distributed disorder problem when writing data into table with primary keys that many users suffers. "
                                                    + "FLINK will auto add a keyed shuffle by default when the sink parallelism differs from upstream operator and sink parallelism is not 1. "
                                                    + "This works only when the upstream ensures the multi-records' order on the primary key, if not, the added shuffle can not solve "
                                                    + "the problem (In this situation, a more proper way is to consider the deduplicate operation for the source firstly or use an "
                                                    + "upsert source with primary key definition which truly reflect the records evolution).")
                                    .linebreak()
                                    .text(
                                            "By default, the keyed shuffle will be added when the sink's parallelism differs from upstream operator. "
                                                    + "You can set to no shuffle(NONE) or force shuffle(FORCE).")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<RowtimeInserter> TABLE_EXEC_SINK_ROWTIME_INSERTER =
            key("table.exec.sink.rowtime-inserter")
                    .enumType(RowtimeInserter.class)
                    .defaultValue(RowtimeInserter.ENABLED)
                    .withDescription(
                            "Some sink implementations require a single rowtime attribute in the input "
                                    + "that can be inserted into the underlying stream record. This option "
                                    + "allows disabling the timestamp insertion and avoids errors around "
                                    + "multiple time attributes being present in the query schema.");

    // ------------------------------------------------------------------------
    //  Sort Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Integer> TABLE_EXEC_SORT_DEFAULT_LIMIT =
            key("table.exec.sort.default-limit")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Default limit when user don't set a limit after order by. -1 indicates that this configuration is ignored.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Integer> TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES =
            key("table.exec.sort.max-num-file-handles")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "The maximal fan-in for external merge sort. It limits the number of file handles per operator. "
                                    + "If it is too small, may cause intermediate merging. But if it is too large, "
                                    + "it will cause too many files opened at the same time, consume memory and lead to random reading.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED =
            key("table.exec.sort.async-merge-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to asynchronously merge sorted spill files.");

    // ------------------------------------------------------------------------
    //  Spill Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Boolean> TABLE_EXEC_SPILL_COMPRESSION_ENABLED =
            key("table.exec.spill-compression.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to compress spilled data. "
                                    + "Currently we only support compress spilled data for sort and hash-agg and hash-join operators.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<MemorySize> TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE =
            key("table.exec.spill-compression.block-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription(
                            "The memory size used to do compress when spilling data. "
                                    + "The larger the memory, the higher the compression ratio, "
                                    + "but more memory resource will be consumed by the job.");

    // ------------------------------------------------------------------------
    //  Resource Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer> TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM =
            key("table.exec.resource.default-parallelism")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Sets default parallelism for all operators "
                                    + "(such as aggregate, join, filter) to run with parallel instances. "
                                    + "This config has a higher priority than parallelism of "
                                    + "StreamExecutionEnvironment (actually, this config overrides the parallelism "
                                    + "of StreamExecutionEnvironment). A value of -1 indicates that no "
                                    + "default parallelism is set, then it will fallback to use the parallelism "
                                    + "of StreamExecutionEnvironment.");

    @Documentation.ExcludeFromDocumentation(
            "Beginning from Flink 1.10, this is interpreted as a weight hint "
                    + "instead of an absolute memory requirement. Users should not need to change these carefully tuned weight hints.")
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY =
            key("table.exec.resource.external-buffer-memory")
                    .memoryType()
                    .defaultValue(MemorySize.parse("10 mb"))
                    .withDescription(
                            "Sets the external buffer memory size that is used in sort merge join"
                                    + " and nested join and over window. Note: memory size is only a weight hint,"
                                    + " it will affect the weight of memory that can be applied by a single operator"
                                    + " in the task, the actual memory used depends on the running environment.");

    @Documentation.ExcludeFromDocumentation(
            "Beginning from Flink 1.10, this is interpreted as a weight hint "
                    + "instead of an absolute memory requirement. Users should not need to change these carefully tuned weight hints.")
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_HASH_AGG_MEMORY =
            key("table.exec.resource.hash-agg.memory")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128 mb"))
                    .withDescription(
                            "Sets the managed memory size of hash aggregate operator."
                                    + " Note: memory size is only a weight hint, it will affect the weight of memory"
                                    + " that can be applied by a single operator in the task, the actual memory used"
                                    + " depends on the running environment.");

    @Documentation.ExcludeFromDocumentation(
            "Beginning from Flink 1.10, this is interpreted as a weight hint "
                    + "instead of an absolute memory requirement. Users should not need to change these carefully tuned weight hints.")
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY =
            key("table.exec.resource.hash-join.memory")
                    .memoryType()
                    // in sync with other weights from Table API and DataStream API
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "Sets the managed memory for hash join operator. It defines the lower"
                                    + " limit. Note: memory size is only a weight hint, it will affect the weight of"
                                    + " memory that can be applied by a single operator in the task, the actual"
                                    + " memory used depends on the running environment.");

    @Documentation.ExcludeFromDocumentation(
            "Beginning from Flink 1.10, this is interpreted as a weight hint "
                    + "instead of an absolute memory requirement. Users should not need to change these carefully tuned weight hints.")
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_SORT_MEMORY =
            key("table.exec.resource.sort.memory")
                    .memoryType()
                    // in sync with other weights from Table API and DataStream API
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "Sets the managed buffer memory size for sort operator. Note: memory"
                                    + " size is only a weight hint, it will affect the weight of memory that can be"
                                    + " applied by a single operator in the task, the actual memory used depends on"
                                    + " the running environment.");

    // ------------------------------------------------------------------------
    //  Agg Options
    // ------------------------------------------------------------------------

    /** See {@code org.apache.flink.table.runtime.operators.window.grouping.HeapWindowsGrouping}. */
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<Integer> TABLE_EXEC_WINDOW_AGG_BUFFER_SIZE_LIMIT =
            key("table.exec.window-agg.buffer-size-limit")
                    .intType()
                    .defaultValue(100 * 1000)
                    .withDescription(
                            "Sets the window elements buffer size limit used in group window agg operator.");

    // ------------------------------------------------------------------------
    //  Async Lookup Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Integer> TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY =
            key("table.exec.async-lookup.buffer-capacity")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The max number of async i/o operation that the async lookup join can trigger.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT =
            key("table.exec.async-lookup.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3))
                    .withDescription(
                            "The async timeout for the asynchronous operation to complete.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<AsyncOutputMode> TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE =
            key("table.exec.async-lookup.output-mode")
                    .enumType(AsyncOutputMode.class)
                    .defaultValue(AsyncOutputMode.ORDERED)
                    .withDescription(
                            "Output mode for asynchronous operations which will convert to {@see AsyncDataStream.OutputMode}, ORDERED by default. "
                                    + "If set to ALLOW_UNORDERED, will attempt to use {@see AsyncDataStream.OutputMode.UNORDERED} when it does not "
                                    + "affect the correctness of the result, otherwise ORDERED will be still used.");

    // ------------------------------------------------------------------------
    //  MiniBatch Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_MINIBATCH_ENABLED =
            key("table.exec.mini-batch.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Specifies whether to enable MiniBatch optimization. "
                                    + "MiniBatch is an optimization to buffer input records to reduce state access. "
                                    + "This is disabled by default. To enable this, users should set this config to true. "
                                    + "NOTE: If mini-batch is enabled, 'table.exec.mini-batch.allow-latency' and "
                                    + "'table.exec.mini-batch.size' must be set.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_MINIBATCH_ALLOW_LATENCY =
            key("table.exec.mini-batch.allow-latency")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "The maximum latency can be used for MiniBatch to buffer input records. "
                                    + "MiniBatch is an optimization to buffer input records to reduce state access. "
                                    + "MiniBatch is triggered with the allowed latency interval and when the maximum number of buffered records reached. "
                                    + "NOTE: If "
                                    + TABLE_EXEC_MINIBATCH_ENABLED.key()
                                    + " is set true, its value must be greater than zero.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Long> TABLE_EXEC_MINIBATCH_SIZE =
            key("table.exec.mini-batch.size")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The maximum number of input records can be buffered for MiniBatch. "
                                    + "MiniBatch is an optimization to buffer input records to reduce state access. "
                                    + "MiniBatch is triggered with the allowed latency interval and when the maximum number of buffered records reached. "
                                    + "NOTE: MiniBatch only works for non-windowed aggregations currently. If "
                                    + TABLE_EXEC_MINIBATCH_ENABLED.key()
                                    + " is set true, its value must be positive.");

    // ------------------------------------------------------------------------
    //  Other Exec Options
    // ------------------------------------------------------------------------
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_EXEC_DISABLED_OPERATORS =
            key("table.exec.disabled-operators")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Mainly for testing. A comma-separated list of operator names, each name "
                                    + "represents a kind of disabled operator.\n"
                                    + "Operators that can be disabled include \"NestedLoopJoin\", \"ShuffleHashJoin\", \"BroadcastHashJoin\", "
                                    + "\"SortMergeJoin\", \"HashAgg\", \"SortAgg\".\n"
                                    + "By default no operator is disabled.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED =
            key("table.exec.operator-fusion-codegen.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, multiple physical operators will be compiled into a single operator by planner which can improve the performance.");

    /** @deprecated Use {@link ExecutionOptions#BATCH_SHUFFLE_MODE} instead. */
    @Deprecated
    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH)
    public static final ConfigOption<String> TABLE_EXEC_SHUFFLE_MODE =
            key("table.exec.shuffle-mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Sets exec shuffle mode.")
                                    .linebreak()
                                    .text("Accepted values are:")
                                    .list(
                                            text(
                                                    "%s: All edges will use blocking shuffle.",
                                                    code("ALL_EDGES_BLOCKING")),
                                            text(
                                                    "%s: Forward edges will use pipelined shuffle, others blocking.",
                                                    code("FORWARD_EDGES_PIPELINED")),
                                            text(
                                                    "%s: Pointwise edges will use pipelined shuffle, others blocking. "
                                                            + "Pointwise edges include forward and rescale edges.",
                                                    code("POINTWISE_EDGES_PIPELINED")),
                                            text(
                                                    "%s: All edges will use pipelined shuffle.",
                                                    code("ALL_EDGES_PIPELINED")),
                                            text(
                                                    "%s: the same as %s. Deprecated.",
                                                    code("batch"), code("ALL_EDGES_BLOCKING")),
                                            text(
                                                    "%s: the same as %s. Deprecated.",
                                                    code("pipelined"), code("ALL_EDGES_PIPELINED")))
                                    .text(
                                            "Note: Blocking shuffle means data will be fully produced before sent to consumer tasks. "
                                                    + "Pipelined shuffle means data will be sent to consumer tasks once produced.")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<LegacyCastBehaviour> TABLE_EXEC_LEGACY_CAST_BEHAVIOUR =
            key("table.exec.legacy-cast-behaviour")
                    .enumType(LegacyCastBehaviour.class)
                    .defaultValue(LegacyCastBehaviour.DISABLED)
                    .withDescription(
                            "Determines whether CAST will operate following the legacy behaviour "
                                    + "or the new one that introduces various fixes and improvements.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Long> TABLE_EXEC_RANK_TOPN_CACHE_SIZE =
            ConfigOptions.key("table.exec.rank.topn-cache-size")
                    .longType()
                    .defaultValue(10000L)
                    .withDeprecatedKeys("table.exec.topn-cache-size")
                    .withDescription(
                            "Rank operators have a cache which caches partial state contents "
                                    + "to reduce state access. Cache size is the number of records "
                                    + "in each ranking task.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.BATCH_STREAMING)
    public static final ConfigOption<Boolean> TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED =
            key("table.exec.simplify-operator-name-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "When it is true, the optimizer will simplify the operator name with id and type of ExecNode and keep detail in description. Default value is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean>
            TABLE_EXEC_DEDUPLICATE_INSERT_UPDATE_AFTER_SENSITIVE_ENABLED =
                    key("table.exec.deduplicate.insert-update-after-sensitive-enabled")
                            .booleanType()
                            .defaultValue(true)
                            .withDeprecatedKeys(
                                    "table.exec.deduplicate.insert-and-updateafter-sensitive.enabled")
                            .withDescription(
                                    "Set whether the job (especially the sinks) is sensitive to "
                                            + "INSERT messages and UPDATE_AFTER messages. "
                                            + "If false, Flink may, sometimes (e.g. deduplication "
                                            + "for last row), send UPDATE_AFTER instead of INSERT "
                                            + "for the first row. If true, Flink will guarantee to "
                                            + "send INSERT for the first row, in that case there "
                                            + "will be additional overhead. Default is true.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Boolean>
            TABLE_EXEC_DEDUPLICATE_MINIBATCH_COMPACT_CHANGES_ENABLED =
                    ConfigOptions.key("table.exec.deduplicate.mini-batch.compact-changes-enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDeprecatedKeys(
                                    "table.exec.deduplicate.mini-batch.compact-changes.enabled")
                            .withDescription(
                                    "Set whether to compact the changes sent downstream in row-time "
                                            + "mini-batch. If true, Flink will compact changes and send "
                                            + "only the latest change downstream. Note that if the "
                                            + "downstream needs the details of versioned data, this "
                                            + "optimization cannot be applied. If false, Flink will send "
                                            + "all changes to downstream just like when the mini-batch is "
                                            + "not enabled.");

    /** @deprecated Use {@link #TABLE_EXEC_UID_GENERATION} instead. */
    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    @Deprecated
    public static final ConfigOption<Boolean> TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS =
            key("table.exec.legacy-transformation-uids")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "This flag has been replaced by table.exec.uid.generation. Use the enum "
                                    + "value DISABLED to restore legacy behavior. However, the new "
                                    + "default value should be sufficient for most use cases as "
                                    + "only pipelines from compiled plans get UIDs assigned.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<UidGeneration> TABLE_EXEC_UID_GENERATION =
            key("table.exec.uid.generation")
                    .enumType(UidGeneration.class)
                    .defaultValue(UidGeneration.PLAN_ONLY)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "In order to remap state to operators during a restore, "
                                                    + "it is required that the pipeline's streaming "
                                                    + "transformations get a UID assigned.")
                                    .linebreak()
                                    .text(
                                            "The planner can generate and assign explicit UIDs. If no "
                                                    + "UIDs have been set by the planner, the UIDs will "
                                                    + "be auto-generated by lower layers that can take "
                                                    + "the complete topology into account for uniqueness "
                                                    + "of the IDs. See the DataStream API for more information.")
                                    .linebreak()
                                    .text(
                                            "This configuration option is for experts only and the default "
                                                    + "should be sufficient for most use cases. By default, "
                                                    + "only pipelines created from a persisted compiled plan will "
                                                    + "get UIDs assigned explicitly. Thus, these pipelines can "
                                                    + "be arbitrarily moved around within the same topology without "
                                                    + "affecting the stable UIDs.")
                                    .build());

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<String> TABLE_EXEC_UID_FORMAT =
            key("table.exec.uid.format")
                    .stringType()
                    .defaultValue("<id>_<transformation>")
                    .withDescription(
                            "Defines the format pattern for generating the UID of an ExecNode streaming transformation. "
                                    + "The pattern can be defined globally or per-ExecNode in the compiled plan. "
                                    + "Supported arguments are: <id> (from static counter), <type> (e.g. 'stream-exec-sink'), "
                                    + "<version>, and <transformation> (e.g. 'constraint-validator' for a sink). "
                                    + "In Flink 1.15.x the pattern was wrongly defined as '<id>_<type>_<version>_<transformation>' "
                                    + "which would prevent migrations in the future.");

    @Documentation.TableOption(execMode = Documentation.ExecMode.STREAMING)
    public static final ConfigOption<Duration> TABLE_EXEC_INTERVAL_JOIN_MIN_CLEAN_UP_INTERVAL =
            key("table.exec.interval-join.min-cleanup-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "Specifies a minimum time interval for how long cleanup unmatched records in the interval join operator. "
                                    + "Before Flink 1.18, the default value of this param was the half of interval duration. "
                                    + "Note: Set this option greater than 0 will cause unmatched records in outer joins to be output later than watermark, "
                                    + "leading to possible discarding of these records by downstream watermark-dependent operators, such as window operators. "
                                    + "The default value is 0, which means it will clean up unmatched records immediately.");

    // ------------------------------------------------------------------------------------------
    // Enum option types
    // ------------------------------------------------------------------------------------------

    /** The enforcer to guarantee NOT NULL column constraint when writing data into sink. */
    @PublicEvolving
    public enum NotNullEnforcer implements DescribedEnum {
        ERROR(text("Throw a runtime exception when writing null values into NOT NULL column.")),
        DROP(
                text(
                        "Drop records silently if a null value would have to be inserted "
                                + "into a NOT NULL column."));

        private final InlineElement description;

        NotNullEnforcer(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /**
     * The enforcer to guarantee that length of CHAR/VARCHAR/BINARY/VARBINARY columns is respected
     * when writing data into a sink.
     */
    @PublicEvolving
    public enum TypeLengthEnforcer implements DescribedEnum {
        IGNORE(
                text(
                        "Don't apply any trimming and padding, and instead "
                                + "ignore the CHAR/VARCHAR/BINARY/VARBINARY length directive.")),
        TRIM_PAD(
                text(
                        "Trim and pad string and binary values to match the length "
                                + "defined by the CHAR/VARCHAR/BINARY/VARBINARY length."));

        private final InlineElement description;

        TypeLengthEnforcer(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Upsert materialize strategy before sink. */
    @PublicEvolving
    public enum UpsertMaterialize {

        /** In no case will materialize operator be added. */
        NONE,

        /** Add materialize operator when a distributed disorder occurs on unique keys. */
        AUTO,

        /** Add materialize operator in any case. */
        FORCE
    }

    /** Shuffle by primary key before sink. */
    @PublicEvolving
    public enum SinkKeyedShuffle {

        /** No keyed shuffle will be added for sink. */
        NONE,

        /** Auto add keyed shuffle when the sink's parallelism differs from upstream operator. */
        AUTO,

        /** Add keyed shuffle in any case except single parallelism. */
        FORCE
    }

    /** Rowtime attribute insertion strategy for the sink. */
    @PublicEvolving
    public enum RowtimeInserter implements DescribedEnum {
        ENABLED(
                text(
                        "Insert a rowtime attribute (if available) into the underlying stream record. "
                                + "This requires at most one time attribute in the input for the sink.")),
        DISABLED(text("Do not insert the rowtime attribute into the underlying stream record."));

        private final InlineElement description;

        RowtimeInserter(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }

    /** Output mode for asynchronous operations, equivalent to {@see AsyncDataStream.OutputMode}. */
    @PublicEvolving
    public enum AsyncOutputMode {

        /** Ordered output mode, equivalent to {@see AsyncDataStream.OutputMode.ORDERED}. */
        ORDERED,

        /**
         * Allow unordered output mode, will attempt to use {@see
         * AsyncDataStream.OutputMode.UNORDERED} when it does not affect the correctness of the
         * result, otherwise ORDERED will be still used.
         */
        ALLOW_UNORDERED
    }

    /** Determine if CAST operates using the legacy behaviour or the new one. */
    @Deprecated
    public enum LegacyCastBehaviour implements DescribedEnum {
        ENABLED(true, text("CAST will operate following the legacy behaviour.")),
        DISABLED(false, text("CAST will operate following the new correct behaviour."));

        private final boolean enabled;
        private final InlineElement description;

        LegacyCastBehaviour(boolean enabled, InlineElement description) {
            this.enabled = enabled;
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }

        public boolean isEnabled() {
            return enabled;
        }
    }

    /**
     * Strategy for generating transformation UIDs for remapping state to operators during restore.
     */
    @PublicEvolving
    public enum UidGeneration implements DescribedEnum {
        PLAN_ONLY(
                text(
                        "Sets UIDs on streaming transformations if and only if the pipeline definition "
                                + "comes from a compiled plan. Pipelines that have been constructed in "
                                + "the API without a compilation step will not set an explicit UID as "
                                + "it might not be stable across multiple translations.")),
        ALWAYS(
                text(
                        "Always sets UIDs on streaming transformations. This strategy is for experts only! "
                                + "Pipelines that have been constructed in the API without a compilation "
                                + "step might not be able to be restored properly. The UID generation "
                                + "depends on previously declared pipelines (potentially across jobs "
                                + "if the same JVM is used). Thus, a stable environment must be ensured. "
                                + "Pipeline definitions that come from a compiled plan are safe to use.")),

        DISABLED(text("No explicit UIDs will be set."));

        private final InlineElement description;

        UidGeneration(InlineElement description) {
            this.description = description;
        }

        @Internal
        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
