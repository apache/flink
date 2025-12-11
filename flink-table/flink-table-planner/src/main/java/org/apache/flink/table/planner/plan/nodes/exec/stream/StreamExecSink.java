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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.RowtimeInserter;
import org.apache.flink.table.api.config.ExecutionConfigOptions.SinkUpsertMaterializeStrategy;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.HashCodeGenerator;
import org.apache.flink.table.planner.connectors.CollectDynamicSink;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerV2;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateConfig;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.typeutils.RowTypeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW;

/**
 * Stream {@link ExecNode} to write data into an external sink defined by a {@link
 * DynamicTableSink}.
 */
@ExecNodeMetadata(
        name = "stream-exec-sink",
        version = 1,
        consumedOptions = {
            "table.exec.sink.not-null-enforcer",
            "table.exec.sink.type-length-enforcer",
            "table.exec.sink.upsert-materialize",
            "table.exec.sink.keyed-shuffle",
            "table.exec.sink.rowtime-inserter"
        },
        producedTransformations = {
            CommonExecSink.CONSTRAINT_VALIDATOR_TRANSFORMATION,
            CommonExecSink.PARTITIONER_TRANSFORMATION,
            CommonExecSink.UPSERT_MATERIALIZE_TRANSFORMATION,
            CommonExecSink.TIMESTAMP_INSERTER_TRANSFORMATION,
            CommonExecSink.SINK_TRANSFORMATION
        },
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
// Version 2: Fixed the data type used for creating constraint enforcer and sink upsert
// materializer. Since this version the sink works correctly with persisted metadata columns.
// We introduced a new version, because statements that were never rolling back to a value from
// state could run succesfully. We allow those jobs to be upgraded. Without a new versions such jobs
// would fail on restore, because the state serializer would differ
@ExecNodeMetadata(
        name = "stream-exec-sink",
        version = 2,
        consumedOptions = {
            "table.exec.sink.not-null-enforcer",
            "table.exec.sink.type-length-enforcer",
            "table.exec.sink.upsert-materialize",
            "table.exec.sink.keyed-shuffle",
            "table.exec.sink.rowtime-inserter"
        },
        producedTransformations = {
            CommonExecSink.CONSTRAINT_VALIDATOR_TRANSFORMATION,
            CommonExecSink.PARTITIONER_TRANSFORMATION,
            CommonExecSink.UPSERT_MATERIALIZE_TRANSFORMATION,
            CommonExecSink.TIMESTAMP_INSERTER_TRANSFORMATION,
            CommonExecSink.SINK_TRANSFORMATION
        },
        minPlanVersion = FlinkVersion.v2_3,
        minStateVersion = FlinkVersion.v2_3)
public class StreamExecSink extends CommonExecSink implements StreamExecNode<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(StreamExecSink.class);

    public static final String FIELD_NAME_INPUT_CHANGELOG_MODE = "inputChangelogMode";
    public static final String FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE = "requireUpsertMaterialize";
    public static final String FIELD_NAME_UPSERT_MATERIALIZE_STRATEGY = "upsertMaterializeStrategy";
    public static final String FIELD_NAME_INPUT_UPSERT_KEY = "inputUpsertKey";

    /** New introduced state metadata to enable operator-level state TTL configuration. */
    public static final String STATE_NAME = "sinkMaterializeState";

    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE)
    private final ChangelogMode inputChangelogMode;

    @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean upsertMaterialize;

    @JsonProperty(FIELD_NAME_UPSERT_MATERIALIZE_STRATEGY)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final SinkUpsertMaterializeStrategy upsertMaterializeStrategy;

    @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final int[] inputUpsertKey;

    @Nullable
    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecSink(
            ReadableConfig tableConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            InputProperty inputProperty,
            LogicalType outputType,
            boolean upsertMaterialize,
            SinkUpsertMaterializeStrategy upsertMaterializeStrategy,
            int[] inputUpsertKey,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecSink.class),
                ExecNodeContext.newPersistedConfig(StreamExecSink.class, tableConfig),
                tableSinkSpec,
                inputChangelogMode,
                upsertMaterialize,
                upsertMaterializeStrategy,
                // do not serialize state metadata if upsertMaterialize is not required
                upsertMaterialize
                        ? StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME)
                        : null,
                inputUpsertKey,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecSink(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK) DynamicTableSinkSpec tableSinkSpec,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE) ChangelogMode inputChangelogMode,
            @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE) boolean upsertMaterialize,
            @Nullable @JsonProperty(FIELD_NAME_UPSERT_MATERIALIZE_STRATEGY)
                    SinkUpsertMaterializeStrategy sinkUpsertMaterializeStrategy,
            @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY) int[] inputUpsertKey,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) LogicalType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                tableSinkSpec,
                inputChangelogMode,
                false, // isBounded
                inputProperties,
                outputType,
                description);
        this.inputChangelogMode = inputChangelogMode;
        this.upsertMaterialize = upsertMaterialize;
        this.inputUpsertKey = inputUpsertKey;
        this.stateMetadataList = stateMetadataList;
        this.upsertMaterializeStrategy = sinkUpsertMaterializeStrategy;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<Object> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final DynamicTableSink tableSink = tableSinkSpec.getTableSink(planner.getFlinkContext());
        final boolean isCollectSink = tableSink instanceof CollectDynamicSink;
        final boolean isDisabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_ROWTIME_INSERTER)
                        == RowtimeInserter.DISABLED;

        final List<Integer> rowtimeFieldIndices = new ArrayList<>();
        for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
            if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                rowtimeFieldIndices.add(i);
            }
        }

        final int rowtimeFieldIndex;
        if (isCollectSink || isDisabled) {
            rowtimeFieldIndex = -1;
        } else if (rowtimeFieldIndices.size() > 1) {
            throw new TableException(
                    String.format(
                            "The query contains more than one rowtime attribute column [%s] for writing into table '%s'.\n"
                                    + "Please select the column that should be used as the event-time timestamp "
                                    + "for the table sink by casting all other columns to regular TIMESTAMP or TIMESTAMP_LTZ.",
                            rowtimeFieldIndices.stream()
                                    .map(i -> inputRowType.getFieldNames().get(i))
                                    .collect(Collectors.joining(", ")),
                            tableSinkSpec
                                    .getContextResolvedTable()
                                    .getIdentifier()
                                    .asSummaryString()));
        } else if (rowtimeFieldIndices.size() == 1) {
            rowtimeFieldIndex = rowtimeFieldIndices.get(0);
        } else {
            rowtimeFieldIndex = -1;
        }

        return createSinkTransformation(
                planner.getExecEnv(),
                config,
                planner.getFlinkContext().getClassLoader(),
                inputTransform,
                tableSink,
                rowtimeFieldIndex,
                upsertMaterialize,
                inputUpsertKey);
    }

    @Override
    protected Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType physicalRowType,
            int[] inputUpsertKey) {

        final GeneratedRecordEqualiser rowEqualiser =
                new EqualiserCodeGenerator(physicalRowType, classLoader)
                        .generateRecordEqualiser("SinkMaterializeEqualiser");

        final GeneratedRecordEqualiser upsertKeyEqualiser =
                inputUpsertKey == null
                        ? null
                        : new EqualiserCodeGenerator(
                                        RowTypeUtils.projectRowType(
                                                physicalRowType, inputUpsertKey),
                                        classLoader)
                                .generateRecordEqualiser("SinkMaterializeUpsertKeyEqualiser");

        GeneratedHashFunction rowHashFunction =
                HashCodeGenerator.generateRowHash(
                        new CodeGeneratorContext(config, classLoader),
                        physicalRowType,
                        "hashCode",
                        IntStream.range(0, physicalRowType.getFieldCount()).toArray());

        final GeneratedHashFunction upsertKeyHashFunction =
                inputUpsertKey == null
                        ? null
                        : HashCodeGenerator.generateRowHash(
                                new CodeGeneratorContext(config, classLoader),
                                RowTypeUtils.projectRowType(physicalRowType, inputUpsertKey),
                                "generated_hashcode_for_" + inputUpsertKey.length + "_keys",
                                IntStream.range(0, inputUpsertKey.length).toArray());

        StateTtlConfig ttlConfig =
                StateConfigUtil.createTtlConfig(
                        StateMetadata.getStateTtlForOneInputOperator(config, stateMetadataList));

        final OneInputStreamOperator<RowData, RowData> operator =
                createSumOperator(
                        config,
                        physicalRowType,
                        inputUpsertKey,
                        upsertKeyEqualiser,
                        upsertKeyHashFunction,
                        ttlConfig,
                        rowEqualiser,
                        rowHashFunction);

        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);
        final List<String> pkFieldNames =
                Arrays.stream(primaryKeys)
                        .mapToObj(idx -> fieldNames[idx])
                        .collect(Collectors.toList());

        OneInputTransformation<RowData, RowData> materializeTransform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(
                                UPSERT_MATERIALIZE_TRANSFORMATION,
                                String.format(
                                        "SinkMaterializer(pk=[%s])",
                                        String.join(", ", pkFieldNames)),
                                "SinkMaterializer",
                                config),
                        operator,
                        inputTransform.getOutputType(),
                        sinkParallelism,
                        sinkParallelismConfigured);
        RowDataKeySelector keySelector =
                KeySelectorUtil.getRowDataSelector(
                        classLoader, primaryKeys, InternalTypeInfo.of(physicalRowType));
        materializeTransform.setStateKeySelector(keySelector);
        materializeTransform.setStateKeyType(keySelector.getProducedType());
        return materializeTransform;
    }

    private OneInputStreamOperator<RowData, RowData> createSumOperator(
            ExecNodeConfig config,
            RowType physicalRowType,
            int[] inputUpsertKey,
            GeneratedRecordEqualiser upsertKeyEqualiser,
            GeneratedHashFunction upsertKeyHashFunction,
            StateTtlConfig ttlConfig,
            GeneratedRecordEqualiser rowEqualiser,
            GeneratedHashFunction rowHashFunction) {

        SinkUpsertMaterializeStrategy sinkUpsertMaterializeStrategy =
                Optional.ofNullable(upsertMaterializeStrategy)
                        .orElse(SinkUpsertMaterializeStrategy.LEGACY);

        return sinkUpsertMaterializeStrategy == SinkUpsertMaterializeStrategy.LEGACY
                ? SinkUpsertMaterializer.create(
                        ttlConfig,
                        physicalRowType,
                        rowEqualiser,
                        upsertKeyEqualiser,
                        inputUpsertKey)
                : SinkUpsertMaterializerV2.create(
                        physicalRowType,
                        rowEqualiser,
                        upsertKeyEqualiser,
                        rowHashFunction,
                        upsertKeyHashFunction,
                        inputUpsertKey,
                        createStateConfig(
                                sinkUpsertMaterializeStrategy,
                                TimeDomain.EVENT_TIME,
                                ttlConfig,
                                config));
    }

    private static SequencedMultiSetStateConfig createStateConfig(
            SinkUpsertMaterializeStrategy strategy,
            TimeDomain ttlTimeDomain,
            StateTtlConfig ttlConfig,
            ReadableConfig config) {
        if (ttlConfig.isEnabled()) {
            // https://issues.apache.org/jira/browse/FLINK-38463
            LOG.warn("TTL is not supported and will be disabled: {}", ttlConfig);
            ttlConfig = StateTtlConfig.DISABLED;
        }
        switch (strategy) {
            case VALUE:
                return SequencedMultiSetStateConfig.forValue(ttlTimeDomain, ttlConfig);
            case MAP:
                return SequencedMultiSetStateConfig.forMap(ttlTimeDomain, ttlConfig);
            case ADAPTIVE:
                return SequencedMultiSetStateConfig.adaptive(
                        ttlTimeDomain,
                        config.get(TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH),
                        config.get(TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW),
                        ttlConfig);
            default:
                throw new IllegalArgumentException("Unsupported strategy: " + strategy);
        }
    }

    @Override
    protected final boolean legacyPhysicalTypeEnabled() {
        return getVersion() == 1;
    }
}
