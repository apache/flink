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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import scala.Option;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

/** Tests for {@link FlinkRelMdImmutableColumns}. */
public class FlinkRelMdImmutableColumnsTest extends FlinkRelMdHandlerTestBase {

    // -------------------------------------------------------------------------------------
    // TableScan
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnTableScanWithImmutableCols() {
        // Projected rowType (a,c,d): PK(a)=0, immutable(c)=1, immutable(d)=2 → {0, 1, 2}
        assertEquals(
                ImmutableBitSet.of(0, 1, 2),
                mq().getImmutableColumns(tableWithImmutableColsLogicalScan()));
    }

    @Test
    public void testGetImmutableColumnsOnTableScanWithPkOnly() {
        // TableSourceTable1: PK(a,b)={0,1}, no immutable constraint → {0, 1}
        RelNode scan = relBuilder().scan("TableSourceTable1").build();
        assertEquals(ImmutableBitSet.of(0, 1), mq().getImmutableColumns(scan));
    }

    @Test
    public void testGetImmutableColumnsOnTableScanWithSingleColPk() {
        // TableSourceTable2: PK(b)={1} → {1}
        RelNode scan = relBuilder().scan("TableSourceTable2").build();
        assertEquals(ImmutableBitSet.of(1), mq().getImmutableColumns(scan));
    }

    @Test
    public void testGetImmutableColumnsOnTableScanWithoutPk() {
        // TableSourceTable3: no PK → null
        RelNode scan = relBuilder().scan("TableSourceTable3").build();
        assertNull(mq().getImmutableColumns(scan));
    }

    @Test
    public void testGetImmutableColumnsOnNonTableSourceTableScan() {
        // student uses MockMetaTable (not TableSourceTable) → null
        assertNull(mq().getImmutableColumns(studentLogicalScan()));
    }

    @Test
    public void testGetImmutableColumnsOnSourceWithDelete() {
        MockTableSourceTable tst =
                (MockTableSourceTable)
                        requireNonNull(((CalciteCatalogReader) relBuilder().getRelOptSchema()))
                                .getTable(
                                        Collections.singletonList(
                                                "projected_table_source_table_with_immutable_cols"));
        assertNotNull(tst);

        DynamicTableSource newTableSource =
                new TestTableSource() {
                    @Override
                    public ChangelogMode getChangelogMode() {
                        return ChangelogMode.all();
                    }
                };

        tst = tst.copy(newTableSource, tst.getRowType());
        TableScan scan =
                new StreamPhysicalDataStreamScan(
                        cluster(), logicalTraits(), Collections.emptyList(), tst, tst.getRowType());

        assertThrowsExactly(
                ValidationException.class,
                () -> mq().getImmutableColumns(scan),
                "The immutable constraint cannot be defined on the table with changelog mode [DELETE]");
    }

    // -------------------------------------------------------------------------------------
    // Project
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnProjectKeepsAll() {
        // Project: a(0), c(1), d(2) → output immutable = {0, 1, 2}
        relBuilder().push(tableWithImmutableColsLogicalScan());
        RelNode project =
                relBuilder()
                        .project(
                                relBuilder().field(0), // a → out 0
                                relBuilder().field(1), // c → out 1
                                relBuilder().field(2)) // d → out 2
                        .build();
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(project));
    }

    @Test
    public void testGetImmutableColumnsOnProjectDropsImmutableCols() {
        // Project with only expressions (no direct field refs) → none tracked → empty
        relBuilder().push(tableWithImmutableColsLogicalScan());
        RelNode project =
                relBuilder()
                        .project(
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.PLUS,
                                                relBuilder().field(0),
                                                relBuilder().literal(1)))
                        .build();
        assertEquals(ImmutableBitSet.of(), mq().getImmutableColumns(project));
    }

    @Test
    public void testGetImmutableColumnsOnProjectWithDuplicateRefs() {
        // Project: a(0), a(0), c(1) → a maps to {0, 1}, c maps to {2} → {0, 1, 2}
        relBuilder().push(tableWithImmutableColsLogicalScan());
        RelNode project =
                relBuilder()
                        .project(
                                relBuilder().field(0), // a → out 0
                                relBuilder().field(0), // a → out 1
                                relBuilder().field(1)) // c → out 2
                        .build();
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(project));
    }

    @Test
    public void testGetImmutableColumnsOnProjectWithExpression() {
        // Project: a(0), a+1 (expression), c(1)
        // Expressions (non-RexInputRef) are not tracked → a+1 is not immutable
        relBuilder().push(tableWithImmutableColsLogicalScan());
        RelNode project =
                relBuilder()
                        .project(
                                relBuilder().field(0), // a → out 0
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.PLUS,
                                                relBuilder().field(0),
                                                relBuilder().literal(1)), // a+1 → out 1
                                relBuilder().field(1)) // c → out 2
                        .build();
        // a→0, c→2 → {0, 2}
        assertEquals(ImmutableBitSet.of(0, 2), mq().getImmutableColumns(project));
    }

    @Test
    public void testGetImmutableColumnsOnProjectNullInput() {
        // Project on student (MockMetaTable immutable = null) → null
        relBuilder().push(studentLogicalScan());
        RelNode project =
                relBuilder().project(relBuilder().field(0), relBuilder().field(1)).build();
        assertNull(mq().getImmutableColumns(project));
    }

    // -------------------------------------------------------------------------------------
    // Filter
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnFilter() {
        // Filter passes through immutable columns unchanged
        relBuilder().push(tableWithImmutableColsLogicalScan());
        RelNode filter =
                relBuilder()
                        .filter(
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.LESS_THAN,
                                                relBuilder().field(0),
                                                relBuilder().literal(100)))
                        .build();
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(filter));
    }

    @Test
    public void testGetImmutableColumnsOnFilterNullInput() {
        // Filter on student (MockMetaTable immutable = null) → null
        assertNull(mq().getImmutableColumns(logicalFilter()));
    }

    // -------------------------------------------------------------------------------------
    // Calc
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnCalc() {
        // Calc with projection [a(0), c(2)] and filter a < 100
        RelNode input = tableWithImmutableColsLogicalScan();
        relBuilder().push(input);

        RexNode proj0 = relBuilder().field(0); // a
        RexNode proj1 = relBuilder().field(1); // c
        List<RexNode> projects = Arrays.asList(proj0, proj1);

        RexNode condition =
                relBuilder()
                        .call(
                                SqlStdOperatorTable.LESS_THAN,
                                relBuilder().field(0),
                                relBuilder().literal(100));
        List<RexNode> conditions = Collections.singletonList(condition);

        // Build a temp project to get the output row type
        RelNode tempProject = relBuilder().project(proj0, proj1).build();
        RelDataType outputRowType = tempProject.getRowType();

        Calc calc = createLogicalCalc(input, outputRowType, projects, conditions);

        // Input immutable: {0, 1, 2} → a→0, c→1 → {0, 1}
        assertEquals(ImmutableBitSet.of(0, 1), mq().getImmutableColumns(calc));
    }

    // -------------------------------------------------------------------------------------
    // WatermarkAssigner
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnWatermarkAssigner() {
        // Build a WatermarkAssigner on top of the immutable scan, using rowtime column (index 3)
        RelNode input = tableWithImmutableColsLogicalScan();
        FlinkContext flinkContext = unwrapContext(cluster());
        RexNode watermarkExpr =
                flinkContext
                        .getRexFactory()
                        .createSqlToRexConverter(input.getRowType(), null)
                        .convertToRexNode("rowtime - INTERVAL '10' SECOND");
        RelNode watermarkAssigner =
                LogicalWatermarkAssigner.create(
                        cluster(), input, Collections.emptyList(), 3, watermarkExpr);
        // Pass through: {0, 1, 2}
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(watermarkAssigner));
    }

    // -------------------------------------------------------------------------------------
    // MiniBatchAssigner
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnMiniBatchAssigner() {
        RelNode input = tableWithImmutableColsStreamScan();
        RelNode miniBatchAssigner =
                new StreamPhysicalMiniBatchAssigner(cluster(), streamPhysicalTraits(), input);
        // Pass through: {0, 1, 2}
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(miniBatchAssigner));
    }

    // -------------------------------------------------------------------------------------
    // Exchange
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnExchange() {
        RelNode scan = tableWithImmutableColsStreamScan();
        FlinkRelDistribution hash = FlinkRelDistribution.hash(new int[] {0}, true);
        RelNode exchange =
                new StreamPhysicalExchange(
                        cluster(), streamPhysicalTraits().replace(hash), scan, hash);
        // Pass through: {0, 1, 2}
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(exchange));
    }

    // -------------------------------------------------------------------------------------
    // ChangelogNormalize
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnChangelogNormalize() {
        RelNode scan = tableWithImmutableColsStreamScan();
        RelNode changelogNormalize =
                new StreamPhysicalChangelogNormalize(
                        cluster(),
                        streamPhysicalTraits(),
                        scan,
                        new int[] {0},
                        null,
                        false,
                        new RexNode[] {});
        // Pass through: {0, 1, 2}
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(changelogNormalize));
    }

    // -------------------------------------------------------------------------------------
    // DropUpdateBefore
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnDropUpdateBefore() {
        RelNode scan = tableWithImmutableColsStreamScan();
        RelNode dropUpdateBefore =
                new StreamPhysicalDropUpdateBefore(cluster(), streamPhysicalTraits(), scan);
        // Pass through: {0, 1, 2}
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(dropUpdateBefore));
    }

    // -------------------------------------------------------------------------------------
    // Join
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnInnerJoin1() {
        // Left: projected_table_source_table_with_immutable_cols (a,c,d,rowtime) →
        // immutable={0,1,2}
        // Right: TableSourceTable1 (a,b,c,d) → immutable={0,1}
        // Right shifted by 4: {4,5}
        // Union: {0,1,2,4,5}
        RelNode join =
                relBuilder()
                        .scan("projected_table_source_table_with_immutable_cols")
                        .scan("TableSourceTable1")
                        .join(
                                JoinRelType.INNER,
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder().field(2, 0, 0),
                                                relBuilder().field(2, 1, 0)))
                        .build();
        assertEquals(ImmutableBitSet.of(0, 1, 2, 4, 5), mq().getImmutableColumns(join));
    }

    @Test
    public void testGetImmutableColumnsOnInnerJoin2() {
        // Left: projected_table_source_table_with_immutable_cols → {0,1,2}
        // Right: projected_table_source_table_with_immutable_cols → {0,1,2} shifted by 4 → {4,5,6}
        // Union: {0,1,2,4,5,6}
        RelNode join =
                relBuilder()
                        .scan("projected_table_source_table_with_immutable_cols")
                        .scan("projected_table_source_table_with_immutable_cols")
                        .join(
                                JoinRelType.INNER,
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder().field(2, 0, 0),
                                                relBuilder().field(2, 1, 0)))
                        .build();
        assertEquals(ImmutableBitSet.of(0, 1, 2, 4, 5, 6), mq().getImmutableColumns(join));
    }

    @Test
    public void testGetImmutableColumnsOnInnerJoinWhileOneSideNoUpsertKey() {
        // Left: TableSourceTable3 → immutable & pk = null
        // Right: projected_table_source_table_with_immutable_cols → {0,1,2} shifted by 7 → {7,8,9}
        // Join has no upsert keys → guarded to null
        RelNode join =
                relBuilder()
                        .scan("TableSourceTable3")
                        .scan("projected_table_source_table_with_immutable_cols")
                        .join(
                                JoinRelType.INNER,
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder().field(2, 0, 0),
                                                relBuilder().field(2, 1, 0)))
                        .build();
        assertNull(mq().getImmutableColumns(join));

        // Left: projected_table_source_table_with_immutable_cols → {0,1,2}
        // Right: TableSourceTable3 → immutable & pk = null
        // Join has no upsert keys → guarded to null
        join =
                relBuilder()
                        .scan("projected_table_source_table_with_immutable_cols")
                        .scan("TableSourceTable3")
                        .join(
                                JoinRelType.INNER,
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder().field(2, 0, 0),
                                                relBuilder().field(2, 1, 0)))
                        .build();

        assertNull(mq().getImmutableColumns(join));

        // Left: TableSourceTable3 → immutable & pk = null
        // Right: TableSourceTable3 → immutable & pk = null
        // Join has no upsert keys → guarded to null
        join =
                relBuilder()
                        .scan("TableSourceTable3")
                        .scan("TableSourceTable3")
                        .join(
                                JoinRelType.INNER,
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder().field(2, 0, 0),
                                                relBuilder().field(2, 1, 0)))
                        .build();

        assertNull(mq().getImmutableColumns(join));
    }

    @Test
    public void testGetImmutableColumnsOnLeftJoin() {
        // LEFT JOIN: right side may produce nulls → ignore right immutable
        // Left: projected_table_source_table_with_immutable_cols → {0,1,2}
        // Result: {0,1,2}
        RelNode join =
                relBuilder()
                        .scan("projected_table_source_table_with_immutable_cols")
                        .scan("projected_table_source_table_with_immutable_cols")
                        .join(
                                JoinRelType.LEFT,
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder().field(2, 0, 0),
                                                relBuilder().field(2, 1, 0)))
                        .build();
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(join));
    }

    @Test
    public void testGetImmutableColumnsOnRightJoin() {
        // RIGHT JOIN: left side may produce nulls → ignore left immutable
        // Right: projected_table_source_table_with_immutable_cols → shifted by 4 → {4,5,6}
        // Result: {4,5,6}
        RelNode join =
                relBuilder()
                        .scan("projected_table_source_table_with_immutable_cols")
                        .scan("projected_table_source_table_with_immutable_cols")
                        .join(
                                JoinRelType.RIGHT,
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder().field(2, 0, 0),
                                                relBuilder().field(2, 1, 0)))
                        .build();
        assertEquals(ImmutableBitSet.of(4, 5, 6), mq().getImmutableColumns(join));
    }

    @Test
    public void testGetImmutableColumnsOnFullJoin() {
        // FULL JOIN: both sides may produce nulls → both ignored → null
        RelNode join =
                relBuilder()
                        .scan("projected_table_source_table_with_immutable_cols")
                        .scan("projected_table_source_table_with_immutable_cols")
                        .join(
                                JoinRelType.FULL,
                                relBuilder()
                                        .call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder().field(2, 0, 0),
                                                relBuilder().field(2, 1, 0)))
                        .build();
        assertNull(mq().getImmutableColumns(join));
    }

    // -------------------------------------------------------------------------------------
    // Lookup Join
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnLookupJoinWithImmutableCols() {
        // Left: projected_table_source_table_with_immutable_cols → {0,1,2}
        // Right projected_table_source_table_with_immutable_cols = {0,1,2}, ignored
        // Result: {0,1,2}
        TableScan src = tableWithImmutableColsStreamScan();
        StreamPhysicalLookupJoin lookupJoin =
                getStreamLookupJoinsWithImmutableCols(
                        src,
                        src.getTable(),
                        JoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(0)),
                        JoinRelType.INNER,
                        Option.empty());
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(lookupJoin));

        lookupJoin =
                getStreamLookupJoinsWithImmutableCols(
                        src,
                        src.getTable(),
                        JoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(0)),
                        JoinRelType.LEFT,
                        Option.empty());
        assertEquals(ImmutableBitSet.of(0, 1, 2), mq().getImmutableColumns(lookupJoin));

        // join without lookup side's pk
        // Result: null
        lookupJoin =
                getStreamLookupJoinsWithImmutableCols(
                        src,
                        src.getTable(),
                        JoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(1)),
                        JoinRelType.LEFT,
                        Option.empty());
        assertNull(mq().getImmutableColumns(lookupJoin));
    }

    // -------------------------------------------------------------------------------------
    // Default (catch-all)
    // -------------------------------------------------------------------------------------

    @Test
    public void testGetImmutableColumnsOnDefault() {
        // TestRel has no specific handler → catch-all returns null
        assertNull(mq().getImmutableColumns(testRel()));
    }

    @Test
    public void testGetImmutableColumnsOnValues() {
        // LogicalValues has no specific handler → catch-all returns null
        assertNull(mq().getImmutableColumns(logicalValues()));
        assertNull(mq().getImmutableColumns(emptyValues()));
    }
}
