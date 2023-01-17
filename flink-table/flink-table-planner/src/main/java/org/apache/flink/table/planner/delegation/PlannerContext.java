package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.tools.FrameworkConfig;

/** Utility Interface implemented by Flink Planner as well as external Planner. */
@PublicEvolving
public interface PlannerContext {

    public RexFactory getRexFactory();

    public FlinkCalciteCatalogReader createCatalogReader(boolean lenientCaseSensitivity);

    public FrameworkConfig createFrameworkConfig();

    public RelOptCluster getCluster();

    public FlinkContext getFlinkContext();

    public FlinkTypeFactory getTypeFactory();

    public FlinkPlannerImpl createFlinkPlanner();

    public FlinkRelBuilder createRelBuilder();

    public CalciteParser createCalciteParser();
}
