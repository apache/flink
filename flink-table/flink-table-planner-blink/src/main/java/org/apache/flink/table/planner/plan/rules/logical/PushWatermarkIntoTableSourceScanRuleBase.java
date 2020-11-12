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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Option;

/**
 * Base rule for interface {@link SupportsWatermarkPushDown}. It offers a util to push the {@link FlinkLogicalWatermarkAssigner}
 * into the {@link FlinkLogicalTableSourceScan}.
 */
public abstract class PushWatermarkIntoTableSourceScanRuleBase extends RelOptRule {

	public PushWatermarkIntoTableSourceScanRuleBase(RelOptRuleOperand operand,
			String description) {
		super(operand, description);
	}

	/**
	 * It uses the input watermark expression to generate the {@link WatermarkGeneratorSupplier}. After the {@link WatermarkStrategy}
	 * is pushed into the scan, it will build a new scan. However, when {@link FlinkLogicalWatermarkAssigner} is the parent of the
	 * {@link FlinkLogicalTableSourceScan} it should modify the rowtime type to keep the type of plan is consistent. In other cases,
	 * it just keep the data type of the scan as same as before and leave the work when rewriting the projection.
	 *
	 * <p>NOTES: the row type of the scan is not always as same as the watermark assigner. Because the scan will not add the rowtime
	 * column into the row when pushing the watermark assigner into the scan. In some cases, query may have computed columns defined on
	 * rowtime column. If modifying the type of the rowtime(with time attribute), it will also influence the type of the computed column.
	 * Therefore, if the watermark assigner is not the parent of the scan, set the type of the scan as before and leave the work to
	 * projection.
	 */
	protected FlinkLogicalTableSourceScan getNewScan(
			FlinkLogicalWatermarkAssigner watermarkAssigner,
			RexNode watermarkExpr,
			FlinkLogicalTableSourceScan scan,
			TableConfig tableConfig,
			boolean useWatermarkAssignerRowType) {

		GeneratedWatermarkGenerator generatedWatermarkGenerator =
				WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
						tableConfig,
						FlinkTypeFactory.toLogicalRowType(scan.getRowType()),
						watermarkExpr,
						Option.apply("context"));
		Configuration configuration = tableConfig.getConfiguration();

		WatermarkGeneratorSupplier<RowData> supplier = new DefaultWatermarkGeneratorSupplier(configuration, generatedWatermarkGenerator);
		String digest = String.format("watermark=[%s]", watermarkExpr);

		WatermarkStrategy<RowData> watermarkStrategy = WatermarkStrategy.forGenerator(supplier);
		Duration idleTimeout = configuration.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT);
		if (!idleTimeout.isZero() && !idleTimeout.isNegative()) {
			watermarkStrategy.withIdleness(idleTimeout);
			digest = String.format("%s, idletimeout=[%s]", digest, idleTimeout.toMillis());
		}

		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		DynamicTableSource newDynamicTableSource = tableSourceTable.tableSource().copy();

		((SupportsWatermarkPushDown) newDynamicTableSource).applyWatermark(watermarkStrategy);
		// scan row type
		TableSourceTable newTableSourceTable;
		if (useWatermarkAssignerRowType) {
			// project is trivial and set rowtime type in scan
			newTableSourceTable = tableSourceTable.copy(
					newDynamicTableSource,
					watermarkAssigner.getRowType(),
					new String[]{digest});
		} else {
			// project add/delete columns and set the rowtime column type in project
			newTableSourceTable = tableSourceTable.copy(
					newDynamicTableSource,
					scan.getRowType(),
					new String[]{digest});
		}

		return FlinkLogicalTableSourceScan.create(scan.getCluster(), newTableSourceTable);
	}

	protected boolean supportsWatermarkPushDown(FlinkLogicalTableSourceScan scan) {
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		return tableSourceTable != null && tableSourceTable.tableSource() instanceof SupportsWatermarkPushDown;
	}

	/**
	 * Wrapper of the {@link GeneratedWatermarkGenerator} that is used to create {@link WatermarkGenerator}.
	 * The {@link DefaultWatermarkGeneratorSupplier} uses the {@link WatermarkGeneratorSupplier.Context} to init
	 * the generated watermark generator.
	 */
	private static class DefaultWatermarkGeneratorSupplier implements WatermarkGeneratorSupplier<RowData> {

		private static final long serialVersionUID = 1L;

		private final Configuration configuration;
		private final GeneratedWatermarkGenerator generatedWatermarkGenerator;

		public DefaultWatermarkGeneratorSupplier(Configuration configuration,
				GeneratedWatermarkGenerator generatedWatermarkGenerator) {
			this.configuration = configuration;
			this.generatedWatermarkGenerator = generatedWatermarkGenerator;
		}

		@Override
		public WatermarkGenerator<RowData> createWatermarkGenerator(Context context) {

			List<Object> references = new ArrayList<>(Arrays.asList(generatedWatermarkGenerator.getReferences()));
			references.add(context);

			org.apache.flink.table.runtime.generated.WatermarkGenerator innerWatermarkGenerator =
					new GeneratedWatermarkGenerator(
							generatedWatermarkGenerator.getClassName(),
							generatedWatermarkGenerator.getCode(),
							references.toArray())
							.newInstance(Thread.currentThread().getContextClassLoader());

			try {
				innerWatermarkGenerator.open(configuration);
			} catch (Exception e) {
				throw new RuntimeException("Fail to instantiate generated watermark generator.", e);
			}
			return new DefaultWatermarkGeneratorSupplier.DefaultWatermarkGenerator(innerWatermarkGenerator);
		}

		/**
		 * Wrapper of the code-generated {@link org.apache.flink.table.runtime.generated.WatermarkGenerator}.
		 */
		private static class DefaultWatermarkGenerator implements WatermarkGenerator<RowData> {

			private static final long serialVersionUID = 1L;

			private final org.apache.flink.table.runtime.generated.WatermarkGenerator innerWatermarkGenerator;
			private Long currentWatermark = Long.MIN_VALUE;

			public DefaultWatermarkGenerator(
					org.apache.flink.table.runtime.generated.WatermarkGenerator watermarkGenerator) {
				this.innerWatermarkGenerator = watermarkGenerator;
			}

			@Override
			public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
				try {
					Long watermark = innerWatermarkGenerator.currentWatermark(event);
					if (watermark != null) {
						currentWatermark = watermark;
					}
				} catch (Exception e) {
					throw new RuntimeException(
							String.format("Generated WatermarkGenerator fails to generate for row: %s.", event), e);
				}
			}

			@Override
			public void onPeriodicEmit(WatermarkOutput output) {
				output.emitWatermark(new Watermark(currentWatermark));
			}
		}
	}
}
