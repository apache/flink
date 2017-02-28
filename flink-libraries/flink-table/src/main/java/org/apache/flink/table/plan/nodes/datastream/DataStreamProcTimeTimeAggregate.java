package org.apache.flink.table.plan.nodes.datastream;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.logical.rel.util.WindowAggregateUtil;
import org.apache.flink.table.plan.nodes.datastream.function.DataStreamProcTimeAggregateGlobalWindowFunction;
import org.apache.flink.table.plan.nodes.datastream.function.DataStreamProcTimeAggregateWindowFunction;
import org.apache.flink.table.typeutils.TypeConverter;

import scala.Option;

public class DataStreamProcTimeTimeAggregate extends DataStreamRelJava {

	private LogicalWindow windowReference;
	private String description;

	public DataStreamProcTimeTimeAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
			RelDataType rowType, String description, LogicalWindow windowReference) {
		super(cluster, traitSet, input);

		this.rowType = rowType;
		this.description = description;
		this.windowReference = windowReference;

	}

	@Override
	protected RelDataType deriveRowType() {
		// TODO Auto-generated method stub
		return super.deriveRowType();
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, java.util.List<RelNode> inputs) {
	
		if(inputs.size()!=1){
			System.err.println(this.getClass().getName()+" : Input size must be one!");
		}
		
		return new DataStreamProcTimeTimeAggregate(
				getCluster(), 
				traitSet, 
				inputs.get(0),
				getRowType(), getDescription(), windowReference);
		
	}

	@Override
	public DataStream<Object> translateToPlan(StreamTableEnvironment tableEnv,
			Option<TypeInformation<Object>> expectedType, Object ignore) {

		// Get the general parameters related to the datastream, inputs, types,
		// result
		TableConfig config = tableEnv.getConfig();

		scala.Option<TypeInformation<Object>> option = scala.Option.apply(null);
		DataStream<Object> inputDataStream = ((DataStreamRel) getInput()).translateToPlan(tableEnv, option);

		TypeInformation<?> returnType = TypeConverter.determineReturnType(getRowType(), expectedType,
				config.getNullCheck(), config.getEfficientTypeUsage());

		// WindowUtil object to help with operations on the LogicalWindow object
		WindowAggregateUtil windowUtil = new WindowAggregateUtil(windowReference);
		int[] partitionKeys = windowUtil.getPartitions();
		
		
		//get aggregations
		List<TypeInformation<?>> typeOutput = new ArrayList<>();
		List<TypeInformation<?>> typeInput = new ArrayList<>();
		List<String> aggregators = new ArrayList<>();
		List<Integer> indexes = new ArrayList<>();	
		windowUtil.getAggregations(aggregators,typeOutput,indexes,typeInput);

		final long time_boundary = Long.parseLong(windowReference.getConstants().get(1).getValue().toString());


		// null indicates non partitioned window
		if (partitionKeys == null) {
			
			inputDataStream.windowAll(GlobalWindows.create())
					.trigger(CountTrigger.of(1))
					.evictor(TimeEvictor.of(Time.milliseconds(time_boundary)))
					.apply(new DataStreamProcTimeAggregateGlobalWindowFunction(aggregators,indexes,typeOutput,typeInput))
					.returns((TypeInformation<Object>) returnType);

			
		} else {
			//inputDataStream =
					inputDataStream.keyBy(partitionKeys)
					.window(GlobalWindows.create())
					.trigger(CountTrigger.of(1))
					.evictor(TimeEvictor.of(Time.milliseconds(time_boundary)))
					.apply(new DataStreamProcTimeAggregateWindowFunction(aggregators,indexes,typeOutput,typeInput))
					.returns((TypeInformation<Object>) returnType);
		}

		return inputDataStream;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString() + "(" + "window=[" + windowReference + "]" + ")";
	}

	@Override
	public void explain(RelWriter pw) {
		// TODO Auto-generated method stub
		super.explain(pw);
	}

	@Override
	public RelWriter explainTerms(RelWriter pw) {
		/*
		 * pw.item("Type", winConf.type); pw.item("Order",
		 * winConf.operateField); pw.item("PartitionBy", winConf.partitionBy);
		 * pw.itemIf("Event-based", winConf.eventWindow, winConf.eventWindow);
		 * pw.itemIf("Time-based", winConf.timeWindow, winConf.timeWindow);
		 * pw.item("LowBoundary", winConf.referenceLowBoundary);
		 * pw.itemIf("LowBoundary constant", winConf.lowBoudary,
		 * winConf.lowBoudary != 0); pw.item("HighBoundary",
		 * winConf.referenceHighBoundary); pw.itemIf("HighBoundary constant",
		 * winConf.highBoudary, winConf.highBoudary != 0);
		 */
		return pw;
	}

}
