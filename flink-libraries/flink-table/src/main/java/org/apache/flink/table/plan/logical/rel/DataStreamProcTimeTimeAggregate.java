package org.apache.flink.table.plan.logical.rel;

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
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel;
import org.apache.flink.table.plan.nodes.datastream.DataStreamRelJava;
import org.apache.flink.table.typeutils.TypeConverter;

import scala.Option;

public class DataStreamProcTimeTimeAggregate extends DataStreamRelJava{

	
	private LogicalWindow windowReference;
	private String description;
	private RelNode input;
	
	
	public DataStreamProcTimeTimeAggregate(
			RelOptCluster cluster, 
			RelTraitSet traitSet, 
			RelNode input, 
			RelDataType rowType,
			String description, 
			LogicalWindow windowReference) {
		super(cluster, traitSet, input);

		this.rowType = rowType;
		this.description = description;
		this.windowReference= windowReference;
		this.input = input;
		
		
	}

	@Override
	protected RelDataType deriveRowType() {
		// TODO Auto-generated method stub
		return super.deriveRowType();
	}
	
	@Override
	public RelNode copy(RelTraitSet traitSet, java.util.List<RelNode> inputs) {
		

		return new DataStreamProcTimeTimeAggregate(
				super.getCluster(),
				traitSet,
				inputs.get(0), 
				rowType, 
				description, 
				windowReference);
		
	}
	
	
	
	@Override
	public DataStream<Object> translateToPlan(StreamTableEnvironment tableEnv,
			Option<TypeInformation<Object>> expectedType, Object ignore) {
		

		//Get the general parameters related to the datastream, inputs, types, result
		TableConfig config = tableEnv.getConfig();
		
		scala.Option<TypeInformation<Object>> option = scala.Option.apply(null);
		DataStream<Object> inputDataStream = ((DataStreamRel) input)
				.translateToPlan(tableEnv,option);
		
		TypeInformation<?> returnType = TypeConverter.determineReturnType(
				getRowType(), 
				expectedType,
				config.getNullCheck(), 
				config.getEfficientTypeUsage());
		
		//WindowUtil object to help with operations on the LogicalWindow object 
		WindowAggregateUtil windowUtil = new WindowAggregateUtil(windowReference);
		List<Integer> partitionKeys = windowUtil.getPartitions();				
		
		
		//null indicates non partitioned window
		if(partitionKeys==null)
		{
			final long time_boundary = Long.parseLong(
					windowReference.getConstants()
					.get(1)
					.getValue().toString());
			
			inputDataStream.windowAll(GlobalWindows.create())
			.trigger(CountTrigger.of(1))
				.evictor(TimeEvictor.of(Time.milliseconds(time_boundary)));
				
			
			//Time.of(winConf.lowBoudary, TimeUnit.MILLISECONDS)
		}
		else
		{
			
		}
		
		
		return null;
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
	/*	pw.item("Type", winConf.type);
		pw.item("Order", winConf.operateField);
		pw.item("PartitionBy", winConf.partitionBy);
		pw.itemIf("Event-based", winConf.eventWindow, winConf.eventWindow);
		pw.itemIf("Time-based", winConf.timeWindow, winConf.timeWindow);
		pw.item("LowBoundary", winConf.referenceLowBoundary);
		pw.itemIf("LowBoundary constant", winConf.lowBoudary, winConf.lowBoudary != 0);
		pw.item("HighBoundary", winConf.referenceHighBoundary);
		pw.itemIf("HighBoundary constant", winConf.highBoudary, winConf.highBoudary != 0);
	 */
		return pw;
	}
	
	
}
