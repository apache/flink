package org.apache.flink.table.plan.logical.rel.util;

import java.util.List;

import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.flink.api.java.tuple.Tuple5;

public class WindowAggregateUtil {


	public List<Tuple5<String,String,String,Integer,Integer>> getAggregateFunctions(LogicalWindow window){
		
		for(RexWinAggCall agg : window.groups.iterator().next().aggCalls){
			
		}
		return null;
		
	}
	
	
	
}
