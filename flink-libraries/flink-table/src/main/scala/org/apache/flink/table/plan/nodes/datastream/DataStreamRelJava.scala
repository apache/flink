package org.apache.flink.table.plan.nodes.datastream

package org.apache.flink.table.plan.nodes.datastream;

import org.apache.calcite.rel.RelNode 
import org.apache.flink.api.common.typeinfo.TypeInformation 
import org.apache.flink.streaming.api.datastream.DataStream 
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel} 
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet} 

 
 


abstract class DataStreamRelJava( 
			     cluster: RelOptCluster, 
			     traitSet: RelTraitSet, 
			     input: RelNode 
			     ) 
			   extends SingleRel(cluster, traitSet, input) 
			   with DataStreamRel { 
	 /** 
	     * Translates the FlinkRelNode into a Flink operator. 
	     * 
	     * @param tableEnv The [[StreamTableEnvironment]] of the translated Table. 
	     * @param expectedType specifies the type the Flink operator should return. The type must 
	     *                     have the same arity as the result. For instance, if the 
	     *                     expected type is a RowTypeInfo this method will return a DataSet of 
	     *                     type Row. If the expected type is Tuple2, the operator will return 
	     *                     a Tuple2 if possible. Row otherwise. 
	     * @return DataStream of type expectedType or RowTypeInfo 
	     */ 

	
}