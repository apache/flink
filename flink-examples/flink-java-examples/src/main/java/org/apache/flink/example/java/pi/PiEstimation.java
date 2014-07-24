package org.apache.flink.example.java.pi;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.IterativeDataSet;
import org.apache.flink.api.java.operators.ReduceGroupOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.functions.*;
import org.apache.flink.example.java.graph.util.PageRankData;
import org.apache.flink.util.Collector;


/** 
 * Estimates the value of Pi using the Monte Carlo method.
 * The area of a circle is Pi * R^2, R being the radius of the circle 
 * The area of a square is 4 * R^2, where the length of the square's edge is 2*R.
 * 
 * Thus Pi = 4 * (area of circle / area of square).
 * 
 * The idea is to find a way to estimate the circle to square area ratio.
 * The Monte Carlo method suggests collecting random points (within the square)
 * ```
 * x = Math.random() * 2 - 1
 * y = Math.random() * 2 - 1
 * ```
 * then counting the number of points that fall within the circle 
 * ```
 * x * x + y * y < 1
 * ```
 */
public class PiEstimation {
	
	static int n;
	
	public static void main(String[] args) throws Exception {
		
	  	int blocks = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
	  	n = 100000 * blocks;
	  	List<Integer> l = new ArrayList<Integer>(n);
	  	for (int i = 0; i < n; i++) {
	  		l.add(i);
	  	}
	  	
	  	//Sets up the execution environment
	  	final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	  	DataSet<Integer> dataSet = env.fromCollection(l);

	  	DataSet<Double> count = dataSet
    		.map(new PiMapper())
    		.setParallelism(blocks)
    		.reduceGroup(new PiReducer());
	  	
	  	System.out.println("We estimate Pi to be:");
	  	count.print();
	  	
	  	env.execute();
  }
  
  
  //*************************************************************************
  //     USER FUNCTIONS
  //*************************************************************************
	
	/** 
	 * PiMapper randomly collects points that fall within a square of edge 2*x = 2*y = 2.
	 * It calculates the distance to the center of a virtually centered circle of radius x = y = 1
	 * It returns 1 if the distance is less than 1, else 0, meaning that the point is not outside the circle.
	 */
	public static final class PiMapper extends MapFunction<Integer,Integer> {

		@Override
		public Integer map(Integer value) throws Exception {
			double x = Math.random() * 2 - 1;
			double y = Math.random() * 2 - 1;
			return (x * x + y * y < 1) ? 1 : 0;
		}
	}
	
	/** 
	 * PiReducer takes over the mapper. It goes through the produced 0s and 1s and returns the sum.
	 * The final operation takes place in the collector which returns the final Pi value.
	 */
	public static final class PiReducer extends GroupReduceFunction<Integer,Double> {

		@Override
		public void reduce(Iterator<Integer> values, Collector<Double> out) throws Exception {
			int intSum = 0;
			while(values.hasNext()){
				intSum += values.next();
			}
			out.collect((double) (intSum*4.0 / n));
		}
	}
}