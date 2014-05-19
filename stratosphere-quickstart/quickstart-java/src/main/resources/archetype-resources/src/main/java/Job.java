package ${package};

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.record.io.DelimitedOutputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.util.Collector;



/**
 * Skeleton for a Stratosphere Job.
 * 
 * For a full example of a Stratosphere Job, see the WordCountJob.java file in the 
 * same package/directory or have a look at the website.
 * 
 * You can also generate a .jar file that you can submit on your Stratosphere
 * cluster.
 * Just type 
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in 
 * 		target/stratosphere-quickstart-0.1-SNAPSHOT-Sample.jar
 * 
 */
@SuppressWarnings("serial")
public class Job {
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		
		/**
		 * Here, you can start creating your execution plan for Stratosphere.
		 * 
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 * 
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.group()
		 * and many more.
		 * 
		 * Run it!
		 * 
		 */
		
		// execute program
		env.execute(" Example");
	}
}