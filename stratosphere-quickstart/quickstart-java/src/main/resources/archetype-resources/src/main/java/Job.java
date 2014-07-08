package ${package};

import eu.stratosphere.api.java.ExecutionEnvironment;

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
		 * Have a look at the programming guide for the Java API:
		 * 
		 * http://stratosphere.eu/docs/0.5/programming_guides/java.html
		 * 
		 * and the examples
		 * 
		 * http://stratosphere.eu/docs/0.5/programming_guides/examples.html
		 * 
		 */
		
		// execute program
		env.execute("Stratosphere Java API Skeleton");
	}
}