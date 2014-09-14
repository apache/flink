package ${package}

import org.apache.flink.api.scala._

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   mvn clean package
 * }}}
 * in the projects root directory. You will find the jar in
 * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    /**
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     * env.readTextFile(textPath);
     *
     * then, transform the resulting DataSet[String] using operations
     * like:
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide for the Scala API:
     *
     * http://flink.incubator.apache.org/docs/0.6-SNAPSHOT/java_api_guide.html
     *
     * and the examples
     *
     * http://flink.incubator.apache.org/docs/0.6-SNAPSHOT/java_api_examples.html
     *
     */


    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}
