package ${package};


import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.scala.TextFile
import eu.stratosphere.api.scala.ScalaPlan
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.client.RemoteExecutor

// You can run this locally using:
// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath ${package}.RunJobLocal 2 file:///some/path file:///some/other/path"
object RunJobLocal {
  def main(args: Array[String]) {
    val job = new Job
    if (args.size < 3) {
      println(job.getDescription)
      return
    }
    val plan = job.getScalaPlan(args(0).toInt, args(1), args(2))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

// You can run this on a cluster using:
// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath ${package}.RunJobRemote 2 file:///some/path file:///some/other/path"
object RunJobRemote {
  def main(args: Array[String]) {
    val job = new Job
    if (args.size < 3) {
      println(job.getDescription)
      return
    }
    val plan = job.getScalaPlan(args(0).toInt, args(1), args(2))
    // This will create an executor to run the plan on a cluster. We assume
    // that the JobManager is running on the local machine on the default
    // port. Change this according to your configuration.
    // You will also need to change the name of the jar if you change the
    // project name and/or version. Before running this you also need
    // to run "mvn package" to create the jar.
    val ex = new RemoteExecutor("localhost", 6123, "target/stratosphere-project-0.1-SNAPSHOT.jar");
    ex.executePlan(plan);
  }
}


/**
 * This is a outline for a Stratosphere scala job. It is actually the WordCount 
 * example from the stratosphere distribution.
 * 
 * You can run it out of your IDE using the main() method of RunJob.
 * This will use the LocalExecutor to start a little Stratosphere instance
 * out of your IDE.
 * 
 * You can also generate a .jar file that you can submit on your Stratosphere
 * cluster.
 * Just type 
 *      mvn clean package
 * in the projects root directory.
 * You will find the jar in 
 *      target/stratosphere-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
class Job extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: [numSubStasks] [input] [output]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2))
  }

  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)

  def getScalaPlan(numSubTasks: Int, textInput: String, wordsOutput: String) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } map { (_, 1) } }
    val counts = words groupBy { case (word, _) => word } reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

    counts neglects { case (word, _) => word }
    counts preserves({ case (word, _) => word }, { case (word, _) => word })
    val output = counts.write(wordsOutput, DelimitedOutputFormat(formatOutput.tupled))
  
    val plan = new ScalaPlan(Seq(output), "Word Count (immutable)")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}
