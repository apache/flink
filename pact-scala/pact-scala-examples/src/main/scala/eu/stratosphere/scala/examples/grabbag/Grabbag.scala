

package eu.stratosphere.scala.examples.grabbag

import scala.Array.canBuildFrom
import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactString

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import eu.stratosphere.scala.analysis.GlobalSchemaPrinter
import eu.stratosphere.pact.example.util.AsciiUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import eu.stratosphere.scala.analysis.postPass.GlobalSchemaOptimizer
import eu.stratosphere.scala.analysis.GlobalSchemaGenerator


// Grab bag of random scala examples

object Main1 extends Serializable {
  
  class Foo(val a: Int) {}
  
  val fun = new Function1[(String, Int), (String, Int)] {
    lazy val tokenizer = new AsciiUtils.WhitespaceTokenizer()
    val foo = new Foo(3)
    def apply(a: (String, Int)) = {
      println("I GOT: " + a + " AND: " + foo.a)
      tokenizer.setStringToTokenize(new PactString(a._1))
      a
    }
  }

  val readFun = new Function1[String, String] {
    def apply(line: String) = {
      println("reading line: " + line)
      line
    }
  }

  var count = 0
  def parseInput = (line: String) => {
    println("reading line: " + count)
    count = count+1
    line
  }
  
  def addCounts(w1: (String, Int), w2: (String, Int)) = (w1._1, w1._2 + w2._2)


  def main(args: Array[String]) {
//    var logger = Logger.getLogger(classOf[GlobalSchemaOptimizer])
//    logger.setLevel(Level.DEBUG)
//    logger = Logger.getLogger(classOf[GlobalSchemaGenerator])
//    logger.setLevel(Level.DEBUG)

    def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)


    val input = DataSource("file:///home/aljoscha/dummy-input", DelimitedInputFormat(readFun) )
    val inputNumbers = DataSource("file:///home/aljoscha/dummy-input-numbers", CsvInputFormat[(Int, String, String)](Seq(0,2,1), "\n", ','))
    
    val counts = input.map { _.split("""\W+""") map { (_, 1) } }
      .flatMap { l => l }
      .groupBy { case (word, _) => word }
      .combinableReduceGroup { _.reduce { (w1, w2) => (w1._1, w1._2 + w2._2) } }
      .map(fun)
//      .filter { case (w, c) => c == 7 }

    val countsCross = counts.cross(counts)
      .filter { case (left, right) => left._1 == "hier" }
      .map { case (w1, w2) => (w1._1 + " + " + w2._1, w1._2 + w2._2) }
    
    val foo = counts.join(inputNumbers) where { case (_, c) => c}

    
    val bar1 = foo.isEqualTo { case (c, _, _) => c } map { (w1, w2) => (w1._1 + " is ONE " + w2._2, w1._2) }
    bar1.right neglects { case (a,b,c) => c }
    
    val bar2 = foo.isEqualTo { case (c, _, _) => c } map { (w1, w2) => (w1._1 + " is TWO " + w2._2, w1._2) }
    bar2.right neglects { case (a,b,c) => c }

    val countsJoin = counts.join(inputNumbers) where { case (_, c) => c} isEqualTo { case (c, _, _) => c } map { (w1, w2) => (w1._1 + " is " + w2._2, w1._2) }
    countsJoin.left preserves({ case (_, count) => count }, { case (_, count) => count })
    countsJoin.right neglects { case (a,b,c) => c }

    val un = countsCross union countsJoin map { x => x }
    
    val sink0 = counts.reduce { (w1, w2) => ( "Total: " , w1._2 + w2._2) }
      .write("file:///home/aljoscha/dummy-outputCounts-reduce", CsvOutputFormat("\n", ","))
    val sinkm1 = counts.reduceAll { _.reduce { (w1, w2) => ( "Total: " , w1._2 + w2._2) } }
      .write("file:///home/aljoscha/dummy-outputCounts-reduce-all", CsvOutputFormat("\n", ","))
    val sink1 = counts.write("file:///home/aljoscha/dummy-outputCounts", CsvOutputFormat("\n", ","))
    val sink2 = countsCross.write("file:///home/aljoscha/dummy-outputCross", CsvOutputFormat("\n", ","))
    val sink3 = countsJoin.write("file:///home/aljoscha/dummy-outputJoin", CsvOutputFormat("\n", ","))
    val sink4 = un.write("file:///home/aljoscha/dummy-outputUnion", CsvOutputFormat("\n", ","))
    val sink5 = bar1.write("file:///home/aljoscha/dummy-outputbar1", CsvOutputFormat("\n", ","))
    val sink6 = bar2.write("file:///home/aljoscha/dummy-outputbar2", CsvOutputFormat("\n", ","))
    
    val plan = new ScalaPlan(Seq(sinkm1, sink0, sink1, sink2, sink3, sink4, sink5, sink6), "SCALA DUMMY JOBB")
    GlobalSchemaPrinter.printSchema(plan)
//    input uniqueKey { x => x }
    
    val ex = new LocalExecutor()
    ex.start()
    ex.executePlan(plan)
    ex.stop()
    
    System.exit(0)
  }

}

object MainIterate {
  case class Path(from: Int, to: Int, dist: Int)

  def parseVertex = (line: String) => { val v = line.toInt; Path(v, v, 0) }

  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
  }

  def formatOutput = (path: Path) => "%d|%d|%d".format(path.from, path.to, path.dist)

  def joinPaths = (p1: Path, p2: Path) => (p1, p2) match {
    case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2) 
  }

  def main(args: Array[String]) {

    val vertices = DataSource("file:///home/aljoscha/transclos-vertices", DelimitedInputFormat(parseVertex))
    val edges = DataSource("file:///home/aljoscha/transclos-edges", DelimitedInputFormat(parseEdge))

    def createClosure(paths: DataSet[Path]) = {

      val allNewPaths = paths join edges where { p => p.to } isEqualTo { p => p.from } map joinPaths
      
      val shortestPaths = allNewPaths cogroup paths where { p => (p.from, p.to) } isEqualTo { p => (p.from, p.to) } map {
        (newPaths, oldPaths) => (newPaths ++ oldPaths) minBy { _.dist }
      }

//      val shortestPaths = allNewPaths union paths groupBy { p => (p.from, p.to) } reduceGroup { _.minBy { _.dist } }

      shortestPaths
    }
    

    val transitiveClosure = vertices.iterate(5, createClosure)
    
    val sink = transitiveClosure.write("file:///home/aljoscha/transclos-output", DelimitedOutputFormat(formatOutput))


//    vertices.avgBytesPerRecord(16)
//    edges.avgBytesPerRecord(16)
//    sink.avgBytesPerRecord(16)
    
    val plan = new ScalaPlan(Seq(sink), "SCALA TRANSITIVE CLOSURE")
    GlobalSchemaPrinter.printSchema(plan)
    
    val ex = new LocalExecutor()
    ex.start()
    ex.executePlan(plan)
    ex.stop()
    
    System.exit(0)
  }

}

object MainWorksetIterate {
  case class Path(from: Int, to: Int, dist: Int)

  def parseVertex = (line: String) => { val v = line.toInt; Path(v, v, 0) }

  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
  }

  def formatOutput = (path: Path) => "%d|%d|%d".format(path.from, path.to, path.dist)

  def joinPaths = (p1: Path, p2: Path) => (p1, p2) match {
    case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2) 
  }
  
  def selectShortestDistance = (dist1: Iterator[Path], dist2: Iterator[Path]) => (dist1 ++ dist2) minBy { _.dist }

  def excludeKnownPaths = (x: Iterator[Path], c: Iterator[Path]) => if (c.isEmpty) x else Iterator.empty

  def main(args: Array[String]) {

    val vertices = DataSource("file:///home/aljoscha/transclos-vertices", DelimitedInputFormat(parseVertex))
    val edges = DataSource("file:///home/aljoscha/transclos-edges", DelimitedInputFormat(parseEdge))

    def createClosure = (c: DataSet[Path], x: DataSet[Path]) => {

      val cNewPaths = x join c where { p => p.to } isEqualTo { p => p.from } map joinPaths
      val c1 = cNewPaths cogroup c where { p => (p.from, p.to) } isEqualTo { p => (p.from, p.to) } map selectShortestDistance

      val xNewPaths = x join x where { p => p.to } isEqualTo { p => p.from } map joinPaths
      val x1 = xNewPaths cogroup c1 where { p => (p.from, p.to) } isEqualTo { p => (p.from, p.to) } flatMap excludeKnownPaths

      (c1, x1)
    }
    

    val transitiveClosure = vertices.iterateWithWorkset(edges, { p => (p.from, p.to) }, createClosure, 10)
//    vertices iterateWithWorkset edges withKey { p => (p.from, p.to) } using createClosure
    
    val sink = transitiveClosure.write("file:///home/aljoscha/transclos-output-workset", DelimitedOutputFormat(formatOutput))


//    vertices.avgBytesPerRecord(16)
//    edges.avgBytesPerRecord(16)
//    sink.avgBytesPerRecord(16)
    
    val plan = new ScalaPlan(Seq(sink), "SCALA TRANSITIVE CLOSURE")
    GlobalSchemaPrinter.printSchema(plan)
    
    val ex = new LocalExecutor()
    ex.start()
    ex.executePlan(plan)
    ex.stop()
    
    System.exit(0)
  }

}

object ConnectedComponents {
  def parseVertex = (line: String) => { val v = line.toInt; v -> v }

  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => from.toInt -> to.toInt
  }

  def formatOutput = (vertex: Int, component: Int) => "%d|%d".format(vertex, component)

  def main(args: Array[String]) {
    val vertices = DataSource("file:///home/aljoscha/transclos-vertices", DelimitedInputFormat(parseVertex))
    val directedEdges = DataSource("file:///home/aljoscha/transclos-edges", DelimitedInputFormat(parseEdge))

    val undirectedEdges = directedEdges flatMap { case (from, to) => Seq(from -> to, to -> from) }

    def propagateComponent = (s: DataSet[(Int, Int)], ws: DataSet[(Int, Int)]) => {

      val allNeighbors = ws join undirectedEdges where { case (v, _) => v } isEqualTo { case (from, _) => from } map { (w, e) => e._2 -> w._2 }
      val minNeighbors = allNeighbors groupBy { case (to, _) => to } reduceGroup { cs => cs minBy { _._2 } }

      // updated solution elements == new workset
      val s1 = minNeighbors join s where { _._1 } isEqualTo { _._1 } flatMap { (n, s) =>
        (n, s) match {
          case ((v, cNew), (_, cOld)) if cNew < cOld => Some((v, cNew))
          case _ => None
        }
      }
      
      s1.left preserves({_._1}, { _._1 })
      s1.right preserves({_._1},{ _._1 })
      
      allNeighbors.left preserves({_._2}, { _._2 })
      allNeighbors.right preserves({_._2}, { _._1 })

      (s1, s1)
    }

    val components = vertices.iterateWithWorkset(vertices, { _._1 }, propagateComponent, 10)

    val sink = components.write("file:///home/aljoscha/connected-components-output", DelimitedOutputFormat(formatOutput.tupled))
    val plan = new ScalaPlan(Seq(sink), "SCALA TRANSITIVE CLOSURE")
    GlobalSchemaPrinter.printSchema(plan)

    val ex = new LocalExecutor()
    ex.start()
    ex.executePlan(plan)
    ex.stop()

    System.exit(0)


  }
}