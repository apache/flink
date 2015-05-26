package org.apache.flink.graph.scala.test.operations


import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.Edge
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.test.TestGraphUtils
import org.apache.flink.test.util.{AbstractMultipleProgramsTestBase, MultipleProgramsTestBase}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class MapEdgesITCase(mode: AbstractMultipleProgramsTestBase.TestExecutionMode) extends MultipleProgramsTestBase(mode) {

    private var resultPath: String = null
    private var expectedResult: String = null

    var tempFolder: TemporaryFolder = new TemporaryFolder()

    @Rule
    def getFolder(): TemporaryFolder = {
        tempFolder;
    }

    @Before
    @throws(classOf[Exception])
    def before {
        resultPath = tempFolder.newFile.toURI.toString
    }

    @After
    @throws(classOf[Exception])
    def after {
        compareResultsByLinesInMemory(expectedResult, resultPath)
    }

    @Test
    @throws(classOf[Exception])
    def testWithSameValue {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
        val mappedEdges: DataSet[Edge[Long, Long]] = graph.mapEdges(new AddOneMapper).getEdges
        // Convert Edge into a Scala Tuple for writing to csv @TODO: do this implicitly?
        mappedEdges.map(edge => (edge.getSource, edge.getTarget, edge.getValue)).writeAsCsv(resultPath)
        env.execute
        expectedResult = "1,2,13\n" +
            "1,3,14\n" + "" +
            "2,3,24\n" +
            "3,4,35\n" +
            "3,5,36\n" +
            "4,5,46\n" +
            "5,1,52\n"
    }

    @Test
    @throws(classOf[Exception])
    def testWithSameValueSugar {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
        val mappedEdges: DataSet[Edge[Long, Long]] = graph.mapEdges(edge => edge.getValue + 1).getEdges
        // Convert Edge into a Scala Tuple for writing to csv @TODO: do this implicitly?
        mappedEdges.map(edge => (edge.getSource, edge.getTarget, edge.getValue)).writeAsCsv(resultPath)
        env.execute
        expectedResult = "1,2,13\n" +
            "1,3,14\n" + "" +
            "2,3,24\n" +
            "3,4,35\n" +
            "3,5,36\n" +
            "4,5,46\n" +
            "5,1,52\n"
    }

    final class AddOneMapper extends MapFunction[Edge[Long, Long], Long] {
        @throws(classOf[Exception])
        def map(edge: Edge[Long, Long]): Long = {
            edge.getValue + 1
        }
    }
}