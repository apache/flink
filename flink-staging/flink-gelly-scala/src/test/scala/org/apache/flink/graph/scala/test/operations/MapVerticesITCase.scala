package org.apache.flink.graph.scala.test.operations

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.Vertex
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.test.TestGraphUtils
import org.apache.flink.test.util.{AbstractMultipleProgramsTestBase, MultipleProgramsTestBase}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class MapVerticesITCase(mode: AbstractMultipleProgramsTestBase.TestExecutionMode) extends MultipleProgramsTestBase(mode) {

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
        val mappedVertices: DataSet[Vertex[Long, Long]] = graph.mapVertices(new AddOneMapper).getVertices
        // Convert Edge into a Scala Tuple for writing to csv @TODO: do this implicitly?
        mappedVertices.map(vertex => (vertex.getId, vertex.getValue)).writeAsCsv(resultPath)
        env.execute

        expectedResult = "1,2\n" +
            "2,3\n" +
            "3,4\n" +
            "4,5\n" +
            "5,6\n";
    }

    @Test
    @throws(classOf[Exception])
    def testWithSameValueSugar {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
        val mappedVertices: DataSet[Vertex[Long, Long]] = graph.mapVertices(vertex => vertex.getValue + 1).getVertices
        // Convert Edge into a Scala Tuple for writing to csv @TODO: do this implicitly?
        mappedVertices.map(vertex => (vertex.getId, vertex.getValue)).writeAsCsv(resultPath)
        env.execute

        expectedResult = "1,2\n" +
            "2,3\n" +
            "3,4\n" +
            "4,5\n" +
            "5,6\n";
    }

    final class AddOneMapper extends MapFunction[Vertex[Long, Long], Long] {
        @throws(classOf[Exception])
        def map(vertex: Vertex[Long, Long]): Long = {
            vertex.getValue + 1
        }
    }

}