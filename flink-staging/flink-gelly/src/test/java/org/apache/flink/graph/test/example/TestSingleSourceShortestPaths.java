package org.apache.flink.graph.test.example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.graph.example.SingleSourceShortestPathsExample;
import org.apache.flink.graph.example.utils.SingleSourceShortestPathsData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

@RunWith(Parameterized.class)
public class TestSingleSourceShortestPaths extends MultipleProgramsTestBase {

    private String verticesPath;

    private String edgesPath;

    private String resultPath;

    private String expected;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    public TestSingleSourceShortestPaths(ExecutionMode mode) {
        super(mode);
    }

    @Before
    public void before() throws Exception {
        resultPath = tempFolder.newFile().toURI().toString();
        File verticesFile = tempFolder.newFile();
        Files.write(SingleSourceShortestPathsData.VERTICES, verticesFile, Charsets.UTF_8);

        File edgesFile = tempFolder.newFile();
        Files.write(SingleSourceShortestPathsData.EDGES, edgesFile, Charsets.UTF_8);

        verticesPath = verticesFile.toURI().toString();
        edgesPath = edgesFile.toURI().toString();
    }

    @Test
    public void testSSSPExample() throws Exception {
        SingleSourceShortestPathsExample.main(new String[]{SingleSourceShortestPathsData.SRC_VERTEX_ID + "",
                verticesPath, edgesPath, resultPath, SingleSourceShortestPathsData.NUM_VERTICES + ""});
        expected = SingleSourceShortestPathsData.RESULTED_SINGLE_SOURCE_SHORTEST_PATHS;
    }

    @After
    public void after() throws Exception {
        compareResultsByLinesInMemory(expected, resultPath);
    }
}
