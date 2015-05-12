package org.apache.flink.graph.test.example;

import java.io.File;

import org.apache.flink.graph.example.AffinityPropogationExample;
import org.apache.flink.graph.example.utils.AffinityPropogationData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

@RunWith(Parameterized.class)
public class AffinityPropogationITCase extends MultipleProgramsTestBase {
	private String edgesPath;

	private String resultPath;

	private String expected;
	
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	
	public AffinityPropogationITCase(TestExecutionMode mode) {
		super(mode);
	}
	
	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < AffinityPropogationData.ToyProblemSimlarities.length; i++){
			sb.append(AffinityPropogationData.ToyProblemSimlarities[i][0] + " "
					+ AffinityPropogationData.ToyProblemSimlarities[i][1] + " "
					+ AffinityPropogationData.ToyProblemSimlarities[i][2] + "\n");
		}
		File edgesFile = tempFolder.newFile();
		Files.write(sb.toString(), edgesFile, Charsets.UTF_8);
		edgesPath = edgesFile.toURI().toString();
		
	}

	@Test
	public void testAffinityPropogationExample() throws Exception {
		AffinityPropogationExample.main(new String[]{edgesPath, 
				resultPath, 
				String.valueOf(AffinityPropogationData.MAX_ITERATIONS),
				String.valueOf(AffinityPropogationData.LAMBDA)});
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < AffinityPropogationData.VERTICES_WITH_CLUSTER_ID.length; i++){
			sb.append(AffinityPropogationData.VERTICES_WITH_CLUSTER_ID[i][0] + " "
					+AffinityPropogationData.VERTICES_WITH_CLUSTER_ID[i][1] + "\n");
		}
		expected = sb.toString();
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}

}
