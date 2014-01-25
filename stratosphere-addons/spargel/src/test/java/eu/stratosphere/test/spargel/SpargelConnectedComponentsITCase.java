package eu.stratosphere.test.spargel;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.spargel.java.examples.connectedcomponents.SpargelConnectedComponents;
import eu.stratosphere.test.iterative.nephele.ConnectedComponentsNepheleITCase;
import eu.stratosphere.test.util.TestBase2;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SpargelConnectedComponentsITCase extends TestBase2 {

	private static final long SEED = 0xBADC0FFEEBEEFL;

	private static final int NUM_VERTICES = 1000;

	private static final int NUM_EDGES = 10000;

	protected String verticesPath;

	protected String edgesPath;

	protected String resultPath;

	public SpargelConnectedComponentsITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", ConnectedComponentsNepheleITCase.getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", ConnectedComponentsNepheleITCase.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}

	@Override
	protected Plan getTestJob() {
		int dop = config.getInteger("ConnectedComponents#NumSubtasks", 1);
		int maxIterations = config.getInteger("ConnectedComponents#NumIterations", 1);
		String[] params = { String.valueOf(dop) , verticesPath, edgesPath, resultPath, String.valueOf(maxIterations) };

		SpargelConnectedComponents cc = new SpargelConnectedComponents();
		return cc.getPlan(params);
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			ConnectedComponentsNepheleITCase.checkOddEvenResult(reader);
		}
	}

	@Parameterized.Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("ConnectedComponents#NumSubtasks", 4);
		config1.setInteger("ConnectedComponents#NumIterations", 100);
		return toParameterList(config1);
	}
}