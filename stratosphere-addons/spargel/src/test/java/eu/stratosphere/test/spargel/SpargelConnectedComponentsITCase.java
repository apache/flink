package eu.stratosphere.test.spargel;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.spargel.java.examples.connectedcomponents.SpargelConnectedComponents;
import eu.stratosphere.test.iterative.ConnectedComponentsITCase;

public class SpargelConnectedComponentsITCase extends ConnectedComponentsITCase {

	public SpargelConnectedComponentsITCase(Configuration config) {
		super(config);
	}

	@Override
	protected Plan getTestJob() {
		int dop = config.getInteger("ConnectedComponents#NumSubtasks", 1);
		int maxIterations = config.getInteger("ConnectedComponents#NumIterations", 1);
		String[] params = { String.valueOf(dop) , verticesPath, edgesPath, resultPath, String.valueOf(maxIterations) };

		SpargelConnectedComponents cc = new SpargelConnectedComponents();
		return cc.getPlan(params);
	}
}