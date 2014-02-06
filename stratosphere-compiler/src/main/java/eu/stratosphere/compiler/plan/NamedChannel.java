package eu.stratosphere.compiler.plan;

import eu.stratosphere.compiler.dag.TempMode;

public class NamedChannel extends Channel {

	private final String name;

	/**
	 * Initializes NamedChannel.
	 * 
	 * @param sourceNode
	 */
	public NamedChannel(String name, PlanNode sourceNode) {
		super(sourceNode);
		this.name = name;
	}

	public NamedChannel(String name, PlanNode sourceNode, TempMode tempMode) {
		super(sourceNode, tempMode);
		this.name = name;
	}

	public String getName() {
		return this.name;
	}
}
