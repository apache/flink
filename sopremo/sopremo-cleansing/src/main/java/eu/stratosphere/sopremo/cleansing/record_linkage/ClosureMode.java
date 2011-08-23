package eu.stratosphere.sopremo.cleansing.record_linkage;

import eu.stratosphere.pact.common.stub.Stub;

public enum ClosureMode {
	LINKS(false, false), CLUSTER(true, false), CLUSTER_PROVENANCE(true, true);

	private boolean cluster, provenance;

	private ClosureMode(boolean cluster, boolean provenance) {
		this.cluster = cluster;
		this.provenance = provenance;
	}

	public boolean isCluster() {
		return cluster;
	}

	public boolean isProvenance() {
		return provenance;
	}

}