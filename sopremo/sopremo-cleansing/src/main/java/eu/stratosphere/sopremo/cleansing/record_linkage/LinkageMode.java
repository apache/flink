package eu.stratosphere.sopremo.cleansing.record_linkage;

public enum LinkageMode {
	LINKS_ONLY(false, ClosureMode.LINKS),
	TRANSITIVE_LINKS(false, ClosureMode.LINKS),
	DUPLICATE_CLUSTERS_FLAT(false, ClosureMode.CLUSTER),
	DUPLICATE_CLUSTERS_PROVENANCE(false, ClosureMode.CLUSTER_PROVENANCE),
	ALL_CLUSTERS_FLAT(true, ClosureMode.CLUSTER),
	ALL_CLUSTERS_PROVENANCE(true, ClosureMode.CLUSTER_PROVENANCE);

	private final boolean withSingles;

	private ClosureMode closureMode;

	private LinkageMode(boolean addSingles, ClosureMode closureMode) {
		this.withSingles = addSingles;
		this.closureMode = closureMode;
	}

	public ClosureMode getClosureMode() {
		return this.closureMode;
	}

	public boolean isWithSingles() {
		return this.withSingles;
	}

}
