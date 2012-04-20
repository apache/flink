package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.Collection;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;

public abstract class HierarchicalCluster {
	
	private String id;
	
	public HierarchicalCluster(String id) {
		this.id = id;
	}

	/** Returns the number of points in the cluster. */
	public abstract int size();
	
	/** Returns the clustroid of the cluster. */
	public abstract Point getClustroid();
	
	/** Returns all points contained in this cluster. */
	public abstract Point[] getPoints();
	
	/** Returns the diameter of this cluster. */
	public abstract int getRadius();
	
	public abstract boolean canBeFinal();
	
	public abstract boolean isFinal();
	
	/**
	 * Only needs to work in case {@link #isFinal()} is <code>true</code>.
	 */
	public abstract Collection<HierarchicalCluster> getChildren();
	
	/**
	 * Optimization. Tell the cluster that its children
	 * will not be needed any longer, so it frees them for
	 * garbage collection.
	 */
	public abstract void makeFinal(boolean makeFinal);
	
	public String getId() {
		return id;
	}

}
