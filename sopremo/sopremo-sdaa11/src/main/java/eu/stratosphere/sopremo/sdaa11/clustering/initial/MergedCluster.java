package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;

public class MergedCluster extends HierarchicalCluster {

	private HierarchicalCluster child1, child2;
	private Point clustroid;
	private Point[] points;
	private int[] oldRowsums;
	private int radius;
	private final int size;
	private boolean isFinal;

	public MergedCluster(final HierarchicalCluster child1,
			final HierarchicalCluster child2, final String id) {
		super(id);
		this.child1 = child1;
		this.child2 = child2;
		this.size = child1.size() + child2.size();

		if (this.canBeFinal()) {
			this.createPoints();
			this.updateRowsums();
			this.chooseClustroid();
			this.calculateRadius();
		} else
			this.clustroid = child1.getClustroid();
	}

	private void createPoints() {
		this.points = new Point[this.size];
		System.arraycopy(this.child1.getPoints(), 0, this.points, 0,
				this.child1.size());
		System.arraycopy(this.child2.getPoints(), 0, this.points,
				this.child1.size(), this.child2.size());
	}

	private void calculateRadius() {
		this.radius = (int) Math.sqrt(this.clustroid.getRowsum()
				/ (this.size() - 1));
	}

	private void updateRowsums() {
		this.storeCurrentRowsums();
		this.updateRowsums(this.child1, this.child2);
		this.updateRowsums(this.child2, this.child1);
	}

	private void storeCurrentRowsums() {
		this.oldRowsums = new int[this.size];
		for (int i = 0; i < this.size; i++)
			this.oldRowsums[i] = this.points[i].getRowsum();
	}

	private void updateRowsums(final HierarchicalCluster updatees,
			final HierarchicalCluster updaters) {
		for (final Point updatee : updatees.getPoints())
			for (final Point updater : updaters.getPoints())
				updatee.addToRowsum(updater);

	}

	private void chooseClustroid() {
		this.clustroid = this.findPointWithLowestRowsum(
				this.child1.getPoints(), null, -1);
		this.clustroid = this.findPointWithLowestRowsum(
				this.child2.getPoints(), this.clustroid,
				this.clustroid.getRowsum());
	}

	private Point findPointWithLowestRowsum(final Point[] points,
			Point candidatePoint, int candidatePointRowsum) {
		for (final Point point : points) {
			final int rowsum = point.getRowsum();
			if (candidatePoint == null || rowsum < candidatePointRowsum) {
				candidatePointRowsum = rowsum;
				candidatePoint = point;
			}
		}
		return candidatePoint;
	}

	@Override
	public int size() {
		return this.size;
	}

	@Override
	public Point getClustroid() {
		return this.clustroid;
	}

	@Override
	public Point[] getPoints() {
		return this.points;
	}

	@Override
	public int getRadius() {
		return this.radius;
	}

	@Override
	public void makeFinal(final boolean makeFinal) {
		if (makeFinal) {
			if (!this.canBeFinal())
				throw new IllegalStateException("Cluster cannot be made final");
			this.child1 = this.child2 = null;
		} else {
			if (this.canBeFinal())
				this.rollbackRowsums();
			this.points = null;
		}
		this.oldRowsums = null;
		this.isFinal = makeFinal;
	}

	private void rollbackRowsums() {
		for (int i = 0; i < this.size; i++)
			this.points[i].setRowsum(this.oldRowsums[i]);
	}

	@Override
	public boolean isFinal() {
		return this.isFinal;
	}

	@Override
	public Collection<HierarchicalCluster> getChildren() {
		if (this.isFinal())
			throw new IllegalStateException("Cluster has no children!");
		return Arrays.asList(this.child1, this.child2);
	}

	@Override
	public boolean canBeFinal() {
		return this.child1.isFinal() && this.child2.isFinal();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MergedCluster[size=" + this.size + "]";
	}

}
