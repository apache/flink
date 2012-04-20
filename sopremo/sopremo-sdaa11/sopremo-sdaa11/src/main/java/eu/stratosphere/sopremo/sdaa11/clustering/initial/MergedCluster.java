package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;

public class MergedCluster extends HierarchicalCluster {

	private HierarchicalCluster child1, child2;
	private Point clustroid;
	private Point[] points;
	private int[] oldRowsums;
	private int radius, size;
	private boolean isFinal;

	public MergedCluster(HierarchicalCluster child1, HierarchicalCluster child2, String id) {
		super(id);
		this.child1 = child1;
		this.child2 = child2;
		this.size = child1.size() + child2.size();

		if (canBeFinal()) {
			createPoints();
			updateRowsums();
			chooseClustroid();
			calculateRadius();
		} else {
			clustroid = child1.getClustroid();
		}
	}
	
	private void createPoints() {
		points = new Point[size];
		System.arraycopy(child1.getPoints(), 0, points, 0, child1.size());
		System.arraycopy(child2.getPoints(), 0, points, child1.size(), child2.size());
	}

	private void calculateRadius() {
		radius = (int) Math.sqrt(clustroid.getRowsum() / (size() - 1));
	}

	private void updateRowsums() {
		storeCurrentRowsums();
		updateRowsums(child1, child2);
		updateRowsums(child2, child1);
	}

	private void storeCurrentRowsums() {
		oldRowsums = new int[size];
		for (int i = 0; i < size; i++) {
			oldRowsums[i] = points[i].getRowsum();
		}
	}

	private void updateRowsums(HierarchicalCluster updatees,
			HierarchicalCluster updaters) {
		for (Point updatee : updatees.getPoints()) {
			for (Point updater : updaters.getPoints()) {
				updatee.addToRowsum(updater);
			}
		}
		
	}

	private void chooseClustroid() {
		clustroid = findPointWithLowestRowsum(child1.getPoints(), null, -1);
		clustroid = findPointWithLowestRowsum(child2.getPoints(), clustroid, clustroid.getRowsum());
	}
	
	private Point findPointWithLowestRowsum(Point[] points,
			Point candidatePoint, int candidatePointRowsum) {
		for (Point point : points) {
			int rowsum = point.getRowsum();
			if (candidatePoint == null || rowsum < candidatePointRowsum) {
				candidatePointRowsum = rowsum;
				candidatePoint = point;
			}
		}
		return candidatePoint;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public Point getClustroid() {
		return clustroid;
	}

	@Override
	public Point[] getPoints() {
		return points;
	}

	@Override
	public int getRadius() {
		return radius;
	}

	@Override
	public void makeFinal(boolean makeFinal) {
		if (makeFinal) {
			if (!canBeFinal()) {
				throw new IllegalStateException("Cluster cannot be made final");
			}
			child1 = child2 = null;
		} else {
			if (canBeFinal()) {
				rollbackRowsums();
			}
			points = null;
		}
		oldRowsums = null;
		isFinal = makeFinal;
	}

	private void rollbackRowsums() {
		for (int i = 0; i < size; i++) {
			points[i].setRowsum(oldRowsums[i]);
		}
	}

	@Override
	public boolean isFinal() {
		return isFinal;
	}

	@Override
	public Collection<HierarchicalCluster> getChildren() {
		if (isFinal()) {
			throw new IllegalStateException("Cluster has no children!");
		}
		return Arrays.asList(child1, child2);
	}

	@Override
	public boolean canBeFinal() {
		return child1.isFinal() && child2.isFinal();
	}

}
