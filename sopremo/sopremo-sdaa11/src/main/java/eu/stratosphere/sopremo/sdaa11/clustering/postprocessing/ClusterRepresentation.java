package eu.stratosphere.sopremo.sdaa11.clustering.postprocessing;

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.util.Ranking;
import eu.stratosphere.sopremo.sdaa11.util.ReverseRanking;

public class ClusterRepresentation {

	public static final int STABLE_FLAG = 0;
	public static final int RECLUSTER_FLAG = 1;
	public static final int SPLIT_FLAG = 2;

	private final Ranking<Point> nearestPoints;
	private final Ranking<Point> furthestPoints;
	private Point clustroid;
	private int size;
	private final String id;

	public ClusterRepresentation(final String id, final Point clustroid,
			final int representationCount) {
		this.id = id;
		this.clustroid = clustroid;
		this.size = 1;
		this.nearestPoints = new Ranking<Point>(representationCount);
		this.furthestPoints = new ReverseRanking<Point>(representationCount);
	}

	public void add(final Point point) {
		point.setRowsum(this.estimateRowsum(point));
		this.clustroid.addToRowsum(point);
		this.updateRepresentantsWith(point);
		this.size++;
	}

	private int estimateRowsum(final Point point) {
		if (this.clustroid == null)
			return 0;
		final int squaredDistance = point.getSquaredDistance(this.clustroid);
		return this.clustroid.getRowsum() + this.size * squaredDistance;
	}

	private void updateRepresentantsWith(final Point point) {
		if (this.clustroid == null)
			this.clustroid = point;
		else if (point.getRowsum() < this.clustroid.getRowsum()) {
			final Point oldClustroid = this.clustroid;
			this.clustroid = point;
			this.updateNearestOrFurthestPointsWith(oldClustroid);
		} else
			this.updateNearestOrFurthestPointsWith(point);
	}

	private void updateNearestOrFurthestPointsWith(final Point point) {
		final Ranking.Item<Point> displacedItem = this.nearestPoints.insert(
				point, point.getRowsum());
		if (displacedItem != null)
			this.furthestPoints.insert(displacedItem);
	}

	public double getAverageRadius() {
		return Math.sqrt((double) this.clustroid.getRowsum() / this.size);
	}

	public Point[] findSplittingClustroids() {

		int highestDistance = -1;
		Point point1 = null;
		Point point2 = null;

		final Set<Point> possiblePoints = new HashSet<Point>(
				this.furthestPoints.toList());
		if (possiblePoints.size() < 2)
			possiblePoints.addAll(this.nearestPoints.toList());
		if (possiblePoints.size() < 2)
			possiblePoints.add(this.clustroid);
		if (possiblePoints.size() < 2)
			throw new IllegalStateException(
					"Not enough points available for splitting!"
							+ possiblePoints.size() + " vs " + this.size);
		final Set<Point> otherPoints = new HashSet<Point>(possiblePoints);
		for (final Point point : possiblePoints) {
			otherPoints.remove(point);
			for (final Point otherPoint : otherPoints) {
				final int distance = point.getDistance(otherPoint);
				if (distance > highestDistance) {
					highestDistance = distance;
					point1 = point;
					point2 = otherPoint;
				}
			}
		}

		return new Point[] { point1, point2 };
	}

	public String getId() {
		return this.id;
	}

	public Point getClustroid() {
		return this.clustroid;
	}

	public int size() {
		return this.size;
	}

}
