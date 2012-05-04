package eu.stratosphere.sopremo.sdaa11.clustering.main;

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.sopremo.sdaa11.JsonSerializable;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.util.Ranking;
import eu.stratosphere.sopremo.sdaa11.util.ReverseRanking;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class ClusterRepresentation implements JsonSerializable {

	public static final int STABLE_FLAG = 0;
	public static final int RECLUSTER_FLAG = 1;
	public static final int SPLIT_FLAG = 2;

	private Ranking<Point> nearestPoints;
	private Ranking<Point> furthestPoints;
	private Point clustroid;
	private int size;
	private String id;
	private int representationCount;

	public ClusterRepresentation(final String id, final Point clustroid,
			final int representationCount) {
		this.id = id;
		this.clustroid = clustroid;
		this.size = 1;
		this.representationCount = representationCount;
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

	public double getRadius() {
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

	@Override
	public IJsonNode write(final IJsonNode node) {
		ObjectNode objectNode;
		if (node == null || !(node instanceof ObjectNode))
			objectNode = new ObjectNode();
		else
			objectNode = (ObjectNode) node;

		objectNode.put("id", new TextNode(this.id));
		objectNode.put("representationCount", new IntNode(this.representationCount));
		objectNode.put("size", new IntNode(this.size));
		objectNode.put("clustroid", clustroid.write(null));
		
		final ArrayNode nearestPointsNode = new ArrayNode();
		for (final Point point : this.furthestPoints)
			nearestPointsNode.add(point.write(null));
		objectNode.put("nearestPoints", nearestPointsNode);
		
		final ArrayNode furthestPointsNode = new ArrayNode();
		for (final Point point : this.furthestPoints)
			furthestPointsNode.add(point.write(null));
		objectNode.put("furthestPoints", furthestPointsNode);

		return objectNode;
	}

	@Override
	public void read(final IJsonNode node) {
		if (node == null || !(node instanceof ObjectNode))
			throw new IllegalArgumentException("Illegal point node: " + node);
		final ObjectNode objectNode = (ObjectNode) node;
		
		this.id = ((TextNode) objectNode.get("id")).getJavaValue();
		this.size = ((IntNode) objectNode.get("size")).getIntValue();
		this.representationCount = ((IntNode) objectNode.get("size")).getIntValue();
		
		this.clustroid = new Point();
		this.clustroid.read(objectNode.get("clustroid"));
		
		nearestPoints = new ReverseRanking<Point>(representationCount);
		ArrayNode pointsNode = (ArrayNode) objectNode.get("nearestPoints");
		for (final IJsonNode pointNode : pointsNode) {
			Point point = new Point();
			point.read(pointNode);
			nearestPoints.insert(point, point.getRowsum());
		}
		
		furthestPoints = new ReverseRanking<Point>(representationCount);
		pointsNode = (ArrayNode) objectNode.get("furthestPoints");
		for (final IJsonNode pointNode : pointsNode) {
			Point point = new Point();
			point.read(pointNode);
			furthestPoints.insert(point, point.getRowsum());
		}
		
	}

}
