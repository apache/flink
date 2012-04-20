package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.Collection;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;



public class BaseCluster extends HierarchicalCluster {
	
	public BaseCluster(Point point, String id) {
		super(id);
		this.point = point;
	}
	
	private Point point;

	@Override
	public int size() {
		return 1;
	}

	@Override
	public Point getClustroid() {
		return point;
	}

	@Override
	public Point[] getPoints() {
		return new Point[] { point };
	}

	@Override
	public int getRadius() {
		return 0;
	}

	@Override
	public void makeFinal(boolean makeFinal) {
	}

	@Override
	public boolean isFinal() {
		return true;
	}

	@Override
	public Collection<HierarchicalCluster> getChildren() {
		throw new UnsupportedOperationException("A BaseCluster never has children");
	}

	@Override
	public boolean canBeFinal() {
		return true;
	}

}
