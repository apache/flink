package eu.stratosphere.sopremo.sdaa11.clustering.initial;

import java.util.Collection;

import eu.stratosphere.sopremo.sdaa11.clustering.Point;

public class BaseCluster extends HierarchicalCluster {

	public BaseCluster(final Point point, final String id) {
		super(id);
		this.point = point;
	}

	private final Point point;

	@Override
	public int size() {
		return 1;
	}

	@Override
	public Point getClustroid() {
		return this.point;
	}

	@Override
	public Point[] getPoints() {
		return new Point[] { this.point };
	}

	@Override
	public int getRadius() {
		return 0;
	}

	@Override
	public void makeFinal(final boolean makeFinal) {
	}

	@Override
	public boolean isFinal() {
		return true;
	}

	@Override
	public Collection<HierarchicalCluster> getChildren() {
		throw new UnsupportedOperationException(
				"A BaseCluster never has children");
	}

	@Override
	public boolean canBeFinal() {
		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "BaseCluster["+point+"]";
	}

}
