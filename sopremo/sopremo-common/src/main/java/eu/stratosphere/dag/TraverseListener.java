package eu.stratosphere.dag;

public interface TraverseListener<Node> {
	public void nodeTraversed(Node node);
}
