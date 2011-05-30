package eu.stratosphere.sopremo;

/**
 * 
 * @author Arvid Heise
 *
 */
public interface DataStream {
	public Operator.Output getSource();
}
