package eu.stratosphere.sopremo;

public interface DataStream {
	public Operator.Output getSource();
}
