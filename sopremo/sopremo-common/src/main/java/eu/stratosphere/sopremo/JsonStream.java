package eu.stratosphere.sopremo;

/**
 * A stream of json objects coming from one {@link Operator} and going into the input of another.
 * 
 * @author Arvid Heise
 */
public interface JsonStream {
	/**
	 * Returns the unambiguous source of the stream.
	 * 
	 * @return the soruce of the stream
	 */
	public Operator<?>.Output getSource();
}
