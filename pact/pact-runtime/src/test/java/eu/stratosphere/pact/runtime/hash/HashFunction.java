package  eu.stratosphere.pact.runtime.hash;

public interface HashFunction {

	public int hash(int code, int level);
	
}
