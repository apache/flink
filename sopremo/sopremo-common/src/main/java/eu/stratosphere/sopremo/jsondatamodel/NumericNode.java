package eu.stratosphere.sopremo.jsondatamodel;



public abstract class NumericNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 677420673530449343L;
	
	public abstract Double getValueAsDouble();

	//TODO check subclasses
	public boolean isFloatingPointNumber() {
		return false;
	}

	
	//TODO check subclasses
	public boolean isIntegralNumber() {
		return false;
	}

	

}
