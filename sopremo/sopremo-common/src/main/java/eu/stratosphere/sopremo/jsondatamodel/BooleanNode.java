package eu.stratosphere.sopremo.jsondatamodel;

public class BooleanNode extends JsonNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9185727528566635632L;

	public final static BooleanNode TRUE = new BooleanNode();

	public final static BooleanNode FALSE = new BooleanNode();

	private BooleanNode() {
	};

	public static BooleanNode valueOf(boolean b) {
		return b ? TRUE : FALSE;
	}

	public boolean getBooleanValue() {
		return (this == TRUE);
	}

	public String toString(){
		return this == TRUE ? "true" : "false";
	}
	
	// TODO implement hashCode()

	public boolean equals(Object o) {
		return (this == o);
	}
}
