package eu.stratosphere.sopremo.jsondatamodel;

import java.io.Serializable;

public abstract class JsonNode implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7164528435336585193L;

	@Override
	public abstract boolean equals(Object o);
}
