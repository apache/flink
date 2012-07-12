package eu.stratosphere.pact.common.io;

import eu.stratosphere.pact.common.generic.io.InputFormat;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * The OutputSchemaProvider interface can be implemented by {@link InputFormat}.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public interface OutputSchemaProvider {

	/**
	 * Returns a sorted array with the field indexes that are set in the {@link PactRecord}s 
	 * emitted by the {@link InputFormat}.
	 * 
	 * @return a <b>sorted</b> array of all field indexes that are set on output {@link PactRecord}s.
	 */
	public int[] getOutputSchema();
	
}
