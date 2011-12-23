package eu.stratosphere.pact.common.type.base.parser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Parses a decimal text field into a PactInteger.
 * Only characters '1' to '0' and '-' are allowed.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class DecimalTextIntParser  implements FieldParser<PactInteger> {

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration config) { }
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#parseField(byte[], int, int, char, eu.stratosphere.pact.common.type.Value)
	 */
	@Override
	public int parseField(byte[] bytes, int startPos, int length, char delim,
			PactInteger field) {
		
		int val = 0;
		boolean neg = false;
		
		if(bytes[startPos] == '-') {
			neg = true;
			startPos++;
		}
		
		for(int i=startPos; i < length; i++) {
			if(bytes[i] == delim) {
				field.setValue(val*(neg ? -1 : 1));
				return i+1;
			}
			if(bytes[i] < 48 || bytes[i] > 57) {
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;
		}
		field.setValue(val*(neg ? -1 : 1));
		return length;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#getValue()
	 */
	@Override
	public PactInteger getValue() {
		return new PactInteger();
	}

}
