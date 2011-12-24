package eu.stratosphere.pact.common.type.base.parser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Converts a variable length field of a byte array into a {@link PactString}. 
 * The field is terminated either by end of array or field delimiter character.
 * A string encapsulator can be configured. If configured, the encapsulator must be present in the input but 
 * will not be included in the PactString.
 * 
 * @author Fabian Hueske
 * 
 * @see PactString
 *
 */
public class VarLengthStringParser implements FieldParser<PactString> {

	public static final String STRING_ENCAPSULATOR = "varlength.string.parser.encapsulator";
	
	private char encapsulator;
	private boolean encapsulated;
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	public void configure(Configuration config) {
		String encapStr = config.getString(STRING_ENCAPSULATOR, "#*+~#**");
		if(encapStr.equals("#*+~#**")) {
			encapsulated = false;
		} else {
			encapsulated = true;
			if(encapStr.length() != 1) {
				throw new IllegalArgumentException("FixedLengthStringParser: String encapsulator must be exactly one character.");
			}
			encapsulator = encapStr.charAt(0);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#parseField(byte[], int, int, char, eu.stratosphere.pact.common.type.Value)
	 */
	@Override
	public int parseField(byte[] bytes, int startPos, int length, char delim, PactString field) {
	
		int i;
		
		if(encapsulated) {
			if(bytes[startPos] != encapsulator) {
				return -1;
			}
			
			// encaps string
			for(i=startPos+1; i<length; i++) {
				if(bytes[i] == encapsulator) {
					if(i+1 == length || bytes[i+1] == delim) {
						break;
					}
				}
			}
			if(i < length && bytes[i] == encapsulator) {
				field.setValueAscii(bytes, startPos+1, i-startPos-1);
				return (i+1 == length ? length : i+2);
			} else {
				return -1;
			}
		} else {
			// non-encaps string
			for(i=startPos; i<length; i++) {
				if(bytes[i] == delim) {
					break;
				}
			}
			field.setValueAscii(bytes, startPos, i-startPos);
			return (i == length ? length : i+1);
		}
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#getValue()
	 */
	@Override
	public PactString getValue() {
		return new PactString();
	}
	
}