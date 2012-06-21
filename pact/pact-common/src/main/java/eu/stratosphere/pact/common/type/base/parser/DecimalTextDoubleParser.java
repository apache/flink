package eu.stratosphere.pact.common.type.base.parser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * Parses a decimal text field into a PactDouble.
 * Only characters '1' to '0', '-', 'E', 'e' (for scientific notation), and a configurable decimal separator character (default is '.') are allowed.
 * Scientific notation can be disabled to speed-up parsing.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class DecimalTextDoubleParser  implements FieldParser<PactDouble> {

	public static final String DECIMAL_SEPARATOR = "decimaltext.doubleparser.decimal.separator";
	public static final String SCIENTIFIC_NOTATION_ENABLED = "decimaltext.doubleparser.scientificnotation.enabled";
	
	private char decimalSep = '.';
	private boolean expNotationEnabled = true;
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration config) { 
		
		String decimalSepStr = config.getString(DECIMAL_SEPARATOR, ".");
		if(decimalSepStr.length() != 1) {
			throw new IllegalArgumentException("DecimalTextDoubleParser: Decimal Separator must be exactly one character.");
		}
		decimalSep = decimalSepStr.charAt(0);
		
		expNotationEnabled = config.getBoolean(SCIENTIFIC_NOTATION_ENABLED, true);
		
	}
	

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#parseField(byte[], int, int, char, eu.stratosphere.pact.common.type.Value)
	 */
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delim, PactDouble field) {
		
		if(this.expNotationEnabled) {
			return parseWithExpNotationEnabled(bytes, startPos, limit, delim, field);
		} else {
			return parseWithExpNotationDisabled(bytes, startPos, limit, delim, field);
		}
		
	}
	
	private int parseWithExpNotationDisabled(byte[] bytes, int startPos, int length, char delim, PactDouble field) {
		long val = 0;
		long fraction = -1;
		int i;
		boolean neg = false;
		
		if(bytes[startPos] == '-') {
			neg = true;
			startPos++;
		}
		
		for(i=startPos; i < length; i++) {

			if(bytes[i] == delim) {
				break;
			}
			if(bytes[i] == this.decimalSep) {
				fraction = 0;
				continue;
			}
			if(bytes[i] < 48 || bytes[i] > 57) {
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;
			if(fraction >= 0) {
				fraction++;
			}
		}
		field.setValue(((double)val)/Math.pow(10, fraction == -1 ? 0 : fraction)*(neg ? -1 : 1));
		return i == length ? length : i+1;
	}
	
	private int parseWithExpNotationEnabled(byte[] bytes, int startPos, int length, char delim, PactDouble field) {
		double val = 0;
		long fraction = -1;
		long expVal = -1;
		int i;
		boolean neg = false;
		boolean negExp = false;
		
		if(bytes[startPos] == '-') {
			neg = true;
			startPos++;
		}
		
		for(i=startPos; i < length; i++) {
			if(bytes[i] == delim) {
				break;
			}
			if(bytes[i] == this.decimalSep) {
				if(expVal != -1) {
					// no fraction in exponent
					return -1;
				}
				// start fraction count
				fraction = 0;
				continue;
			}
			if(bytes[i] == 'E' || bytes[i] == 'e') {
				if(expVal != -1) {
					// already in exponent mode
					return -1;
				}
				// start exponent mode
				expVal = 0;
				if(i < length && bytes[i+1] == '-') {
					negExp = true;
					i++;
				}
				continue;
			}
			if(bytes[i] < 48 || bytes[i] > 57) {
				// invalid char found
				return -1;
			}
			if(expVal == -1) {
				val *= 10;
				val += bytes[i] - 48;
				if(fraction >= 0) {
					fraction++;
				}
			} else {
				expVal *= 10;
				expVal += bytes[i] - 48;
			}
		}
		// return val
		if(expVal != -1) {
			fraction -= (expVal * (negExp ? -1 : 1));
		}
		field.setValue(((double)val)/Math.pow(10, fraction == -1 ? 0 : fraction)*(neg ? -1 : 1));
		return i == length ? length : i+1;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.type.base.parser.FieldParser#getValue()
	 */
	@Override
	public PactDouble getValue() {
		return new PactDouble();
	}

}
