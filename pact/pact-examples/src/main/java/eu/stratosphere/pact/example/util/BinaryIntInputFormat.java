package eu.stratosphere.pact.example.util;


import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.FixedLengthInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class BinaryIntInputFormat extends FixedLengthInputFormat {


	public static final String PAYLOAD_SIZE_PARAMETER_KEY = "int.file.inputformat.payloadSize";
	
	private final PactInteger key = new PactInteger();
	
	private final PactString payLoad = new PactString();
	
	
	
	@Override
	public void configure(Configuration parameters) {
		
		parameters.setInteger(FixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 4);
		super.configure(parameters);
		
		int payLoadSize = (int)parameters.getLong(PAYLOAD_SIZE_PARAMETER_KEY, 0);
		if(payLoadSize > 0) {
			char[] payLoadC = new char[payLoadSize/2];
			for(int i=0;i<payLoadC.length;i++) {
				payLoadC[i] = '.';
			}
			this.payLoad.setValue(new String(payLoadC));
		} else if(payLoadSize == 0) {
			this.payLoad.setValue("");
		} else {
			throw new IllegalArgumentException("PayLoadSize must be >= 0");
		}
	}
	
	@Override
	public boolean readBytes(PactRecord target, byte[] readBuffer, int pos)
	{	
		int key = ((readBuffer[pos] & 0xFF) << 24) |
				  ((readBuffer[pos+1] & 0xFF) << 16) |
				  ((readBuffer[pos+2] & 0xFF) << 8) |
				   (readBuffer[pos+3] & 0xFF);
		
		this.key.setValue(key);
		target.setField(0, this.key);
		target.setField(1, this.payLoad);
		return true;
	}
}
