package eu.stratosphere.pact.example.skew;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.input.ExternalProcessFixedLengthInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class MyriadIntInputFormat extends MyriadGeneratorFixedLengthInputFormat<PactInteger, PactString> {

	public static final String PAYLOAD_SIZE_PARAMETER_KEY = "myriad.input.format.payloadSize";
	
	private String payLoad;
	
	@Override
	public void configure(Configuration parameters) {
		parameters.setLong(ExternalProcessFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 4);
		super.configure(parameters);
		super.setTargetReadBufferSize(1024);
		
		int payLoadSize = (int)parameters.getLong(PAYLOAD_SIZE_PARAMETER_KEY, 0);
		if(payLoadSize > 0) {
			char[] payLoadC = new char[payLoadSize/2];
			for(int i=0;i<payLoadC.length;i++) {
				payLoadC[i] = '.';
			}
			this.payLoad = new String(payLoadC);
		} else if(payLoadSize == 0) {
			this.payLoad = "";
		} else {
			throw new IllegalArgumentException("PayLoadSize must be >= 0");
		}
	}
	
	@Override
	public boolean readBytes(KeyValuePair<PactInteger, PactString> record, byte[] bytes) {
		
		int key = 0;
		key = key        | (0xFF & bytes[0]);
		key = (key << 8) | (0xFF & bytes[1]);
		key = (key << 8) | (0xFF & bytes[2]);
		key = (key << 8) | (0xFF & bytes[3]);
					
		record.getKey().setValue(key);
		record.getValue().setValue(payLoad);
	
		return true;			
	}

}
