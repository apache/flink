/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.skew;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.input.ExternalProcessFixedLengthInputFormat;
import eu.stratosphere.pact.common.io.input.ExternalProcessInputSplit;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class MyriadGeneratorInputFormatTest {

	private final MyriadGeneratorFixedLengthInputFormat<PactInteger, PactString> format = new MyMyriadGeneratorInputFormat();
	
	
	//@Test
	public void testConfigure() {
	
		final Configuration parameters = new Configuration();
		parameters.setInteger(ExternalProcessFixedLengthInputFormat.RECORDLENGTH_PARAMETER_KEY, 100);
		parameters.setString(MyriadGeneratorFixedLengthInputFormat.GENERATOR_PATH_PARAMETER_KEY, "/home/fhueske/Development/projects/skew_bin/bin/skew-node");
		parameters.setFloat(MyriadGeneratorFixedLengthInputFormat.GENERATOR_SCALEFACTOR_PARAMETER_KEY, 0.01f);
		parameters.setString(MyriadGeneratorFixedLengthInputFormat.GENERATOR_CONFIGDIR_PARAMETER_KEY, "/home/fhueske/Development/projects/skew_bin/config");
		
		format.configure(parameters);
		try {
			format.open(new ExternalProcessInputSplit(1, "/home/fhueske/Development/projects/skew_bin/bin/skew-node -c/home/fhueske/Development/projects/skew_bin/config -s1.0 -i1 -N1"));
			
			KeyValuePair<PactInteger, PactString> record = new KeyValuePair<PactInteger, PactString>(new PactInteger(), new PactString());
			
			long cnt = 0;
			
			while(!format.reachedEnd()) {
				if (format.nextRecord(record)) {
					cnt++;
				}
			}
			
			System.out.println(cnt+" records read");
			
		} catch (IOException e) {
		}
	}

	private final class MyMyriadGeneratorInputFormat extends MyriadGeneratorFixedLengthInputFormat<PactInteger, PactString> {

		@Override
		public boolean readBytes(KeyValuePair<PactInteger, PactString> record, byte[] bytes) {
			
			int key = 0;
			key = key        | (0xFF & bytes[0]);
			key = (key << 8) | (0xFF & bytes[1]);
			key = (key << 8) | (0xFF & bytes[2]);
			key = (key << 8) | (0xFF & bytes[3]);
			String val = new String(bytes,4,96);
						
			record.getKey().setValue(key);
			record.getValue().setValue(val);
			
			return true;			
		}

	}
	
}
