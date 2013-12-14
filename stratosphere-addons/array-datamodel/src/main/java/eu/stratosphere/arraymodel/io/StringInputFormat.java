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

package eu.stratosphere.arraymodel.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.io.DelimitedInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.PactString;
import eu.stratosphere.types.Value;

/**
 * Base implementation for an input format that returns each line as a separate record that contains
 * only a single string, namely the line.
 */
public class StringInputFormat extends DelimitedInputFormat<Value[]> {
	
	private static final long serialVersionUID = 1L;

	public static final String CHARSET_NAME = "textformat.charset";
	
	public static final String DEFAULT_CHARSET_NAME = "UTF-8";
	
	private static final Log LOG = LogFactory.getLog(StringInputFormat.class);
	
	protected final PactString theString = new PactString();
	
	protected CharsetDecoder decoder;
	
	protected ByteBuffer byteWrapper;
	
	protected boolean ascii;
	
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		
		// get the charset for the decoding
		String charsetName = parameters.getString(CHARSET_NAME, DEFAULT_CHARSET_NAME);
		if (charsetName == null || !Charset.isSupported(charsetName)) {
			throw new RuntimeException("Unsupported charset: " + charsetName);
		}
		
		if (charsetName.equals("ISO-8859-1") || charsetName.equalsIgnoreCase("ASCII")) {
			this.ascii = true;
		} else {
			this.decoder = Charset.forName(charsetName).newDecoder();
			this.byteWrapper = ByteBuffer.allocate(1);
		}
	}

	// --------------------------------------------------------------------------------------------

	public boolean readRecord(Value[] target, byte[] bytes, int offset, int numBytes) {
		PactString str = this.theString;
		
		if (this.ascii) {
			str.setValueAscii(bytes, offset, numBytes);
		}
		else {
			ByteBuffer byteWrapper = this.byteWrapper;
			if (bytes != byteWrapper.array()) {
				byteWrapper = ByteBuffer.wrap(bytes, 0, bytes.length);
				this.byteWrapper = byteWrapper;
			}
			byteWrapper.clear();
			byteWrapper.position(offset);
			byteWrapper.limit(offset + numBytes);
				
			try {
				CharBuffer result = this.decoder.decode(byteWrapper);
				str.setValue(result);
			}
			catch (CharacterCodingException e) {
				byte[] copy = new byte[numBytes];
				System.arraycopy(bytes, offset, copy, 0, numBytes);
				LOG.warn("Line could not be encoded: " + Arrays.toString(copy), e);
				return false;
			}
		}
		
		target[0] = str;
		return true;
	}
}
