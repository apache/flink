/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.io;

import java.nio.charset.Charset;

import eu.stratosphere.api.common.io.DelimitedInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;


public class TextInputFormat extends DelimitedInputFormat<String> {
	
	private static final long serialVersionUID = 1L;
	
	private String charsetName = "UTF-8";
	
//	private boolean skipInvalidLines;
	
	private transient Charset charset;

	/**
	 * Code of \r, used to remove \r from a line when the line ends with \r\n
	 */
	private static final byte CARRIAGE_RETURN = (byte) '\r';

	/**
	 * Code of \n, used to identify if \n is used as delimiter
	 */
	private static final byte NEW_LINE = (byte) '\n';

	
	// --------------------------------------------------------------------------------------------
	
	public TextInputFormat(Path filePath) {
		super(filePath);
	}
	
	// --------------------------------------------------------------------------------------------	
	
	public String getCharsetName() {
		return charsetName;
	}
	
	public void setCharsetName(String charsetName) {
		if (charsetName == null) {
			throw new IllegalArgumentException("Charset must not be null.");
		}
		
		this.charsetName = charsetName;
	}
	
//	public boolean isSkipInvalidLines() {
//		return skipInvalidLines;
//	}
//	
//	public void setSkipInvalidLines(boolean skipInvalidLines) {
//		this.skipInvalidLines = skipInvalidLines;
//	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		
		if (charsetName == null || !Charset.isSupported(charsetName)) {
			throw new RuntimeException("Unsupported charset: " + charsetName);
		}
		this.charset = Charset.forName(charsetName);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String readRecord(String reusable, byte[] bytes, int offset, int numBytes) {
		//Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
		if (this.getDelimiter() != null && this.getDelimiter().length == 1 
				&& this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1 
				&& bytes[offset+numBytes-1] == CARRIAGE_RETURN){
			numBytes -= 1;
		}
		
		return new String(bytes, offset, numBytes, this.charset);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "TextInputFormat (" + getFilePath() + ") - " + this.charsetName; // + (this.skipInvalidLines ? "(skipping invalid lines)" : "");
	}
}
