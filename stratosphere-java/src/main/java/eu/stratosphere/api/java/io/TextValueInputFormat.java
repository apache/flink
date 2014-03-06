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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

import eu.stratosphere.api.common.io.DelimitedInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.StringValue;


public class TextValueInputFormat extends DelimitedInputFormat<StringValue> {
	
	private static final long serialVersionUID = 1L;
	
	private String charsetName = "UTF-8";
	
	private boolean skipInvalidLines;
	
	private transient CharsetDecoder decoder;
	
	private transient ByteBuffer byteWrapper;
	
	private transient boolean ascii;
	
	// --------------------------------------------------------------------------------------------
	
	public TextValueInputFormat(Path filePath) {
		super(filePath);
	}
	
	// --------------------------------------------------------------------------------------------	
	
	public String getCharsetName() {
		return charsetName;
	}
	
	public void setCharsetName(String charsetName) {
		if (charsetName == null)
			throw new IllegalArgumentException("The charset name may not be null.");
		
		this.charsetName = charsetName;
	}
	
	public boolean isSkipInvalidLines() {
		return skipInvalidLines;
	}
	
	public void setSkipInvalidLines(boolean skipInvalidLines) {
		this.skipInvalidLines = skipInvalidLines;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		
		if (charsetName == null || !Charset.isSupported(charsetName)) {
			throw new RuntimeException("Unsupported charset: " + charsetName);
		}
		
		this.decoder = Charset.forName(charsetName).newDecoder();
		this.byteWrapper = ByteBuffer.allocate(1);
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public StringValue readRecord(StringValue reuse, byte[] bytes, int offset, int numBytes) {
		if (this.ascii) {
			reuse.setValueAscii(bytes, offset, numBytes);
			return reuse;
		}
		else {
			ByteBuffer byteWrapper = this.byteWrapper;
			if (bytes != byteWrapper.array()) {
				byteWrapper = ByteBuffer.wrap(bytes, 0, bytes.length);
				this.byteWrapper = byteWrapper;
			}
			byteWrapper.limit(offset + numBytes);
			byteWrapper.position(offset);
				
			try {
				CharBuffer result = this.decoder.decode(byteWrapper);
				reuse.setValue(result);
				return reuse;
			}
			catch (CharacterCodingException e) {
				if (skipInvalidLines) {
					return null;
				} else {
					byte[] copy = new byte[numBytes];
					System.arraycopy(bytes, offset, copy, 0, numBytes);
					throw new RuntimeException("Line could not be encoded: " + Arrays.toString(copy), e);
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "TextValueInputFormat (" + getFilePath() + ") - " + this.charsetName + (this.skipInvalidLines ? "(skipping invalid lines)" : "");
	}
}
