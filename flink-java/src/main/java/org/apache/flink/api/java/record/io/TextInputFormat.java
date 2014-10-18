/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.api.java.record.io;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;

/**
 * Base implementation for an input format that returns each line as a separate record that contains
 * only a single string, namely the line.
 */
public class TextInputFormat extends DelimitedInputFormat {
	private static final long serialVersionUID = 1L;
	
	public static final String CHARSET_NAME = "textformat.charset";
	
	public static final String FIELD_POS = "textformat.pos";
	
	public static final String DEFAULT_CHARSET_NAME = "UTF-8";
	
	private static final Logger LOG = LoggerFactory.getLogger(TextInputFormat.class);
	
	
	protected final StringValue theString = new StringValue();
	
	
	// all fields below here are set in configure / open, so we do not serialze them
	
	protected transient CharsetDecoder decoder;
	
	protected transient ByteBuffer byteWrapper;
	
	protected transient int pos;
	
	protected transient boolean ascii;
	
	/**
	 * Code of \r, used to remove \r from a line when the line ends with \r\n
	 */
	private static final byte CARRIAGE_RETURN = (byte) '\r';

	/**
	 * Code of \n, used to identify if \n is used as delimiter
	 */
	private static final byte NEW_LINE = (byte) '\n';

	
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
		
		// get the field position to write in the record
		this.pos = parameters.getInteger(FIELD_POS, 0);
		if (this.pos < 0) {
			throw new RuntimeException("Illegal configuration value for the target position: " + this.pos);
		}
	}

	// --------------------------------------------------------------------------------------------

	public Record readRecord(Record reuse, byte[] bytes, int offset, int numBytes) {
		StringValue str = this.theString;
		
		//Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
		if (this.getDelimiter() != null && this.getDelimiter().length == 1 
				&& this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1 
				&& bytes[offset+numBytes-1] == CARRIAGE_RETURN){
			numBytes -= 1;
		}

		
		if (this.ascii) {
			str.setValueAscii(bytes, offset, numBytes);
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
				str.setValue(result);
			}
			catch (CharacterCodingException e) {
				byte[] copy = new byte[numBytes];
				System.arraycopy(bytes, offset, copy, 0, numBytes);
				LOG.warn("Line could not be encoded: " + Arrays.toString(copy), e);
				return null;
			}
		}
		
		reuse.clear();
		reuse.setField(this.pos, str);
		return reuse;
	}
}
