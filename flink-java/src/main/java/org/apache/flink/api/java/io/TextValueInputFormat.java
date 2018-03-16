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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Input format that reads text files.
 */
@PublicEvolving
public class TextValueInputFormat extends DelimitedInputFormat<StringValue> {

	private static final long serialVersionUID = 1L;

	private String charsetName = "UTF-8";

	private boolean skipInvalidLines;

	private transient CharsetDecoder decoder;

	private transient ByteBuffer byteWrapper;

	private transient boolean ascii;

	// --------------------------------------------------------------------------------------------

	public TextValueInputFormat(Path filePath) {
		super(filePath, null);
	}

	// --------------------------------------------------------------------------------------------

	public String getCharsetName() {
		return charsetName;
	}

	public void setCharsetName(String charsetName) {
		if (charsetName == null) {
			throw new IllegalArgumentException("The charset name may not be null.");
		}

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

		if (charsetName.equalsIgnoreCase(StandardCharsets.US_ASCII.name())) {
			ascii = true;
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
		return "TextValueInputFormat (" + Arrays.toString(getFilePaths()) + ") - " + this.charsetName + (this.skipInvalidLines ? "(skipping invalid lines)" : "");
	}

	@Override
	public boolean supportsMultiPaths() {
		return true;
	}
}
