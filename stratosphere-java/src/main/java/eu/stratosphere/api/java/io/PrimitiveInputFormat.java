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

import eu.stratosphere.api.common.io.DelimitedInputFormat;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.parser.FieldParser;
import eu.stratosphere.util.InstantiationUtil;

import java.io.IOException;

/**
 * An input format that reads single field primitive data from a given file. The difference between this and
 * {@link eu.stratosphere.api.java.io.CsvInputFormat} is that it won't go through {@link eu.stratosphere.api.java.tuple.Tuple1}.
 */
public class PrimitiveInputFormat<OT> extends DelimitedInputFormat<OT> {

	private Class<OT> primitiveClass;

	private static final byte CARRIAGE_RETURN = (byte) '\r';

	private static final byte NEW_LINE = (byte) '\n';

	private transient FieldParser<OT> parser;


	public PrimitiveInputFormat(Path filePath, Class<OT> primitiveClass) {
		super(filePath);
		this.primitiveClass = primitiveClass;
	}

	public PrimitiveInputFormat(Path filePath, char delimiter, Class<OT> primitiveClass) {
		super(filePath);
		this.primitiveClass = primitiveClass;
		this.setDelimiter(delimiter);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		Class<? extends FieldParser<OT>> parserType = FieldParser.getParserForType(primitiveClass);
		if (parserType == null) {
			throw new IllegalArgumentException("The type '" + primitiveClass.getName() + "' is not supported for the primitive input format.");
		}
		parser = InstantiationUtil.instantiate(parserType, FieldParser.class);
	}

	@Override
	public OT readRecord(OT reuse, byte[] bytes, int offset, int numBytes) {
		//Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
		if (this.getDelimiter() != null && this.getDelimiter().length == 1
			&& this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1
			&& bytes[offset+numBytes-1] == CARRIAGE_RETURN){
			numBytes -= 1;
		}

		parser.parseField(bytes, offset, numBytes + offset, (char) this.getDelimiter()[0], reuse);
		return parser.getLastResult();
	}
}
