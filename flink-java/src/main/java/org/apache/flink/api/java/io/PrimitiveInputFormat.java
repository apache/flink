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
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * An input format that reads single field primitive data from a given file. The difference between
 * this and {@link org.apache.flink.api.java.io.CsvInputFormat} is that it won't go through {@link
 * org.apache.flink.api.java.tuple.Tuple1}.
 *
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@PublicEvolving
public class PrimitiveInputFormat<OT> extends DelimitedInputFormat<OT> {

    private static final long serialVersionUID = 1L;

    private Class<OT> primitiveClass;

    private static final byte CARRIAGE_RETURN = (byte) '\r';

    private static final byte NEW_LINE = (byte) '\n';

    private transient FieldParser<OT> parser;

    public PrimitiveInputFormat(Path filePath, Class<OT> primitiveClass) {
        super(filePath, null);
        this.primitiveClass = primitiveClass;
    }

    public PrimitiveInputFormat(Path filePath, String delimiter, Class<OT> primitiveClass) {
        super(filePath, null);
        this.primitiveClass = primitiveClass;
        this.setDelimiter(delimiter);
    }

    @Override
    protected void initializeSplit(FileInputSplit split, Long offset) throws IOException {
        super.initializeSplit(split, offset);
        Class<? extends FieldParser<OT>> parserType = FieldParser.getParserForType(primitiveClass);
        if (parserType == null) {
            throw new IllegalArgumentException(
                    "The type '"
                            + primitiveClass.getName()
                            + "' is not supported for the primitive input format.");
        }
        parser = InstantiationUtil.instantiate(parserType, FieldParser.class);
    }

    @Override
    public OT readRecord(OT reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        // Check if \n is used as delimiter and the end of this line is a \r, then remove \r from
        // the line
        if (this.getDelimiter().length == 1
                && this.getDelimiter()[0] == NEW_LINE
                && offset + numBytes >= 1
                && bytes[offset + numBytes - 1] == CARRIAGE_RETURN) {
            numBytes -= 1;
        }

        // Null character as delimiter is used because there's only 1 field to be parsed
        if (parser.resetErrorStateAndParse(
                        bytes, offset, numBytes + offset, new byte[] {'\0'}, reuse)
                >= 0) {
            return parser.getLastResult();
        } else {
            String s = new String(bytes, offset, numBytes, getCharset());
            throw new IOException(
                    "Could not parse value: \""
                            + s
                            + "\" as type "
                            + primitiveClass.getSimpleName());
        }
    }
}
