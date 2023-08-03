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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Input Format that reads text files. Each line results in another element.
 *
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@PublicEvolving
public class TextInputFormat extends DelimitedInputFormat<String> {

    private static final long serialVersionUID = 1L;

    /** Code of \r, used to remove \r from a line when the line ends with \r\n. */
    private static final byte CARRIAGE_RETURN = (byte) '\r';

    /** Code of \n, used to identify if \n is used as delimiter. */
    private static final byte NEW_LINE = (byte) '\n';

    /** The name of the charset to use for decoding. */
    private String charsetName = "UTF-8";

    // --------------------------------------------------------------------------------------------

    public TextInputFormat(Path filePath) {
        super(filePath, null);
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

    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        if (charsetName == null || !Charset.isSupported(charsetName)) {
            throw new RuntimeException("Unsupported charset: " + charsetName);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String readRecord(String reusable, byte[] bytes, int offset, int numBytes)
            throws IOException {
        // Check if \n is used as delimiter and the end of this line is a \r, then remove \r from
        // the line
        if (this.getDelimiter() != null
                && this.getDelimiter().length == 1
                && this.getDelimiter()[0] == NEW_LINE
                && offset + numBytes >= 1
                && bytes[offset + numBytes - 1] == CARRIAGE_RETURN) {
            numBytes -= 1;
        }

        return new String(bytes, offset, numBytes, this.charsetName);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "TextInputFormat (" + Arrays.toString(getFilePaths()) + ") - " + this.charsetName;
    }

    @Override
    public boolean supportsMultiPaths() {
        return true;
    }
}
