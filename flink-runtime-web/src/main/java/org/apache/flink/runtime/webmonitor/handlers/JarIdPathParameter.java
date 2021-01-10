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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;

import java.nio.file.Path;
import java.nio.file.Paths;

/** Path parameter to identify uploaded jar files. */
public class JarIdPathParameter extends MessagePathParameter<String> {

    public static final String KEY = "jarid";

    protected JarIdPathParameter() {
        super(KEY);
    }

    @Override
    protected String convertFromString(final String value) throws ConversionException {
        final Path path = Paths.get(value);
        if (path.getParent() != null) {
            throw new ConversionException(
                    String.format("%s must be a filename only (%s)", KEY, path));
        }
        return value;
    }

    @Override
    protected String convertToString(final String value) {
        return value;
    }

    @Override
    public String getDescription() {
        return "String value that identifies a jar. When uploading the jar a path is returned, where the filename "
                + "is the ID. This value is equivalent to the `"
                + JarListInfo.JarFileInfo.JAR_FILE_FIELD_ID
                + "` field "
                + "in the list of uploaded jars ("
                + JarListHeaders.URL
                + ").";
    }
}
