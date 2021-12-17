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

package org.apache.flink.table.utils.print;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;

import java.io.PrintWriter;
import java.util.Iterator;

/** Print only the result content as raw form. column delimiter is ",", row delimiter is "\n". */
@Internal
public final class RawContentStyle implements PrintStyle {

    private final RowDataToStringConverter converter;

    RawContentStyle(RowDataToStringConverter converter) {
        this.converter = converter;
    }

    @Override
    public void print(Iterator<RowData> it, PrintWriter printWriter) {
        while (it.hasNext()) {
            printWriter.println(String.join(", ", converter.convert(it.next())));
        }
        printWriter.flush();
    }
}
