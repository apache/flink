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

package org.apache.flink.table.runtime.keyselector;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

/** A utility class which key is always empty no matter what the input row is. */
public class EmptyRowDataKeySelector implements RowDataKeySelector {

    public static final EmptyRowDataKeySelector INSTANCE = new EmptyRowDataKeySelector();

    private static final long serialVersionUID = -2079386198687082032L;

    private final InternalTypeInfo<RowData> returnType = InternalTypeInfo.ofFields();

    @Override
    public RowData getKey(RowData value) throws Exception {
        return BinaryRowDataUtil.EMPTY_ROW;
    }

    @Override
    public InternalTypeInfo<RowData> getProducedType() {
        return returnType;
    }
}
