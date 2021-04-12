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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.api.common.typeutils.TypeInformationTestBase;

/** Test for {@link BasicTypeInfo}. */
public class BasicTypeInfoTest extends TypeInformationTestBase<BasicTypeInfo<?>> {

    @Override
    protected BasicTypeInfo<?>[] getTestData() {
        return new BasicTypeInfo<?>[] {
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.BOOLEAN_TYPE_INFO,
            BasicTypeInfo.BYTE_TYPE_INFO,
            BasicTypeInfo.SHORT_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.LONG_TYPE_INFO,
            BasicTypeInfo.FLOAT_TYPE_INFO,
            BasicTypeInfo.DOUBLE_TYPE_INFO,
            BasicTypeInfo.CHAR_TYPE_INFO,
            BasicTypeInfo.DATE_TYPE_INFO,
            BasicTypeInfo.VOID_TYPE_INFO,
            BasicTypeInfo.BIG_INT_TYPE_INFO,
            BasicTypeInfo.BIG_DEC_TYPE_INFO
        };
    }
}
