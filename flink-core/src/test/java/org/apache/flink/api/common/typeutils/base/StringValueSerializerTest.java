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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.StringValue;

/** A test for the {@link StringValueSerializer}. */
public class StringValueSerializerTest extends SerializerTestBase<StringValue> {

    @Override
    protected TypeSerializer<StringValue> createSerializer() {
        return new StringValueSerializer();
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<StringValue> getTypeClass() {
        return StringValue.class;
    }

    @Override
    protected StringValue[] getTestData() {
        return new StringValue[] {
            new StringValue("a"),
            new StringValue(""),
            new StringValue("bcd"),
            new StringValue("jbmbmner8 jhk hj \n \t üäßß@µ"),
            new StringValue(""),
            new StringValue("non-empty"),
            new StringValue(
                    "http://some-uri.com/that/is/a/common/prefix/to/all(((cmNH`0R)H<tnLa:/;Q,igWY2EdwW^W7T3H6NMRoqR[O2TqQ@SbGKc(:0XOXq-5]ndm-R8?=,o?AW+9Pi_v4eON=Mpje7N4n*-nhFWKn>Sn0cGMlnDquY@-F:QY@-UZ.-//*OL*8\\SIpiZa)tefalZ99-P_-WFIaKPeGbkQ^iRgd,YYkn7:jBAW::PqAYtgl73dTaJ2CIT:11HJ70<ATOXZ]c6b_7EgQU,@uq+SMa=7Z]kg/OZ>TGduw>D7Lu[nEj_l=Ucwo5BQtBESh/4V>N9nj/pDLw[NM)a=ac6R-(FM2U+dwROMUH;);Y=")
        };
    }
}
