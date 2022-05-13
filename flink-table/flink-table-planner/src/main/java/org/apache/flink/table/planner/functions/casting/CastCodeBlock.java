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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/**
 * Generated cast code block result. This POJO contains the Java code of the block performing the
 * cast, the output is null term and the output variable containing the cast result. It is
 * guaranteed that the {@code returnTerm} and {@code isNullTerm} can be accessed within the external
 * scope of the returned {@code code}. Both {@code returnTerm} and {@code isNullTerm} can be either
 * variable names or expressions.
 */
@Internal
public class CastCodeBlock {

    private final String code;
    private final String returnTerm;
    private final String isNullTerm;

    private CastCodeBlock(String code, String returnTerm, String isNullTerm) {
        this.code = code;
        this.returnTerm = returnTerm;
        this.isNullTerm = isNullTerm;
    }

    public static CastCodeBlock withoutCode(String returnTerm, String isNullTerm) {
        return new CastCodeBlock("", returnTerm, isNullTerm);
    }

    public static CastCodeBlock withCode(String code, String returnTerm, String isNullTerm) {
        return new CastCodeBlock(code, returnTerm, isNullTerm);
    }

    public String getCode() {
        return code;
    }

    public String getReturnTerm() {
        return returnTerm;
    }

    public String getIsNullTerm() {
        return isNullTerm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CastCodeBlock that = (CastCodeBlock) o;
        return Objects.equals(code, that.code)
                && Objects.equals(returnTerm, that.returnTerm)
                && Objects.equals(isNullTerm, that.isNullTerm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, returnTerm, isNullTerm);
    }
}
