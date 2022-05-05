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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a DynamoDb Attribute value.
 *
 * <p>The following fields represents the different data types in dynamo db. Only <b>one</b> field
 * should be set.
 *
 * <ul>
 *   <li>s - String
 *   <li>n - Number
 *   <li>b - Binary
 *   <li>bool - Boolean
 *   <li>nul - Null
 *   <li>ss - String set
 *   <li>ns - Number set
 *   <li>bs - Binary set
 *   <li>l - List of DynamoDbAttributeValue
 *   <li>m - Map of String to DynamoDbAttributeValue
 * </ul>
 */
@PublicEvolving
public class DynamoDbAttributeValue implements Serializable {

    private final Boolean nul;
    private final String s;
    private final String n;
    private final byte[] b;
    private final Boolean bool;
    private final Set<String> ss;
    private final Set<String> ns;
    private final Set<byte[]> bs;
    private final List<DynamoDbAttributeValue> l;
    private final Map<String, DynamoDbAttributeValue> m;

    private DynamoDbAttributeValue(
            Boolean nul,
            String s,
            String n,
            byte[] b,
            Boolean bool,
            Set<String> ss,
            Set<String> ns,
            Set<byte[]> bs,
            List<DynamoDbAttributeValue> l,
            Map<String, DynamoDbAttributeValue> m) {
        this.nul = nul;
        this.s = s;
        this.n = n;
        this.b = b;
        this.bool = bool;
        this.ss = ss;
        this.ns = ns;
        this.bs = bs;
        this.l = l;
        this.m = m;
    }

    public Boolean nul() {
        return nul;
    }

    public String s() {
        return s;
    }

    public String n() {
        return n;
    }

    public byte[] b() {
        return b;
    }

    public Boolean bool() {
        return bool;
    }

    public Set<String> ss() {
        return ss;
    }

    public Set<String> ns() {
        return ns;
    }

    public Set<byte[]> bs() {
        return bs;
    }

    public List<DynamoDbAttributeValue> l() {
        return l;
    }

    public Map<String, DynamoDbAttributeValue> m() {
        return m;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamoDbAttributeValue that = (DynamoDbAttributeValue) o;
        return Objects.equals(nul, that.nul)
                && Objects.equals(s, that.s)
                && Objects.equals(n, that.n)
                && Arrays.equals(b, that.b)
                && Objects.equals(bool, that.bool)
                && Objects.equals(ss, that.ss)
                && Objects.equals(ns, that.ns)
                && Objects.equals(bs, that.bs)
                && Objects.equals(l, that.l)
                && Objects.equals(m, that.m);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(nul, s, n, bool, ss, ns, bs, l, m);
        result = 31 * result + Arrays.hashCode(b);
        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder. */
    public static final class Builder {
        private Boolean nul;
        private String s;
        private String n;
        private byte[] b;
        private Boolean bool;
        private Set<String> ss;
        private Set<String> ns;
        private Set<byte[]> bs;
        private List<DynamoDbAttributeValue> l;
        private Map<String, DynamoDbAttributeValue> m;

        public Builder nul(Boolean nul) {
            this.nul = nul;
            return this;
        }

        public Builder s(String s) {
            this.s = s;
            return this;
        }

        public Builder n(String n) {
            this.n = n;
            return this;
        }

        public Builder b(byte[] b) {
            this.b = b;
            return this;
        }

        public Builder bool(Boolean bool) {
            this.bool = bool;
            return this;
        }

        public Builder ss(Set<String> ss) {
            this.ss = ss;
            return this;
        }

        public Builder ns(Set<String> ns) {
            this.ns = ns;
            return this;
        }

        public Builder bs(Set<byte[]> bs) {
            this.bs = bs;
            return this;
        }

        public Builder l(List<DynamoDbAttributeValue> l) {
            this.l = l;
            return this;
        }

        public Builder m(Map<String, DynamoDbAttributeValue> m) {
            this.m = m;
            return this;
        }

        public DynamoDbAttributeValue build() {
            return new DynamoDbAttributeValue(nul, s, n, b, bool, ss, ns, bs, l, m);
        }
    }
}
