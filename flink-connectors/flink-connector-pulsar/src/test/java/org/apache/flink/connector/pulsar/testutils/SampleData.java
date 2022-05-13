/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.testutils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Sample data for various test cases. */
public class SampleData {

    // --------------------------------//
    //                                 //
    // Random sample data for tests.   //
    //                                 //
    // --------------------------------//

    /** Foo type. */
    public static class Foo {
        public int i;
        public float f;
        public Bar bar;

        public Foo(int i, float f, Bar bar) {
            this.i = i;
            this.f = f;
            this.bar = bar;
        }

        public Foo() {}

        @Override
        public String toString() {
            return "" + i + "," + f + "," + (bar == null ? "null" : bar.toString());
        }

        public int getI() {
            return this.i;
        }

        public float getF() {
            return this.f;
        }

        public Bar getBar() {
            return this.bar;
        }

        public void setI(int i) {
            this.i = i;
        }

        public void setF(float f) {
            this.f = f;
        }

        public void setBar(Bar bar) {
            this.bar = bar;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Foo)) {
                return false;
            }
            Foo foo = (Foo) o;
            return i == foo.i && Float.compare(foo.f, f) == 0 && Objects.equals(bar, foo.bar);
        }

        @Override
        public int hashCode() {
            return Objects.hash(i, f, bar);
        }
    }

    /** Bar type. */
    public static class Bar {
        public boolean b;
        public String s;

        public Bar(boolean b, String s) {
            this.b = b;
            this.s = s;
        }

        public Bar() {}

        @Override
        public String toString() {
            return "" + b + "," + s;
        }

        public boolean isB() {
            return this.b;
        }

        public String getS() {
            return this.s;
        }

        public void setB(boolean b) {
            this.b = b;
        }

        public void setS(String s) {
            this.s = s;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Bar)) {
                return false;
            }
            Bar bar = (Bar) o;
            return b == bar.b && Objects.equals(s, bar.s);
        }

        @Override
        public int hashCode() {
            return Objects.hash(b, s);
        }
    }

    /** FL type. */
    public static class FL {
        public List<Bar> l;

        public FL(List<Bar> l) {
            this.l = l;
        }

        public FL() {}

        @Override
        public String toString() {
            if (l == null) {
                return "null";
            } else {
                StringBuilder sb = new StringBuilder();

                for (int i = 0; i < l.size(); i++) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append("[");
                    sb.append(l.get(i));
                    sb.append("]");
                }

                return sb.toString();
            }
        }

        public List<Bar> getL() {
            return this.l;
        }

        public void setL(List<Bar> l) {
            this.l = l;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FL)) {
                return false;
            }
            FL fl = (FL) o;
            return Objects.equals(l, fl.l);
        }

        @Override
        public int hashCode() {
            return Objects.hash(l);
        }
    }

    /** FA type. */
    public static class FA {
        public Bar[] l;

        public FA(Bar[] l) {
            this.l = l;
        }

        public FA() {}

        @Override
        public String toString() {
            if (l == null) {
                return "null";
            } else {
                StringBuilder sb = new StringBuilder();

                for (int i = 0; i < l.length; i++) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append("[");
                    sb.append(l[i]);
                    sb.append("]");
                }

                return sb.toString();
            }
        }

        public Bar[] getL() {
            return this.l;
        }

        public void setL(Bar[] l) {
            this.l = l;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FA)) {
                return false;
            }
            FA fa = (FA) o;
            return Arrays.equals(l, fa.l);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(l);
        }
    }

    /** FM type. */
    public static class FM {
        public Map<String, Bar> m;

        public FM(Map<String, Bar> m) {
            this.m = m;
        }

        public FM() {}

        @Override
        public String toString() {
            if (m == null) {
                return "null";
            } else {
                StringBuilder sb = new StringBuilder();

                Iterator<Map.Entry<String, Bar>> iterator = m.entrySet().iterator();
                int i = 0;
                while (iterator.hasNext()) {
                    if (i != 0) {
                        sb.append(",");
                    }

                    sb.append("{");
                    sb.append(iterator.next());
                    sb.append("}");
                    i += 1;
                }

                return sb.toString();
            }
        }

        public Map<String, Bar> getM() {
            return this.m;
        }

        public void setM(Map<String, Bar> m) {
            this.m = m;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FM)) {
                return false;
            }
            FM fm = (FM) o;
            return Objects.equals(m, fm.m);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m);
        }
    }
}
