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

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static java.util.function.Function.identity;
import static java.util.stream.Stream.generate;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap.toImmutableMap;

/** Sample data for various test cases. */
public class SampleData {

    private static final Random RAND = new Random(System.currentTimeMillis());
    private static final Supplier<Integer> LIST_SIZE = () -> RAND.nextInt(10) + 5;
    private static final long MIN_DAY = LocalDate.of(2000, 1, 1).toEpochDay();
    private static final long MAX_DAY = LocalDate.of(2040, 12, 31).toEpochDay();
    private static final Supplier<Bar> BAR_SUPPLIER =
            () -> new Bar(RAND.nextBoolean(), randomAlphanumeric(10));
    private static final Supplier<List<Bar>> BAR_LIST_SUPPLIER =
            () -> generate(BAR_SUPPLIER).limit(LIST_SIZE.get()).collect(toImmutableList());
    private static final Supplier<Map<String, Bar>> BAR_MAP_SUPPLIER =
            () ->
                    generate(BAR_SUPPLIER)
                            .limit(LIST_SIZE.get())
                            .collect(toImmutableMap(Bar::toString, identity()));

    // --------------------------------//
    //                                 //
    // Random sample data for tests.   //
    //                                 //
    // --------------------------------//

    public static final List<Boolean> BOOLEAN_LIST =
            generate(RAND::nextBoolean).limit(LIST_SIZE.get()).collect(toImmutableList());

    public static final List<Integer> INTEGER_LIST =
            generate(RAND::nextInt).limit(LIST_SIZE.get()).collect(toImmutableList());

    public static final List<byte[]> BYTES_LIST =
            generate(() -> randomAscii(8))
                    .limit(LIST_SIZE.get())
                    .map(s -> s.getBytes(StandardCharsets.UTF_8))
                    .collect(toImmutableList());

    public static final List<Byte> INT_8_LIST =
            generate(RAND::nextInt)
                    .limit(LIST_SIZE.get())
                    .map(Integer::byteValue)
                    .collect(toImmutableList());

    public static final List<Short> INT_16_LIST =
            generate(RAND::nextInt)
                    .limit(LIST_SIZE.get())
                    .map(Integer::shortValue)
                    .collect(toImmutableList());

    public static final List<Long> INT_64_LIST =
            generate(RAND::nextLong).limit(LIST_SIZE.get()).collect(toImmutableList());

    public static final List<Double> DOUBLE_LIST =
            generate(RAND::nextDouble).limit(LIST_SIZE.get()).collect(toImmutableList());

    public static final List<Float> FLOAT_LIST =
            generate(RAND::nextFloat).limit(LIST_SIZE.get()).collect(toImmutableList());

    public static final List<String> STRING_LIST =
            generate(() -> randomAlphanumeric(8)).limit(LIST_SIZE.get()).collect(toImmutableList());

    public static final List<LocalDate> LOCAL_DATE_LIST =
            generate(() -> ThreadLocalRandom.current().nextLong(MIN_DAY, MAX_DAY))
                    .limit(LIST_SIZE.get())
                    .map(LocalDate::ofEpochDay)
                    .collect(toImmutableList());

    public static final List<LocalDateTime> LOCAL_DATE_TIME_LIST =
            generate(() -> ThreadLocalRandom.current().nextLong(MIN_DAY, MAX_DAY))
                    .limit(LIST_SIZE.get())
                    .map(LocalDate::ofEpochDay)
                    .map(LocalDate::atStartOfDay)
                    .collect(toImmutableList());

    public static final List<FA> FA_LIST =
            generate(() -> new FA(BAR_LIST_SUPPLIER.get().toArray(new Bar[0])))
                    .limit(LIST_SIZE.get())
                    .collect(toImmutableList());

    public static final List<Foo> FOO_LIST =
            generate(() -> new Foo(RAND.nextInt(), RAND.nextFloat(), BAR_SUPPLIER.get()))
                    .limit(LIST_SIZE.get())
                    .collect(toImmutableList());

    public static final List<FL> FL_LIST =
            generate(() -> new FL(BAR_LIST_SUPPLIER.get()))
                    .limit(LIST_SIZE.get())
                    .collect(toImmutableList());

    public static final List<FM> FM_LIST =
            generate(() -> new FM(BAR_MAP_SUPPLIER.get()))
                    .limit(LIST_SIZE.get())
                    .collect(toImmutableList());

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
