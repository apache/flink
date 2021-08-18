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

// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------

package org.apache.flink.api.java.tuple.builder;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.tuple.Tuple16;

import java.util.ArrayList;
import java.util.List;

/**
 * A builder class for {@link Tuple16}.
 *
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 * @param <T2> The type of field 2
 * @param <T3> The type of field 3
 * @param <T4> The type of field 4
 * @param <T5> The type of field 5
 * @param <T6> The type of field 6
 * @param <T7> The type of field 7
 * @param <T8> The type of field 8
 * @param <T9> The type of field 9
 * @param <T10> The type of field 10
 * @param <T11> The type of field 11
 * @param <T12> The type of field 12
 * @param <T13> The type of field 13
 * @param <T14> The type of field 14
 * @param <T15> The type of field 15
 */
@Public
public class Tuple16Builder<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> {

    private List<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>
            tuples = new ArrayList<>();

    public Tuple16Builder<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> add(
            T0 f0,
            T1 f1,
            T2 f2,
            T3 f3,
            T4 f4,
            T5 f5,
            T6 f6,
            T7 f7,
            T8 f8,
            T9 f9,
            T10 f10,
            T11 f11,
            T12 f12,
            T13 f13,
            T14 f14,
            T15 f15) {
        tuples.add(
                new Tuple16<>(
                        f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15));
        return this;
    }

    @SuppressWarnings("unchecked")
    public Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>[] build() {
        return tuples.toArray(new Tuple16[tuples.size()]);
    }
}
