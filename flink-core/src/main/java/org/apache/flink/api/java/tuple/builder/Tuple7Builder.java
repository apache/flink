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
import org.apache.flink.api.java.tuple.Tuple7;

import java.util.ArrayList;
import java.util.List;

/**
 * A builder class for {@link Tuple7}.
 *
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 * @param <T2> The type of field 2
 * @param <T3> The type of field 3
 * @param <T4> The type of field 4
 * @param <T5> The type of field 5
 * @param <T6> The type of field 6
 */
@Public
public class Tuple7Builder<T0, T1, T2, T3, T4, T5, T6> {

    private List<Tuple7<T0, T1, T2, T3, T4, T5, T6>> tuples = new ArrayList<>();

    public Tuple7Builder<T0, T1, T2, T3, T4, T5, T6> add(
            T0 f0, T1 f1, T2 f2, T3 f3, T4 f4, T5 f5, T6 f6) {
        tuples.add(new Tuple7<>(f0, f1, f2, f3, f4, f5, f6));
        return this;
    }

    @SuppressWarnings("unchecked")
    public Tuple7<T0, T1, T2, T3, T4, T5, T6>[] build() {
        return tuples.toArray(new Tuple7[tuples.size()]);
    }
}
