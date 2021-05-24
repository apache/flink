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

package org.apache.flink.runtime.util;

import java.util.Arrays;
import java.util.NoSuchElementException;

/** Minimal implementation of an array-backed list of ints */
public class IntArrayList {

    private int size;

    private int[] array;

    public IntArrayList(final int capacity) {
        this.size = 0;
        this.array = new int[capacity];
    }

    public int size() {
        return size;
    }

    public boolean add(final int number) {
        grow(size + 1);
        array[size++] = number;
        return true;
    }

    public int removeLast() {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        --size;
        return array[size];
    }

    public void clear() {
        size = 0;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    private void grow(final int length) {
        if (length > array.length) {
            final int newLength =
                    (int) Math.max(Math.min(2L * array.length, Integer.MAX_VALUE - 8), length);
            final int[] t = new int[newLength];
            System.arraycopy(array, 0, t, 0, size);
            array = t;
        }
    }

    public int[] toArray() {
        return Arrays.copyOf(array, size);
    }

    public static final IntArrayList EMPTY =
            new IntArrayList(0) {

                @Override
                public boolean add(int number) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int removeLast() {
                    throw new UnsupportedOperationException();
                };
            };
}
