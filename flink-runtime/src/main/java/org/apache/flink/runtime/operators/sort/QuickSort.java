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

package org.apache.flink.runtime.operators.sort;

public final class QuickSort implements IndexedSorter {

    private static final IndexedSorter alt = new HeapSort();

    public QuickSort() {}

    /**
     * Fix the records into sorted order, swapping when the first record is greater than the second
     * record.
     *
     * @param s paged sortable
     * @param pN page number of first record
     * @param pO page offset of first record
     * @param rN page number of second record
     * @param rO page offset of second record
     */
    private static void fix(IndexedSortable s, int pN, int pO, int rN, int rO) {
        if (s.compare(pN, pO, rN, rO) > 0) {
            s.swap(pN, pO, rN, rO);
        }
    }

    /** Deepest recursion before giving up and doing a heapsort. Returns 2 * ceil(log(n)). */
    protected static int getMaxDepth(int x) {
        if (x <= 0) {
            throw new IllegalArgumentException("Undefined for " + x);
        }
        return (32 - Integer.numberOfLeadingZeros(x - 1)) << 2;
    }

    /**
     * Sort the given range of items using quick sort. {@inheritDoc} If the recursion depth falls
     * below {@link #getMaxDepth}, then switch to {@link HeapSort}.
     */
    public void sort(final IndexedSortable s, int p, int r) {
        int recordsPerSegment = s.recordsPerSegment();
        int recordSize = s.recordSize();
        int maxOffset = recordSize * (recordsPerSegment - 1);

        int pN = p / recordsPerSegment;
        int pO = (p % recordsPerSegment) * recordSize;

        int rN = r / recordsPerSegment;
        int rO = (r % recordsPerSegment) * recordSize;

        sortInternal(
                s,
                recordsPerSegment,
                recordSize,
                maxOffset,
                p,
                pN,
                pO,
                r,
                rN,
                rO,
                getMaxDepth(r - p));
    }

    public void sort(IndexedSortable s) {
        sort(s, 0, s.size());
    }

    /**
     * Sort the given range of items using quick sort. If the recursion depth falls below {@link
     * #getMaxDepth}, then switch to {@link HeapSort}.
     *
     * @param s paged sortable
     * @param recordsPerSegment number of records per memory segment
     * @param recordSize number of bytes per record
     * @param maxOffset offset of a last record in a memory segment
     * @param p index of first record in range
     * @param pN page number of first record in range
     * @param pO page offset of first record in range
     * @param r index of last-plus-one'th record in range
     * @param rN page number of last-plus-one'th record in range
     * @param rO page offset of last-plus-one'th record in range
     * @param depth recursion depth
     * @see #sort(IndexedSortable, int, int)
     */
    private static void sortInternal(
            final IndexedSortable s,
            int recordsPerSegment,
            int recordSize,
            int maxOffset,
            int p,
            int pN,
            int pO,
            int r,
            int rN,
            int rO,
            int depth) {
        while (true) {
            if (r - p < 13) {
                // switch to insertion sort
                int i = p + 1, iN, iO;
                if (pO == maxOffset) {
                    iN = pN + 1;
                    iO = 0;
                } else {
                    iN = pN;
                    iO = pO + recordSize;
                }

                while (i < r) {
                    int j = i, jN = iN, jO = iO;
                    int jd = j - 1, jdN, jdO;
                    if (jO == 0) {
                        jdN = jN - 1;
                        jdO = maxOffset;
                    } else {
                        jdN = jN;
                        jdO = jO - recordSize;
                    }

                    while (j > p && s.compare(jdN, jdO, jN, jO) > 0) {
                        s.swap(jN, jO, jdN, jdO);

                        j = jd;
                        jN = jdN;
                        jO = jdO;
                        jd--;
                        if (jdO == 0) {
                            jdN--;
                            jdO = maxOffset;
                        } else {
                            jdO -= recordSize;
                        }
                    }

                    i++;
                    if (iO == maxOffset) {
                        iN++;
                        iO = 0;
                    } else {
                        iO += recordSize;
                    }
                }
                return;
            }

            if (--depth < 0) {
                // switch to heap sort
                alt.sort(s, p, r);
                return;
            }

            int rdN, rdO;
            if (rO == 0) {
                rdN = rN - 1;
                rdO = maxOffset;
            } else {
                rdN = rN;
                rdO = rO - recordSize;
            }
            int m = (p + r) >>> 1,
                    mN = m / recordsPerSegment,
                    mO = (m % recordsPerSegment) * recordSize;

            // select, move pivot into first position
            fix(s, mN, mO, pN, pO);
            fix(s, mN, mO, rdN, rdO);
            fix(s, pN, pO, rdN, rdO);

            // Divide
            int i = p, iN = pN, iO = pO;
            int j = r, jN = rN, jO = rO;
            int ll = p, llN = pN, llO = pO;
            int rr = r, rrN = rN, rrO = rO;
            int cr;
            while (true) {
                i++;
                if (iO == maxOffset) {
                    iN++;
                    iO = 0;
                } else {
                    iO += recordSize;
                }

                while (i < j) {
                    if ((cr = s.compare(iN, iO, pN, pO)) > 0) {
                        break;
                    }

                    if (0 == cr) {
                        ll++;
                        if (llO == maxOffset) {
                            llN++;
                            llO = 0;
                        } else {
                            llO += recordSize;
                        }

                        if (ll != i) {
                            s.swap(llN, llO, iN, iO);
                        }
                    }

                    i++;
                    if (iO == maxOffset) {
                        iN++;
                        iO = 0;
                    } else {
                        iO += recordSize;
                    }
                }

                j--;
                if (jO == 0) {
                    jN--;
                    jO = maxOffset;
                } else {
                    jO -= recordSize;
                }

                while (j > i) {
                    if ((cr = s.compare(pN, pO, jN, jO)) > 0) {
                        break;
                    }

                    if (0 == cr) {
                        rr--;
                        if (rrO == 0) {
                            rrN--;
                            rrO = maxOffset;
                        } else {
                            rrO -= recordSize;
                        }

                        if (rr != j) {
                            s.swap(rrN, rrO, jN, jO);
                        }
                    }

                    j--;
                    if (jO == 0) {
                        jN--;
                        jO = maxOffset;
                    } else {
                        jO -= recordSize;
                    }
                }
                if (i < j) {
                    s.swap(iN, iO, jN, jO);
                } else {
                    break;
                }
            }
            j = i;
            jN = iN;
            jO = iO;
            // swap pivot- and all eq values- into position
            while (ll >= p) {
                i--;
                if (iO == 0) {
                    iN--;
                    iO = maxOffset;
                } else {
                    iO -= recordSize;
                }

                s.swap(llN, llO, iN, iO);

                ll--;
                if (llO == 0) {
                    llN--;
                    llO = maxOffset;
                } else {
                    llO -= recordSize;
                }
            }
            while (rr < r) {
                s.swap(rrN, rrO, jN, jO);

                rr++;
                if (rrO == maxOffset) {
                    rrN++;
                    rrO = 0;
                } else {
                    rrO += recordSize;
                }
                j++;
                if (jO == maxOffset) {
                    jN++;
                    jO = 0;
                } else {
                    jO += recordSize;
                }
            }

            // Conquer
            // Recurse on smaller interval first to keep stack shallow
            assert i != j;
            if (i - p < r - j) {
                sortInternal(
                        s, recordsPerSegment, recordSize, maxOffset, p, pN, pO, i, iN, iO, depth);
                p = j;
                pN = jN;
                pO = jO;
            } else {
                sortInternal(
                        s, recordsPerSegment, recordSize, maxOffset, j, jN, jO, r, rN, rO, depth);
                r = i;
                rN = iN;
                rO = iO;
            }
        }
    }
}
