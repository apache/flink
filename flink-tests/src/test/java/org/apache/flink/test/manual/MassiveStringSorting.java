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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/** Test {@link ExternalSorter} on a large set of {@code String}. */
public class MassiveStringSorting {

    private static final long SEED = 347569784659278346L;

    public void testStringSorting() {
        File input = null;
        File sorted = null;

        try {
            // the source file
            input =
                    generateFileWithStrings(
                            300000, "http://some-uri.com/that/is/a/common/prefix/to/all");

            // the sorted file
            sorted = File.createTempFile("sorted_strings", "txt");

            String[] command = {
                "/bin/bash",
                "-c",
                "export LC_ALL=\"C\" && cat \""
                        + input.getAbsolutePath()
                        + "\" | sort > \""
                        + sorted.getAbsolutePath()
                        + "\""
            };

            Process p = null;
            try {
                p = Runtime.getRuntime().exec(command);
                int retCode = p.waitFor();
                if (retCode != 0) {
                    throw new Exception("Command failed with return code " + retCode);
                }
                p = null;
            } finally {
                if (p != null) {
                    p.destroy();
                }
            }

            // sort the data
            Sorter<String> sorter = null;
            BufferedReader reader = null;
            BufferedReader verifyReader = null;
            MemoryManager mm = null;

            try (IOManager ioMan = new IOManagerAsync()) {
                mm = MemoryManagerBuilder.newBuilder().setMemorySize(1024 * 1024).build();

                TypeSerializer<String> serializer = StringSerializer.INSTANCE;
                TypeComparator<String> comparator = new StringComparator(true);

                reader = new BufferedReader(new FileReader(input));
                MutableObjectIterator<String> inputIterator =
                        new StringReaderMutableObjectIterator(reader);

                sorter =
                        ExternalSorter.newBuilder(mm, new DummyInvokable(), serializer, comparator)
                                .maxNumFileHandles(4)
                                .enableSpilling(ioMan, 0.8f)
                                .memoryFraction(1.0)
                                .objectReuse(false)
                                .largeRecords(true)
                                .build(inputIterator);

                MutableObjectIterator<String> sortedData = sorter.getIterator();

                reader.close();

                // verify
                verifyReader = new BufferedReader(new FileReader(sorted));
                String next;

                while ((next = verifyReader.readLine()) != null) {
                    String nextFromStratoSort = sortedData.next("");

                    Assert.assertNotNull(nextFromStratoSort);
                    Assert.assertEquals(next, nextFromStratoSort);
                }
            } finally {
                if (reader != null) {
                    reader.close();
                }
                if (verifyReader != null) {
                    verifyReader.close();
                }
                if (sorter != null) {
                    sorter.close();
                }
                if (mm != null) {
                    mm.shutdown();
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (input != null) {
                input.delete();
            }
            if (sorted != null) {
                sorted.delete();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testStringTuplesSorting() {
        final int numStrings = 300000;

        File input = null;
        File sorted = null;

        try {
            // the source file
            input =
                    generateFileWithStringTuples(
                            numStrings, "http://some-uri.com/that/is/a/common/prefix/to/all");

            // the sorted file
            sorted = File.createTempFile("sorted_strings", "txt");

            String[] command = {
                "/bin/bash",
                "-c",
                "export LC_ALL=\"C\" && cat \""
                        + input.getAbsolutePath()
                        + "\" | sort > \""
                        + sorted.getAbsolutePath()
                        + "\""
            };

            Process p = null;
            try {
                p = Runtime.getRuntime().exec(command);
                int retCode = p.waitFor();
                if (retCode != 0) {
                    throw new Exception("Command failed with return code " + retCode);
                }
                p = null;
            } finally {
                if (p != null) {
                    p.destroy();
                }
            }

            // sort the data
            Sorter<Tuple2<String, String[]>> sorter = null;
            BufferedReader reader = null;
            BufferedReader verifyReader = null;
            MemoryManager mm = null;

            try (IOManager ioMan = new IOManagerAsync()) {
                mm = MemoryManagerBuilder.newBuilder().setMemorySize(1024 * 1024).build();

                TupleTypeInfo<Tuple2<String, String[]>> typeInfo =
                        (TupleTypeInfo<Tuple2<String, String[]>>)
                                new TypeHint<Tuple2<String, String[]>>() {}.getTypeInfo();

                TypeSerializer<Tuple2<String, String[]>> serializer =
                        typeInfo.createSerializer(new ExecutionConfig());
                TypeComparator<Tuple2<String, String[]>> comparator =
                        typeInfo.createComparator(
                                new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

                reader = new BufferedReader(new FileReader(input));
                MutableObjectIterator<Tuple2<String, String[]>> inputIterator =
                        new StringTupleReaderMutableObjectIterator(reader);

                sorter =
                        ExternalSorter.newBuilder(mm, new DummyInvokable(), serializer, comparator)
                                .maxNumFileHandles(4)
                                .enableSpilling(ioMan, 0.8f)
                                .memoryFraction(1.0)
                                .objectReuse(false)
                                .largeRecords(true)
                                .build(inputIterator);

                // use this part to verify that all if good when sorting in memory

                //				List<MemorySegment> memory = mm.allocatePages(new DummyInvokable(),
                // mm.computeNumberOfPages(1024*1024*1024));
                //				NormalizedKeySorter<Tuple2<String, String[]>> nks = new
                // NormalizedKeySorter<Tuple2<String,String[]>>(serializer, comparator, memory);
                //
                //				{
                //					Tuple2<String, String[]> wi = new Tuple2<String, String[]>("", new
                // String[0]);
                //					while ((wi = inputIterator.next(wi)) != null) {
                //						Assert.assertTrue(nks.write(wi));
                //					}
                //
                //					new QuickSort().sort(nks);
                //				}
                //
                //				MutableObjectIterator<Tuple2<String, String[]>> sortedData =
                // nks.getIterator();

                MutableObjectIterator<Tuple2<String, String[]>> sortedData = sorter.getIterator();
                reader.close();

                // verify
                verifyReader = new BufferedReader(new FileReader(sorted));
                MutableObjectIterator<Tuple2<String, String[]>> verifyIterator =
                        new StringTupleReaderMutableObjectIterator(verifyReader);

                Tuple2<String, String[]> next = new Tuple2<String, String[]>("", new String[0]);
                Tuple2<String, String[]> nextFromStratoSort =
                        new Tuple2<String, String[]>("", new String[0]);

                int num = 0;

                while ((next = verifyIterator.next(next)) != null) {
                    num++;

                    nextFromStratoSort = sortedData.next(nextFromStratoSort);
                    Assert.assertNotNull(nextFromStratoSort);

                    Assert.assertEquals(next.f0, nextFromStratoSort.f0);
                    Assert.assertArrayEquals(next.f1, nextFromStratoSort.f1);
                }

                Assert.assertNull(sortedData.next(nextFromStratoSort));
                Assert.assertEquals(numStrings, num);

            } finally {
                if (reader != null) {
                    reader.close();
                }
                if (verifyReader != null) {
                    verifyReader.close();
                }
                if (sorter != null) {
                    sorter.close();
                }
                if (mm != null) {
                    mm.shutdown();
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (input != null) {
                input.delete();
            }
            if (sorted != null) {
                sorted.delete();
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private static final class StringReaderMutableObjectIterator
            implements MutableObjectIterator<String> {

        private final BufferedReader reader;

        public StringReaderMutableObjectIterator(BufferedReader reader) {
            this.reader = reader;
        }

        @Override
        public String next(String reuse) throws IOException {
            return reader.readLine();
        }

        @Override
        public String next() throws IOException {
            return reader.readLine();
        }
    }

    private static final class StringTupleReaderMutableObjectIterator
            implements MutableObjectIterator<Tuple2<String, String[]>> {

        private final BufferedReader reader;

        public StringTupleReaderMutableObjectIterator(BufferedReader reader) {
            this.reader = reader;
        }

        @Override
        public Tuple2<String, String[]> next(Tuple2<String, String[]> reuse) throws IOException {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }

            String[] parts = line.split(" ");
            reuse.f0 = parts[0];
            reuse.f1 = parts;
            return reuse;
        }

        @Override
        public Tuple2<String, String[]> next() throws IOException {
            return next(new Tuple2<String, String[]>());
        }
    }

    // --------------------------------------------------------------------------------------------

    private File generateFileWithStrings(int numStrings, String prefix) throws IOException {
        final Random rnd = new Random(SEED);

        final StringBuilder bld = new StringBuilder();
        final int resetValue = prefix.length();

        bld.append(prefix);

        File f = File.createTempFile("strings", "txt");
        BufferedWriter wrt = null;
        try {
            wrt = new BufferedWriter(new FileWriter(f));

            for (int i = 0; i < numStrings; i++) {
                bld.setLength(resetValue);

                int len = rnd.nextInt(20) + 300;
                for (int k = 0; k < len; k++) {
                    char c = (char) (rnd.nextInt(80) + 40);
                    bld.append(c);
                }

                String str = bld.toString();
                wrt.write(str);
                wrt.newLine();
            }
        } finally {
            wrt.close();
        }

        return f;
    }

    private File generateFileWithStringTuples(int numStrings, String prefix) throws IOException {
        final Random rnd = new Random(SEED);

        final StringBuilder bld = new StringBuilder();

        File f = File.createTempFile("strings", "txt");
        BufferedWriter wrt = null;
        try {
            wrt = new BufferedWriter(new FileWriter(f));

            for (int i = 0; i < numStrings; i++) {
                bld.setLength(0);

                int numComps = rnd.nextInt(5) + 1;

                for (int z = 0; z < numComps; z++) {
                    if (z > 0) {
                        bld.append(' ');
                    }
                    bld.append(prefix);

                    int len = rnd.nextInt(20) + 10;
                    for (int k = 0; k < len; k++) {
                        char c = (char) (rnd.nextInt(80) + 40);
                        bld.append(c);
                    }
                }

                String str = bld.toString();

                wrt.write(str);
                wrt.newLine();
            }
        } finally {
            wrt.close();
        }

        return f;
    }

    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) {
        new MassiveStringSorting().testStringSorting();
        new MassiveStringSorting().testStringTuplesSorting();
    }
}
