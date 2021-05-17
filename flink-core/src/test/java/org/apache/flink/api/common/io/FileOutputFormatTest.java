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

package org.apache.flink.api.common.io;

import org.apache.flink.api.common.io.FileOutputFormat.OutputDirectoryMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.IntValue;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;

public class FileOutputFormatTest {

    @Test
    public void testCreateNonParallelLocalFS() throws IOException {

        File tmpOutPath = File.createTempFile("fileOutputFormatTest", "Test1");
        File tmpOutFile = new File(tmpOutPath.getAbsolutePath() + "/1");

        String tmpFilePath = tmpOutPath.toURI().toString();

        // check fail if file exists
        DummyFileOutputFormat dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
            fail();
        } catch (Exception e) {
            // exception expected
        }
        tmpOutPath.delete();

        // check fail if directory exists
        Assert.assertTrue("Directory could not be created.", tmpOutPath.mkdir());

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
            fail();
        } catch (Exception e) {
            // exception expected
        }
        tmpOutPath.delete();

        // check success
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isFile());
        tmpOutPath.delete();

        // check fail for path with tailing '/'
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath + "/"));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isFile());
        tmpOutPath.delete();

        // ----------- test again with always directory mode

        // check fail if file exists
        tmpOutPath.createNewFile();

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
            fail();
        } catch (Exception e) {
            // exception expected
        }
        tmpOutPath.delete();

        // check success if directory exists
        Assert.assertTrue("Directory could not be created.", tmpOutPath.mkdir());

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        (new File(tmpOutPath.getAbsoluteFile() + "/1")).delete();

        // check custom file name inside directory if directory exists
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);
        dfof.testFileName = true;
        Configuration c = new Configuration();
        dfof.configure(c);

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        File customOutFile = new File(tmpOutPath.getAbsolutePath() + "/fancy-1-0.avro");
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(customOutFile.exists() && customOutFile.isFile());
        customOutFile.delete();

        // check fail if file in directory exists
        // create file for test
        customOutFile = new File(tmpOutPath.getAbsolutePath() + "/1");
        customOutFile.createNewFile();

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
            fail();
        } catch (Exception e) {
            // exception expected
        }
        (new File(tmpOutPath.getAbsoluteFile() + "/1")).delete();
        tmpOutPath.delete();

        // check success if no file exists
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        (new File(tmpOutPath.getAbsoluteFile() + "/1")).delete();
        tmpOutPath.delete();

        // check success for path with tailing '/'
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath + '/'));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        (new File(tmpOutPath.getAbsoluteFile() + "/1")).delete();
        tmpOutPath.delete();
    }

    @Test
    public void testCreateParallelLocalFS() throws IOException {

        File tmpOutPath = null;
        File tmpOutFile = null;

        tmpOutPath = File.createTempFile("fileOutputFormatTest", "Test1");
        tmpOutFile = new File(tmpOutPath.getAbsolutePath() + "/1");

        String tmpFilePath = tmpOutPath.toURI().toString();

        // check fail if file exists
        DummyFileOutputFormat dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
            fail();
        } catch (Exception e) {
            // exception expected
        }
        tmpOutPath.delete();

        // check success if directory exists
        Assert.assertTrue("Directory could not be created.", tmpOutPath.mkdir());

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();

        // check fail if file in directory exists
        tmpOutPath.mkdir();
        tmpOutFile.createNewFile();

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
            fail();
        } catch (Exception e) {
            // exception expected
        }
        tmpOutFile.delete();
        tmpOutPath.delete();

        // check success if no file exists
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();

        // check success for path with tailing '/'
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath + "/"));
        dfof.setWriteMode(WriteMode.NO_OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();
    }

    @Test
    public void testOverwriteNonParallelLocalFS() throws IOException {

        File tmpOutPath = null;
        File tmpOutFile = null;

        tmpOutPath = File.createTempFile("fileOutputFormatTest", "Test1");
        tmpOutFile = new File(tmpOutPath.getAbsolutePath() + "/1");

        String tmpFilePath = tmpOutPath.toURI().toString();

        // check success if file exists
        DummyFileOutputFormat dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isFile());

        // check success if directory exists
        tmpOutPath.delete();
        Assert.assertTrue("Directory could not be created.", tmpOutPath.mkdir());

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isFile());
        tmpOutPath.delete();

        // check success
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isFile());
        tmpOutPath.delete();

        // check fail for path with tailing '/'
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath + "/"));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isFile());
        tmpOutPath.delete();

        // ----------- test again with always directory mode

        // check success if file exists
        tmpOutPath.createNewFile();

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());

        tmpOutFile.delete();
        tmpOutPath.delete();

        // check success if directory exists
        Assert.assertTrue("Directory could not be created.", tmpOutPath.mkdir());

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutPath.delete();
        tmpOutFile.delete();

        // check success if file in directory exists
        tmpOutPath.mkdir();
        tmpOutFile.createNewFile();

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutPath.delete();
        tmpOutFile.delete();

        // check success if no file exists
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();

        // check success for path with tailing '/'
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath + "/"));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.ALWAYS);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 1);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();
    }

    @Test
    public void testOverwriteParallelLocalFS() throws IOException {

        File tmpOutPath = null;
        File tmpOutFile = null;

        tmpOutPath = File.createTempFile("fileOutputFormatTest", "Test1");
        tmpOutFile = new File(tmpOutPath.getAbsolutePath() + "/1");

        String tmpFilePath = tmpOutPath.toURI().toString();

        // check success if file exists
        DummyFileOutputFormat dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();

        // check success if directory exists
        Assert.assertTrue("Directory could not be created.", tmpOutPath.mkdir());

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();

        // check success if file in directory exists
        tmpOutPath.mkdir();
        tmpOutFile.createNewFile();

        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        (new File(tmpOutPath.getAbsoluteFile() + "/1")).delete();
        tmpOutPath.delete();

        // check success if no file exists
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();

        // check success for path with tailing '/'
        dfof = new DummyFileOutputFormat();
        dfof.setOutputFilePath(new Path(tmpFilePath + "/"));
        dfof.setWriteMode(WriteMode.OVERWRITE);
        dfof.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);

        dfof.configure(new Configuration());

        try {
            dfof.open(0, 2);
            dfof.close();
        } catch (Exception e) {
            fail();
        }
        Assert.assertTrue(tmpOutPath.exists() && tmpOutPath.isDirectory());
        Assert.assertTrue(tmpOutFile.exists() && tmpOutFile.isFile());
        tmpOutFile.delete();
        tmpOutPath.delete();
    }

    // -------------------------------------------------------------------------------------------

    public static class DummyFileOutputFormat extends FileOutputFormat<IntValue> {

        private static final long serialVersionUID = 1L;
        public boolean testFileName = false;

        @Override
        public void writeRecord(IntValue record) throws IOException {
            // DO NOTHING
        }

        @Override
        protected String getDirectoryFileName(int taskNumber) {
            if (testFileName) {
                return "fancy-" + (taskNumber + 1) + "-" + taskNumber + ".avro";
            } else {
                return super.getDirectoryFileName(taskNumber);
            }
        }
    }
}
