/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.fs.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.util.CommonTestUtils;

/**
 * This class tests the functionality of the Local File System class. File/directory access, creation,
 * deletion, read, write.
 * 
 * @author casp
 */

public class LocalFileSystemTest {

	/**
	 * This test tests the functionality of the Local File System class.
	 */
	@Test
	public void testLocalFilesystem() {

		File tempdir = new File(CommonTestUtils.getTempDir() + File.separator
			+ CommonTestUtils.getRandomDirectoryName() + File.separator);

		// we must ensure, that this directory does NOT exist yet
		// probability is extremely low but just to make sure..
		while (tempdir.exists()) {
			tempdir = new File(CommonTestUtils.getTempDir() + File.separator + CommonTestUtils.getRandomDirectoryName()
				+ File.separator);
		}

		final File testfile1 = new File(tempdir.getAbsolutePath() + File.separator
			+ CommonTestUtils.getRandomFilename());

		final File testfile2 = new File(tempdir.getAbsolutePath() + File.separator
			+ CommonTestUtils.getRandomFilename());

		final Path pathtotestfile1 = new Path(testfile1.toURI().getPath());
		final Path pathtotestfile2 = new Path(testfile2.toURI().getPath());

		try {
			LocalFileSystem lfs = new LocalFileSystem();

			Path pathtotmpdir = new Path(tempdir.toURI().getPath());

			/*
			 * check that lfs can see/create/delete/read directories
			 */

			// check that dir is not existent yet
			assertFalse(lfs.exists(pathtotmpdir));
			tempdir.mkdirs();

			// check that local file system recognizes file..
			assertTrue(lfs.exists(pathtotmpdir));
			FileStatus localstatus1 = lfs.getFileStatus(pathtotmpdir);

			// check that lfs recognizes directory..
			assertTrue(localstatus1.isDir());

			// get status for files in this (empty) directory..
			FileStatus[] statusforfiles = lfs.listStatus(pathtotmpdir);

			// no files in there.. hence, must be zero
			assertTrue(statusforfiles.length == 0);

			// check that lfs can delete directory..
			lfs.delete(pathtotmpdir, true);

			// double check that directory is not existent anymore..
			assertFalse(lfs.exists(pathtotmpdir));
			assertFalse(tempdir.exists());

			// re-create directory..
			lfs.mkdirs(pathtotmpdir);

			// creation successful?
			assertTrue(tempdir.exists());

			/*
			 * check that lfs can create/read/write from/to files properly and read meta information..
			 */

			// create files.. one ""natively"", one using lfs
			FSDataOutputStream lfsoutput1 = lfs.create(pathtotestfile1, false);
			testfile2.createNewFile();

			// does lfs create files? does lfs recognize created files?
			assertTrue(testfile1.exists());
			assertTrue(lfs.exists(pathtotestfile2));

			// test that lfs can write to files properly
			final byte[] testbytes = { 1, 2, 3, 4, 5 };
			lfsoutput1.write(testbytes);
			lfsoutput1.close();

			assertEquals(testfile1.length(), 5l);

			FileInputStream fisfile1 = new FileInputStream(testfile1);
			byte[] testbytestest = new byte[5];
			fisfile1.read(testbytestest);
			fisfile1.close();

			// assertEquals is not able to compare arrays properly...
			// assertEquals(testbytes, testbytestest);
			assertTrue(Arrays.equals(testbytes, testbytestest));

			// does lfs see the correct file length?
			assertEquals(lfs.getFileStatus(pathtotestfile1).getLen(), testfile1.length());

			// as well, when we call the listStatus (that is intended for directories?)
			assertEquals(lfs.listStatus(pathtotestfile1)[0].getLen(), testfile1.length());

			// test that lfs can read files properly
			FileOutputStream fosfile2 = new FileOutputStream(testfile2);
			fosfile2.write(testbytes);
			fosfile2.close();

			testbytestest = new byte[5];
			FSDataInputStream lfsinput2 = lfs.open(pathtotestfile2);
			assertEquals(lfsinput2.read(testbytestest), 5);
			lfsinput2.close();
			assertTrue(Arrays.equals(testbytes, testbytestest));

			// does lfs see two files?
			assertEquals(lfs.listStatus(pathtotmpdir).length, 2);

			// do we get exactly one blocklocation per file? no matter what start and len we provide
			assertEquals(lfs.getFileBlockLocations(lfs.getFileStatus(pathtotestfile1), 0, 0).length, 1);

			/*
			 * can lfs delete files / directories?
			 */

			assertTrue(lfs.delete(pathtotestfile1, false));

			// and can lfs also delete directories recursively?
			assertTrue(lfs.delete(pathtotmpdir, true));

			assertTrue(!tempdir.exists());

		} catch (IOException e) {
			e.printStackTrace();
			fail(e.toString());

		} finally {
			// clean up!

			testfile1.delete();
			testfile2.delete();
			tempdir.delete();
		}

	}

}
