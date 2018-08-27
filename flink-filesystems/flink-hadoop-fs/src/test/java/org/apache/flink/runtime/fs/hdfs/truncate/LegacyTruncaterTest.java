package org.apache.flink.runtime.fs.hdfs.truncate;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


/**
 * Tests that validate truncate logic and all recovery scenarios.
 */
public class LegacyTruncaterTest {

	private static final String TEST_IN_PROGRESS_FILE_PREFIX = ".inprogress.foo.bar";

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	private Path createFileWithContent(String fileName, String content) throws IOException {
		final File file = tempFolder.newFile(fileName);
		try (PrintStream out = new PrintStream(new FileOutputStream(file))) {
			out.print(content);
		}
		return new Path(file.getPath());
	}

	@Test
	public void testTruncateFile() throws IOException {
		final String testContent = "Apache Flink - Stateful Computations over Data Streams";
		final long truncateLength = 12;
		final String expectedContent = "Apache Flink";

		final Path testFilePath = createFileWithContent(TEST_IN_PROGRESS_FILE_PREFIX, testContent);
		final Path truncatedFilePath = LegacyTruncater.truncatedFile(testFilePath);
		final FileSystem fs = Mockito.spy(FileSystem.get(new Configuration()));
		final LegacyTruncater truncater = Mockito.spy(new LegacyTruncater(fs));
		truncater.truncate(testFilePath, truncateLength);

		final String result = FileUtils.readFileToString(new File(testFilePath.toString()));
		assertEquals(expectedContent, result);
		verify(truncater).copyFileContent(testFilePath, truncatedFilePath, truncateLength);
		verify(fs).delete(testFilePath, false);
		verify(fs).rename(truncatedFilePath, testFilePath);
	}

	@Test
	public void testRecoveryAfterFailOnCopingStage() throws IOException {
		final String originalFileContent = "Apache Flink - Stateful Computations over Data Streams";
		final String brokenTruncatedFileContent = "Apache Fl";
		final long truncateLength = 12;
		final String expectedContent = "Apache Flink";

		final Path originalFilePath = createFileWithContent(TEST_IN_PROGRESS_FILE_PREFIX, originalFileContent);
		final Path truncatedFilePath = LegacyTruncater.truncatedFile(originalFilePath);
		createFileWithContent(truncatedFilePath.getName(), brokenTruncatedFileContent);

		final FileSystem fs = Mockito.spy(FileSystem.get(new Configuration()));
		final LegacyTruncater truncater = Mockito.spy(new LegacyTruncater(fs));
		truncater.truncate(originalFilePath, truncateLength);

		final String result = FileUtils.readFileToString(new File(originalFilePath.toString()));
		assertEquals(expectedContent, result);
		verify(truncater).copyFileContent(originalFilePath, truncatedFilePath, truncateLength);
		verify(fs).delete(originalFilePath, false);
		verify(fs).rename(truncatedFilePath, originalFilePath);
	}

	@Test
	public void testRecoveryAfterFailRenamingStage() throws IOException {
		final String truncatedFileContent = "Apache Flink";
		final long truncateLength = 12;
		final String expectedContent = "Apache Flink";

		final FileSystem fs = Mockito.spy(FileSystem.get(new Configuration()));

		final Path originalFilePath = new Path(tempFolder.getRoot().getPath(),
			new Path(TEST_IN_PROGRESS_FILE_PREFIX));

		final Path truncatedFilePath = LegacyTruncater.truncatedFile(originalFilePath);
		createFileWithContent(truncatedFilePath.getName(), truncatedFileContent);
		final LegacyTruncater truncater = Mockito.spy(new LegacyTruncater(fs));

		truncater.truncate(originalFilePath, truncateLength);

		final String result = FileUtils.readFileToString(new File(originalFilePath.toString()));
		assertEquals(expectedContent, result);
		verify(truncater, never()).copyFileContent(any(Path.class), any(Path.class), anyLong());
		verify(fs, never()).delete(any(Path.class), anyBoolean());
		verify(fs).rename(truncatedFilePath, originalFilePath);
	}

	@Test
	public void testTruncateZeroLength() throws IOException {
		final String originalFileContent = "Apache Flink";
		final long truncateLength = 0;
		final String expectedContent = "";
		final FileSystem fs = Mockito.spy(FileSystem.get(new Configuration()));

		final Path originalFilePath = createFileWithContent(TEST_IN_PROGRESS_FILE_PREFIX, originalFileContent);
		final Path truncatedFilePath = LegacyTruncater.truncatedFile(originalFilePath);
		final LegacyTruncater truncater = Mockito.spy(new LegacyTruncater(fs));

		truncater.truncate(originalFilePath, truncateLength);

		final String result = FileUtils.readFileToString(new File(originalFilePath.toString()));
		assertEquals(expectedContent, result);
		// Verify that empty file was recreated without intermediate copying stages
		verify(truncater).touch(originalFilePath);
		verify(truncater, never()).copyFileContent(any(Path.class), any(Path.class), anyLong());
	}
}
