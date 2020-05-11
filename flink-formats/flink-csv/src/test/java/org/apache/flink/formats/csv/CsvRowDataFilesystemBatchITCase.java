package org.apache.flink.formats.csv;

import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.collection.JavaConverters;

/**
 * ITCase to test csv format for {@link CsvRowDataFileSystemFormatFactory} in batch mode.
 */
@RunWith(Enclosed.class)
public class CsvRowDataFilesystemBatchITCase {

	/**
	 * General IT cases for CsvRowDataFilesystem in batch mode.
	 */
	public static class GeneralCsvRowDataFilesystemBatchITCase extends BatchFileSystemITCaseBase {

		@Override
		public String[] formatProperties() {
			List<String> ret = new ArrayList<>();
			ret.add("'format'='csv'");
			ret.add("'format.field-delimiter'=';'");
			ret.add("'format.quote-character'='#'");
			return ret.toArray(new String[0]);
		}
	}

	/**
	 * Enriched IT cases that including testParseError and testEscapeChar for CsvRowDataFilesystem in batch mode.
	 */
	public static class EnrichedCsvRowDataFilesystemBatchITCase extends BatchFileSystemITCaseBase {

		@Override
		public String[] formatProperties() {
			List<String> ret = new ArrayList<>();
			ret.add("'format'='csv'");
			ret.add("'format.ignore-parse-errors'='true'");
			ret.add("'format.escape-character'='\t'");
			return ret.toArray(new String[0]);
		}

		@Test
		public void testParseError() throws Exception {
			String path = new URI(resultPath()).getPath();
			new File(path).mkdirs();
			File file = new File(path, "test_file");
			file.createNewFile();
			FileUtils.writeFileUtf8(file,
				"x5,5,1,1\n" +
					"x5,5,2,2,2\n" +
					"x5,5,1,1");

			check("select * from nonPartitionedTable",
				JavaConverters.asScalaIteratorConverter(Arrays.asList(
					Row.of("x5,5,1,1"),
					Row.of("x5,5,1,1")).iterator()).asScala().toSeq());
		}

		@Test
		public void testEscapeChar() throws Exception {
			String path = new URI(resultPath()).getPath();
			new File(path).mkdirs();
			File file = new File(path, "test_file");
			file.createNewFile();
			FileUtils.writeFileUtf8(file,
				"x5,\t\n5,1,1\n" +
					"x5,\t5,2,2");

			check("select * from nonPartitionedTable",
				JavaConverters.asScalaIteratorConverter(Arrays.asList(
					Row.of("x5,5,1,1"),
					Row.of("x5,5,2,2")).iterator()).asScala().toSeq());
		}
	}
}
