package eu.stratosphere.test.hadoopcompatibility;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.hadoopcompatibility.example.WordCountWithHadoopOutputFormat;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.TestBase2;

/**
 * test the hadoop inputformat and outputformat for stratosphere
 */
public class HadoopInputOutputTest extends TestBase2 {
	protected String textPath;
	protected String resultPath;
	protected String counts;

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
		counts = WordCountData.COUNTS.replaceAll(" ", "\t");
	}

	@Override
	protected Plan getTestJob() {
		//WordCountWithHadoopOutputFormat takes hadoop TextInputFormat as input and output file in hadoop TextOutputFormat
		WordCountWithHadoopOutputFormat wc = new WordCountWithHadoopOutputFormat();
		return wc.getPlan("1", textPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results, append /1 to resultPath due to the generated _temproray file.
		compareResultsByLinesInMemory(counts, resultPath + "/1");
	}
}
