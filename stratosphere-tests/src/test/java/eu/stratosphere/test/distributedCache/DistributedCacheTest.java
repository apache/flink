package eu.stratosphere.test.distributedCache;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;


/**
 * Test the distributed cache via using the cache file to do a selection on the input
 */
public class DistributedCacheTest extends TestBase2 {

	public static final String cacheData = "machen\n" + "zeit\n" + "heerscharen\n" + "keiner\n" + "meine\n"
		+ "fuehr\n" + "triumph\n" + "kommst\n" + "frei\n" + "schaffen\n" + "gesinde\n"
		+ "langbeinigen\n" + "schalk\n" + "besser\n" + "solang\n" + "meer\n" + "fragst\n"
		+ "gabriel\n" + "selbst\n" + "bin\n" + "sich\n" + "du\n" + "sogar\n" + "geht\n"
		+ "immer\n" + "mensch\n" + "befestigt\n" + "lebt\n" + "mag\n" + "engeln\n" + "breiten\n"
		+ "blitzendes\n" + "tags\n" + "sie\n" + "plagen\n" + "allzu\n" + "meisten\n" + "o\n"
		+ "pfade\n" + "kennst\n" + "nichts\n" + "gedanken\n" + "befriedigt\n" + "mich\n" + "s\n"
		+ "es\n" + "verneinen\n" + "er\n" + "gleich\n" + "baeumchen\n" + "donnergang\n";

	public static final String selectedCounts = "machen 1\n" + "zeit 2\n" + "heerscharen 1\n" + "keiner 2\n" + "meine 3\n"
		+ "fuehr 1\n" + "triumph 1\n" + "kommst 1\n" + "frei 1\n" + "schaffen 1\n" + "gesinde 1\n"
		+ "langbeinigen 1\n" + "schalk 1\n" + "besser 1\n" + "solang 1\n" + "meer 4\n" + "fragst 1\n"
		+ "gabriel 1\n" + "selbst 2\n" + "bin 1\n" + "sich 7\n" + "du 11\n" + "sogar 1\n" + "geht 1\n"
		+ "immer 4\n" + "mensch 2\n" + "befestigt 1\n" + "lebt 2\n" + "mag 3\n" + "engeln 2\n" + "breiten 1\n"
		+ "blitzendes 1\n" + "tags 1\n" + "sie 2\n" + "plagen 2\n" + "allzu 1\n" + "meisten 1\n" + "o 1\n"
		+ "pfade 1\n" + "kennst 1\n" + "nichts 3\n" + "gedanken 1\n" + "befriedigt 1\n" + "mich 6\n" + "s 3\n"
		+ "es 8\n" + "verneinen 1\n" + "er 13\n" + "gleich 1\n" + "baeumchen 1\n" + "donnergang 1\n";

	protected String textPath;
	protected String cachePath;
	protected String resultPath;

	public static class TokenizeLine extends MapFunction {
		private static final long serialVersionUID = 1L;

		private Set<String> stringList = new HashSet<String>();
		@Override
		public void open(eu.stratosphere.configuration.Configuration conf) {

			File file = getRuntimeContext().getDistributedCache().getFile("cache_test");
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				while ((tempString = reader.readLine()) != null) {
					stringList.add(tempString);
				}
				reader.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void map(Record record, Collector<Record> collector) {
			String line = record.getField(0, StringValue.class).getValue();
			String [] element = line.split(" ");
			String word = element[0];
			int count = Integer.parseInt(element[1]);
			if (stringList.contains(word)) {
				collector.collect(new Record(new StringValue(word), new IntValue(count)));
			}
		}
	}

	public Plan getPlan(int numSubTasks, String dataInput, String output) {


		// input is {word, count} pair
		FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput, "Input Lines");

		//do a selection using cached file
		MapOperator mapper = MapOperator.builder(new TokenizeLine())
			.input(source)
			.name("Tokenize Lines")
			.build();

		FileDataSink out = new FileDataSink(new CsvOutputFormat(), output, mapper, "Selection");
		CsvOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(StringValue.class, 0)
			.field(IntValue.class, 1);

		Plan plan = new Plan(out, "Distributed Cache");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}


	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("count.txt", WordCountData.COUNTS);
		cachePath = createTempFile("cache.txt", cacheData);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		Plan plan =  getPlan(1 , textPath, resultPath);
		plan.registerCachedFile(cachePath, "cache_test");
		return plan;
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results
		compareResultsByLinesInMemory(selectedCounts, resultPath);
	}
}
