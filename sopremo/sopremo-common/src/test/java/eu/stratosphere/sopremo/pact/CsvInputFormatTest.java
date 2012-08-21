package eu.stratosphere.sopremo.pact;

import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class CsvInputFormatTest {
	/**
	 * Tests if a {@link TestPlan} can be executed.
	 * 
	 * @throws IOException
	 */
	@Test
	public void shouldParseCsv() throws IOException {
		final Source read =
			new Source(CsvInputFormat.class, this.getResource("CsvInputFormat/restaurant_short.csv"));

		final SopremoTestPlan testPlan = new SopremoTestPlan(read); // write

		testPlan.getExpectedOutput(0).
			addObject("id", "1", "name", "arnie morton's of chicago",
				"addr", "435 s. la cienega blv.", "city", "los angeles",
				"phone", "310/246-1501", "type", "american", "class", "'0'").
			addObject("id", "2", "name", "\"arnie morton's of chicago\"",
				"addr", "435 s. la cienega blv.", "city", "los,angeles",
				"phone", "310/246-1501", "type", "american", "class", "'0'").
			addObject("id", "3", "name", "arnie morton's of chicago",
				"addr", "435 s. la cienega blv.", "city", "los\nangeles", "phone", "310/246-1501",
				"type", "american", "class", "'0'");

		testPlan.run();
	}

	private String getResource(final String name) throws IOException {
		return JsonInputFormatTest.class.getClassLoader().getResources(name)
			.nextElement().toString();
	}
}
