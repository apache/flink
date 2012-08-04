package eu.stratosphere.sopremo.pact;

import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class N3InputFormatTest {


	@Test
	public void shouldParseN3() throws IOException {
		final Source read =
			new Source(N3InputFormat.class, this.getResource("N3InputFormat/skos_categories.n3"));

		final SopremoTestPlan testPlan = new SopremoTestPlan(read); // write

		testPlan.getExpectedOutput(0).
			addObject("s", "cat:Star_Trek", 
				"p", "skos:related",
				"o", "cat:Star_Trek_television_series").
			addObject("s", "cat:Star_Trek", 
				"p", "skos:prefLabel",
				"o", "\"Star Trek\"@en");

		testPlan.run();
	}

	private String getResource(final String name) throws IOException {
		return JsonInputFormatTest.class.getClassLoader().getResources(name)
			.nextElement().toString();
	}
}
