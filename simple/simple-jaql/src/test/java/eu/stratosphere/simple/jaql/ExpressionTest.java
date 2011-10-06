package eu.stratosphere.simple.jaql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.antlr.runtime.RecognitionException;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.SopremoPlan;

@RunWith(Parameterized.class)
public class ExpressionTest {
	private File scriptPath;

	public ExpressionTest(File scriptPath) {
		this.scriptPath = scriptPath;
	}

	public static JsonNode udfTest(JsonNode... nodes) {
		return nodes[0];
	}
	
	@Test
	public void testParser() throws FileNotFoundException, IOException {
		SopremoPlan plan = null;

		try {
			plan = new QueryParser().tryParse(new FileInputStream(this.scriptPath));
		} catch (RecognitionException e) {
			Assert.fail(String.format("could not parse %s @ token %s in line %s: %s", this.scriptPath.getName(),
				e.token, e.line, e));
		}

		Assert.assertNotNull("could not parse " + this.scriptPath.getName(), plan);
	}

	@Parameters
	public static List<Object[]> cleansScripts() {
		ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		URL scriptDir = ExpressionTest.class.getClassLoader().getResource("expressionScripts");
		for (File script : new File(scriptDir.getPath()).listFiles())
			parameters.add(new Object[] { script });
		return parameters;
	}
}
