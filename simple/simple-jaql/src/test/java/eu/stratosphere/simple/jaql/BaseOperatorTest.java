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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.SopremoPlan;

@RunWith(Parameterized.class)
public class BaseOperatorTest {
	private File scriptPath;

	public BaseOperatorTest(File scriptPath) {
		this.scriptPath = scriptPath;
	}

	@Test
	public void testParser() throws FileNotFoundException, IOException {
		SopremoPlan plan = null;

		try {		
			plan = new QueryParser().tryParse(new FileInputStream(this.scriptPath));
		}
		catch (RecognitionException e) {
			Assert.fail(String.format("could not parse %s @ token %s in line %s: %s", scriptPath.getName(), e.token, e.line, e));
		}
		
		Assert.assertNotNull("could not parse " + scriptPath.getName(), plan);
		System.out.println(plan);
	}

	@Parameters
	public static List<Object[]> baseScripts() throws IOException {
		ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		URL scriptDir = BaseOperatorTest.class.getClassLoader().getResource("baseScripts");
		for (File script : new File(scriptDir.getPath()).listFiles())
			parameters.add(new Object[] { script });
		return parameters;
	}
}
