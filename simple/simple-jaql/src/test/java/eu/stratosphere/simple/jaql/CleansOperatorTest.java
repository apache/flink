package eu.stratosphere.simple.jaql;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.simple.SimpleException;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.TextNode;

public class CleansOperatorTest {
	public void testParser(String script) throws FileNotFoundException, IOException {
		SopremoPlan plan = null;
		URL scriptURI = CleansOperatorTest.class.getClassLoader().getResource("cleansScripts/" + script);

		try {
			plan = new QueryParser().tryParse(new FileInputStream(scriptURI.getFile()));
			System.out.println(new QueryParser().toJavaString(new FileInputStream(scriptURI.getFile())));
		} catch (SimpleException e) {
			Assert.fail(String.format("could not parse %s: %s", scriptURI.getFile(), e.getMessage()));
		}

		Assert.assertNotNull("could not parse " + scriptURI.getFile(), plan);
	}

	@Test
	public void testScrub() throws FileNotFoundException, IOException {
		testParser("scrub.jaql");
	}

	@Test
	public void testFuse2() throws FileNotFoundException, IOException {
		testParser("fuse2.jaql");
	}

	@Test
	public void testFuse() throws FileNotFoundException, IOException {
		testParser("fuse.jaql");
	}

	@Test
	public void testExtract() throws FileNotFoundException, IOException {
		testParser("extract.jaql");
	}

	@Test
	public void testJoin() throws FileNotFoundException, IOException {
		testParser("join.jaql");
	}

	@Test
	public void testRL() throws FileNotFoundException, IOException {
		testParser("record_linkage.jaql");
	}

	public static TextNode normalizeName(TextNode name) {
		return name;
	}

	public static ArrayNode mergeAddresses(ArrayNode addresses) {
		return addresses;
	}

}
