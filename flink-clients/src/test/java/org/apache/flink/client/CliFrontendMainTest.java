package org.apache.flink.client;


import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.MainOptions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.net.MalformedURLException;

import static org.apache.flink.client.CliFrontendTestUtils.getTestJarPath;
import static org.junit.Assert.assertEquals;

public class CliFrontendMainTest {


	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
		CliFrontendTestUtils.clearGlobalConfiguration();
	}

	@Test
	public void simpleParse() throws FileNotFoundException, MalformedURLException, CliArgsException {
			String[] parameters = {"--configDir", "expectedConfigDirectory", "run", getTestJarPath()};
			MainOptions options = CliFrontendParser.parseMainCommand(parameters);
			assertEquals("expectedConfigDirectory", options.getConfigDir());
	}

	@Test
	public void dropAfterAction() throws FileNotFoundException, MalformedURLException, CliArgsException {
		String[] parameters = {"--configDir", "expectedConfigDirectory", "run", getTestJarPath(), "--configDir", "notExpected"};
		MainOptions options = CliFrontendParser.parseMainCommand(parameters);
		assertEquals("expectedConfigDirectory", options.getConfigDir());
		assertEquals(options.getOptions().length, 1);
	}
}
