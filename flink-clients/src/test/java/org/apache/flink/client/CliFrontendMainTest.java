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
	public void testMain() throws CliArgsException, FileNotFoundException, MalformedURLException {
			// test configure configDir
			{
				String[] parameters = {"--configDir", "expectedConfigDirectory", getTestJarPath()};
				MainOptions options = CliFrontendParser.parseMainCommand(parameters);
				assertEquals("expectedConfigDirectory", options.getConfigDir());
			}
	}

}
