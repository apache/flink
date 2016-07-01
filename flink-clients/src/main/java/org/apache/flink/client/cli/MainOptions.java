package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import static org.apache.flink.client.cli.CliFrontendParser.CONFIGDIR_OPTION;

public class MainOptions {

	private final String configDir;

	private final Option[] options;

	public MainOptions(CommandLine line) throws CliArgsException {

		if (line.hasOption(CONFIGDIR_OPTION.getLongOpt())) {
			configDir = line.getOptionValue(CONFIGDIR_OPTION.getLongOpt());
		} else {
			configDir = null;
		}

		this.options = line.getOptions();
	}

	public Option[] getOptions() {
		return options == null ? new Option[0] : options;
	}


	public String getConfigDir(){
		return configDir;
	}
}
