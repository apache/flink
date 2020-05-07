package org.apache.flink.formats.json;

import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * ITCase to test json format for {@link JsonFileSystemFormatFactory}.
 */
@RunWith(Parameterized.class)
public class FsBatchJsonSinkITCase extends BatchFileSystemITCaseBase {

	private final boolean configure;

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(false, true);
	}

	public FsBatchJsonSinkITCase(boolean configure) {
		this.configure = configure;
	}

	@Override
	public String[] formatProperties() {
		List<String> ret = new ArrayList<>();
		ret.add("'format'='json'");
		if (configure) {
			ret.add("'format.fail-on-missing-field'='true'");
			ret.add("'format.ignore-parse-errors'='false'");
		}
		return ret.toArray(new String[0]);
	}
}
