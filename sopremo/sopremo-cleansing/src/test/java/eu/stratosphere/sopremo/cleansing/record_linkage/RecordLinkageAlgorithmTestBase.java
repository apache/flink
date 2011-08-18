package eu.stratosphere.sopremo.cleansing.record_linkage;

import org.junit.Ignore;

/**
 * @author Arvid Heise
 */
@Ignore
public class RecordLinkageAlgorithmTestBase {

	/**
	 * 
	 */
	protected static String[][] BlockingKeys = new String[][] {
		{ "first name", "firstName" },
		{ "last name", "lastName" },
		{ "age", "age" },
	};

	/**
	 * 
	 */
	protected static String[][][] CombinedBlockingKeys = new String[][][] {
		{ { BlockingKeys[0][0] }, { BlockingKeys[0][1] } },
		{ { BlockingKeys[1][0] }, { BlockingKeys[1][1] } },
		// { { BlockingKeys[2][0] }, { BlockingKeys[2][1] } },

		{ { BlockingKeys[0][0], BlockingKeys[1][0] }, { BlockingKeys[0][1], BlockingKeys[1][1] } },
		// { { BlockingKeys[0][0], BlockingKeys[2][0] }, { BlockingKeys[0][1], BlockingKeys[2][1] } },
		// { { BlockingKeys[1][0], BlockingKeys[2][0] }, { BlockingKeys[1][1], BlockingKeys[2][1] } },

		{ { BlockingKeys[0][0], BlockingKeys[1][0], BlockingKeys[2][0] },
		{ BlockingKeys[0][1], BlockingKeys[1][1], BlockingKeys[2][1] } },
	};
}
