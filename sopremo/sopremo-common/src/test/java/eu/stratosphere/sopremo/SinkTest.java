package eu.stratosphere.sopremo;

import org.junit.*;
import eu.stratosphere.pact.common.plan.PactModule;
import static org.junit.Assert.*;

/**
 * The class <code>SinkTest</code> contains tests for the class <code>{@link Sink}</code>.
 *
 * @generatedBy CodePro at 7/12/11 2:23 PM
 * @author arv
 * @version $Revision: 1.0 $
 */
public class SinkTest {
	/**
	 * Run the Sink(PersistenceType,String,JsonStream) constructor test.
	 *
	 * @throws Exception
	 *
	 * @generatedBy CodePro at 7/12/11 2:23 PM
	 */
	@Test
	public void testSink_1()
		throws Exception {
		PersistenceType type = PersistenceType.ADHOC;
		String outputName = "";
		JsonStream input = new ElementaryOperator(new JsonStream[] {});

		Sink result = new Sink(type, outputName, input);

		// add additional test code here
		assertNotNull(result);
		assertEquals("Sink []", result.toString());
		assertEquals("", result.getOutputName());
		assertEquals("Sink", result.getName());
	}

	/**
	 * Run the PactModule asPactModule(EvaluationContext) method test.
	 *
	 * @throws Exception
	 *
	 * @generatedBy CodePro at 7/12/11 2:23 PM
	 */
	@Test
	public void testAsPactModule_1()
		throws Exception {
		Sink fixture = new Sink(PersistenceType.ADHOC, "", new ElementaryOperator(new JsonStream[] {}));
		EvaluationContext context = new EvaluationContext();

		PactModule result = fixture.asPactModule(context);

		// add additional test code here
		assertNotNull(result);
		assertEquals("DataSinkContract []                     \n│                                       \nInput 0                                 \n", result.toString());
		assertEquals("Sink []", result.getName());
	}

	/**
	 * Run the String getOutputName() method test.
	 *
	 * @throws Exception
	 *
	 * @generatedBy CodePro at 7/12/11 2:23 PM
	 */
	@Test
	public void testGetOutputName_1()
		throws Exception {
		Sink fixture = new Sink(PersistenceType.ADHOC, "", new ElementaryOperator(new JsonStream[] {}));

		String result = fixture.getOutputName();

		// add additional test code here
		assertEquals("", result);
	}

	/**
	 * Run the PersistenceType getType() method test.
	 *
	 * @throws Exception
	 *
	 * @generatedBy CodePro at 7/12/11 2:23 PM
	 */
	@Test
	public void testGetType_1()
		throws Exception {
		Sink fixture = new Sink(PersistenceType.ADHOC, "", new ElementaryOperator(new JsonStream[] {}));

		PersistenceType result = fixture.getType();

		// add additional test code here
		assertNotNull(result);
		assertEquals("ADHOC", result.name());
		assertEquals("ADHOC", result.toString());
		assertEquals(1, result.ordinal());
	}

	/**
	 * Run the String toString() method test.
	 *
	 * @throws Exception
	 *
	 * @generatedBy CodePro at 7/12/11 2:23 PM
	 */
	@Test
	public void testToString_1()
		throws Exception {
		Sink fixture = new Sink(PersistenceType.ADHOC, "", new ElementaryOperator(new JsonStream[] {}));

		String result = fixture.toString();

		// add additional test code here
		assertEquals("Sink []", result);
	}

	/**
	 * Perform pre-test initialization.
	 *
	 * @throws Exception
	 *         if the initialization fails for some reason
	 *
	 * @generatedBy CodePro at 7/12/11 2:23 PM
	 */
	@Before
	public void setUp()
		throws Exception {
		// add additional set up code here
	}

	/**
	 * Perform post-test clean-up.
	 *
	 * @throws Exception
	 *         if the clean-up fails for some reason
	 *
	 * @generatedBy CodePro at 7/12/11 2:23 PM
	 */
	@After
	public void tearDown()
		throws Exception {
		// Add additional tear down code here
	}

	/**
	 * Launch the test.
	 *
	 * @param args the command line arguments
	 *
	 * @generatedBy CodePro at 7/12/11 2:23 PM
	 */
	public static void main(String[] args) {
		new org.junit.runner.JUnitCore().run(SinkTest.class);
	}
}