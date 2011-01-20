package eu.stratosphere.test.inputformat;

import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.relational.util.IntTupleDataInFormat;
import eu.stratosphere.pact.example.relational.util.Tuple;

@RunWith(PowerMockRunner.class)
public class IntTupleDataInFormatTest {

	KeyValuePair<PactInteger, Tuple> pair = new KeyValuePair<PactInteger, Tuple>();

	@Before
	public void setup() {
		initMocks(this);
	}

	@Test
	public void testReadLineKeyValuePairOfPactIntegerTupleByteArray() {
		IntTupleDataInFormat toTest = new IntTupleDataInFormat();
		String testString = "1| TestValue |";
		byte[] line = testString.getBytes();
		toTest.readLine(pair, line);
		Assert.assertEquals(2, pair.getValue().getNumberOfColumns());
		Assert.assertEquals(new PactInteger(1), pair.getKey());
		Assert.assertEquals(testString, pair.getValue().toString());
		
		testString = "16572| TestValue |";
		line = testString.getBytes();
		toTest.readLine(pair, line);
		Assert.assertEquals(2, pair.getValue().getNumberOfColumns());
		Assert.assertEquals(new PactInteger(16572), pair.getKey());
		Assert.assertEquals(testString, pair.getValue().toString());
		
		testString = "0| TestValue |";
		line = testString.getBytes();
		toTest.readLine(pair, line);
		Assert.assertEquals(2, pair.getValue().getNumberOfColumns());
		Assert.assertEquals(new PactInteger(0), pair.getKey());
		Assert.assertEquals(testString, pair.getValue().toString());

		testString = "-3653| TestValue |";
		line = testString.getBytes();
		toTest.readLine(pair, line);
		Assert.assertEquals(2, pair.getValue().getNumberOfColumns());
		Assert.assertEquals(new PactInteger(-3653), pair.getKey());
		Assert.assertEquals(testString, pair.getValue().toString());
	}
}
