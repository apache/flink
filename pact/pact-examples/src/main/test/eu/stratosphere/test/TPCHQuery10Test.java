/**
 * 
 */
package eu.stratosphere.pact.example.relational;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.relational.TPCHQuery10.FilterLI;
import eu.stratosphere.pact.example.relational.TPCHQuery10.FilterO;
import eu.stratosphere.pact.example.relational.TPCHQuery10.GroupKey;
import eu.stratosphere.pact.example.relational.TPCHQuery10.JoinCOL;
import eu.stratosphere.pact.example.relational.TPCHQuery10.JoinNCOL;
import eu.stratosphere.pact.example.relational.TPCHQuery10.JoinOL;
import eu.stratosphere.pact.example.relational.TPCHQuery10.ProjectC;
import eu.stratosphere.pact.example.relational.TPCHQuery10.ProjectN;
import eu.stratosphere.pact.example.relational.util.Tuple;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;

/**
 * @author rico
 */
public class TPCHQuery10Test {

	private TPCHQuery10 query = new TPCHQuery10();

	@Mock
	OutputCollector<PactInteger, Tuple> collector;

	@Mock
	OutputCollector<GroupKey, Tuple> collector2;

	@Before
	public void setUp() {
		initMocks(this);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testFilterLi() {
		reset(collector);
		TPCHQuery10.FilterLI filterLi = new FilterLI();
		PactInteger key = new PactInteger(1);
		String extPrice = "100.00";
		String discount = "0.04";
		String returnflag = "R";
		Tuple value = createLIValue(extPrice, discount, returnflag, "1");

		filterLi.map(key, value, this.collector);

		verify(collector, times(1)).collect(any(PactInteger.class), any(Tuple.class));
		Assert.assertEquals(2, value.getNumberOfColumns());
		Assert.assertEquals(extPrice, value.getStringValueAt(0));
		Assert.assertEquals(discount, value.getStringValueAt(1));

		returnflag = "F";
		value = createLIValue(extPrice, discount, returnflag, "1");
		Tuple same = createLIValue(extPrice, discount, returnflag, "1");

		filterLi.map(key, value, collector);

		verify(collector, times(1)).collect(any(PactInteger.class), any(Tuple.class));
		verifySameTuples(value, same);
	}

	private void verifySameTuples(Tuple value, Tuple same) {
		Assert.assertEquals(same.getNumberOfColumns(), value.getNumberOfColumns());
		for (int i = 0; i < same.getNumberOfColumns(); i++) {
			Assert.assertEquals(same.getStringValueAt(i), value.getStringValueAt(i));
		}
	}

	private Tuple createLIValue(String extPrice, String discount, String returnflag, String orderkey) {
		Tuple value = new Tuple();
		value.addAttribute(orderkey);// orderkey
		value.addAttribute("2"); // partkey
		value.addAttribute("3");// suppkey
		value.addAttribute("4");// linenumber
		value.addAttribute("5");// quantity
		value.addAttribute(extPrice);// extendedprice
		value.addAttribute(discount); // discount
		value.addAttribute("8");// tax
		value.addAttribute(returnflag);// returnflag
		return value;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testFilterO() {
		TPCHQuery10.FilterO filterO = new FilterO();
		reset(collector);
		PactInteger key = new PactInteger(1);
		String orderkey = "1";
		String custkey = "2";
		String orderdate = "1995-02-01";
		Tuple value = createTupleO(orderkey, custkey, orderdate);
		filterO.map(key, value, this.collector);
		verify(collector, times(1)).collect(any(PactInteger.class), any(Tuple.class));
		Assert.assertEquals(1, value.getNumberOfColumns());
		Assert.assertEquals(custkey, value.getStringValueAt(0));

		orderdate = "1994-12-31";
		value = createTupleO(orderkey, custkey, orderdate);
		Tuple same = createTupleO(orderkey, custkey, orderdate);
		filterO.map(key, value, this.collector);
		verify(collector, times(1)).collect(any(PactInteger.class), any(Tuple.class));
		Assert.assertEquals(9, value.getNumberOfColumns());
		verifySameTuples(same, value);

	}

	private Tuple createTupleO(String orderkey, String custkey, String orderdate) {
		Tuple value = new Tuple();
		value.addAttribute(orderkey);
		value.addAttribute(custkey);
		value.addAttribute("3"); // orderstatus
		value.addAttribute("4.00"); // totalprice
		value.addAttribute(orderdate);
		value.addAttribute("6");// orderpriority
		value.addAttribute("7"); // clerk
		value.addAttribute("8"); // shippriority
		value.addAttribute("9"); // comment
		return value;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProjectN() {
		TPCHQuery10.ProjectN projectN = new ProjectN();
		reset(collector);
		PactInteger key = new PactInteger(1);
		String name = "MyName";
		Tuple value = createTupleN(name);
		projectN.map(key, value, collector);
		verify(collector, times(1)).collect(any(PactInteger.class), any(Tuple.class));
		Assert.assertEquals(1, value.getNumberOfColumns());
		Assert.assertEquals(name, value.getStringValueAt(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProjectC() {
		TPCHQuery10.ProjectC projectC = new ProjectC();
		reset(collector);
		PactInteger key = new PactInteger(1);
		String name = "MyName";
		String custkey = "1";
		String nationkey = "3";
		Tuple value = createTupleC(custkey, name, nationkey, "3", "phone", "acctbal", "comment");
		projectC.map(key, value, collector);
		verify(collector, times(1)).collect(any(PactInteger.class), any(Tuple.class));
		Assert.assertEquals(6, value.getNumberOfColumns());
		Assert.assertEquals(name, value.getStringValueAt(0));
		Assert.assertEquals(nationkey, value.getStringValueAt(2));
	}

	private Tuple createTupleC(String custkey, String name, String nationkey, String address, String phone,
			String acctbal, String comment) {
		Tuple value = new Tuple();
		value.addAttribute(custkey);
		value.addAttribute(name);
		value.addAttribute(address); // address
		value.addAttribute(nationkey);
		value.addAttribute(phone); // phone
		value.addAttribute(acctbal); // acctbal
		value.addAttribute("mktsegment");// mktsegment
		value.addAttribute(comment); // comment

		return value;
	}

	private Tuple createTupleN(String name) {
		Tuple value = new Tuple();
		value.addAttribute("1");
		value.addAttribute(name);
		value.addAttribute("3");// regionkey
		value.addAttribute("4"); // comment
		return value;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testJoinOLi() {
		reset(collector);
		Tuple oValue = createTupleO("1", "1", "1995-01-01");
		Tuple liValue = createLIValue("0.00", "0.00", "R", "1");
		Tuple sameLi = createLIValue("0.00", "0.00", "R", "1");
		liValue.project(2);
		sameLi.project(2);
		oValue.project(1);

		TPCHQuery10.JoinOL joinOLi = new JoinOL();
		joinOLi.match(new PactInteger(), oValue, liValue, collector);
		verify(collector, times(1)).collect(any(PactInteger.class), any(Tuple.class));
		Assert.assertEquals(sameLi, liValue);
		verify(collector).collect(eq(new PactInteger(1)), eq(sameLi));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testJoinCOL() {
		reset(collector);
		String extPrice = "1.00";
		String discount = "0.04";
		Tuple olValue = createLIValue(extPrice, discount, "R", "4");
		String name = "my name";
		String address = "address";
		String phone = "phone";
		String acctbal = "acctbal";
		String comment = "comment";
		String custkey = "1";
		Tuple cValue = createTupleC(custkey, name, "3"/* nationkey! */, address, phone, acctbal, comment);
		Tuple result = new Tuple();
		result.addAttribute(name);
		result.addAttribute(address);
		result.addAttribute(phone);
		result.addAttribute(acctbal);
		result.addAttribute(comment);
		result.addAttribute(custkey);
		result.addAttribute(extPrice);
		result.addAttribute(discount);

		olValue.project(96);
		cValue.project(190);

		TPCHQuery10.JoinCOL joinCOL = new JoinCOL();
		joinCOL.match(new PactInteger(1), cValue, olValue, collector);
		verify(collector, times(1)).collect(any(PactInteger.class), any(Tuple.class));
		verify(collector).collect(eq(new PactInteger(3)), eq(result));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testJoinNCOL() {
		reset(collector2);
		String extPrice = "1.00";
		String discount = "0.04";
		String nation = "Nation";
		Tuple nValue = createTupleN(nation);
		String name = "my name";
		String address = "address";
		String phone = "phone";
		String acctbal = "acctbal";
		String comment = "comment";
		String custkey = "1";
		Tuple colValue = new Tuple();
		GroupKey key = new GroupKey();
		key.addAttribute(nation);
		colValue.addAttribute(name);
		colValue.addAttribute(address);
		colValue.addAttribute(phone);
		colValue.addAttribute(acctbal);
		colValue.addAttribute(comment);
		colValue.addAttribute(custkey);
		key.concatenate(colValue);
		colValue.addAttribute(extPrice);
		colValue.addAttribute(discount);

		Tuple result = new Tuple();
		result.addAttribute(extPrice);
		result.addAttribute(discount);

		nValue.project(2);

		TPCHQuery10.JoinNCOL joinNCOL = new JoinNCOL();
		joinNCOL.match(new PactInteger(1), colValue, nValue, collector2);
		verify(collector2, times(1)).collect(any(GroupKey.class), any(Tuple.class));
		verify(collector2).collect(eq(key), eq(result));

	}

	@Test
	public void testReduce() {
		GroupKey key = new GroupKey();
	}
}
