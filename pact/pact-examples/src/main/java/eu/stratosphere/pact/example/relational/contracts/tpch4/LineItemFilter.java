/**
 * 
 */
package eu.stratosphere.pact.example.relational.contracts.tpch4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * Simple filter for the line item selection. It filters all teh tuples that do
 * not satisfy the &quot;l_commitdate &lt; l_receiptdate&quot; condition.
 * 
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 * 
 */
@SameKey
public class LineItemFilter extends
		MapStub<PactInteger, Tuple, PactInteger, Tuple> {

	private static Logger LOGGER = Logger.getLogger(LineItemFilter.class);

	@Override
	protected void map(PactInteger key, Tuple value,
			Collector<PactInteger, Tuple> out) {

		String commitString = value.getStringValueAt(11);
		String receiptString = value.getStringValueAt(12);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Date commitDate;
		try {
			commitDate = sdf.parse(commitString);
			Date receiptDate = sdf.parse(receiptString);

			if (commitDate.before(receiptDate)) {
				//TODO: do projection and filter everything except the orderkey 
				out.collect(key, value);
			}

		} catch (ParseException ex) {
			LOGGER.error(ex);
		}

	}

}
