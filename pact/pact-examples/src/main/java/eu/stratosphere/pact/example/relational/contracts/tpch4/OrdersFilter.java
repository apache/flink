/**
 * 
 */
package eu.stratosphere.pact.example.relational.contracts.tpch4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.relational.util.Tuple;

/**
 * Small {@link MapStub} to filer out the irrelevant orders.
 * @author Mathias Peters <mathias.peters@informatik.hu-berlin.de>
 *
 */
@SameKey
public class OrdersFilter extends MapStub<PactInteger, Tuple, PactInteger, Tuple> {

	private static Logger LOGGER = Logger.getLogger(OrdersFilter.class);
	private String dateParamString = "1995-01-01";
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stub.MapStub#map(eu.stratosphere.pact.common.type.Key, eu.stratosphere.pact.common.type.Value, eu.stratosphere.pact.common.stub.Collector)
	 */
	@Override
	public void map(PactInteger key, Tuple value,
			Collector<PactInteger, Tuple> out) {
		
		String orderStringDate = value.getStringValueAt(4);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		try {
			Date orderDate = sdf.parse(orderStringDate);
			Date paramDate = sdf.parse(this.dateParamString);
			
			Date plusThreeMonths = getPlusThreeMonths(paramDate);
			
			if(paramDate.before(orderDate) && plusThreeMonths.after(orderDate))
			{
				//TODO: make projection
				out.collect(key, value);
			}
			
		} catch (ParseException e) {
			LOGGER.error(e);		}
		

	}

	/**
	 * Calculates the {@link Date} which is three months after the given one.
	 * @param paramDate of type {@link Date}.
	 * @return a {@link Date} three month later.
	 */
	private Date getPlusThreeMonths(Date paramDate) {
		GregorianCalendar gregCal = new GregorianCalendar();
		gregCal.setTime(paramDate);
		gregCal.add(Calendar.MONTH, 3);
		Date plusThreeMonths = gregCal.getTime();
		return plusThreeMonths;
	}

}
