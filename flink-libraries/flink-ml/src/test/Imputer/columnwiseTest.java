package Imputer;

import static org.junit.Assert.*;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.ml.math.DenseVector;
import org.junit.Test;

public class columnwiseTest {

	static DenseVector testvec1= new DenseVector(new double[]{Double.NaN,3.0,1.0, 3.0});
	static DenseVector testvec2= new DenseVector(new double[]{1.0,7.0,Double.NaN, 1.0});
	static DenseVector testvec3= new DenseVector(new double[]{0.0,5.0,Double.NaN, 2.0});
	static DenseVector testvec4= new DenseVector(new double[]{6.5,Double.NaN,0.5, 0.5});
	static DenseVector testvec5= new DenseVector(new double[]{6.5,1.0,0.5, 0.5});
	
	static ExecutionEnvironment env= ExecutionEnvironment.getExecutionEnvironment();
	static DataSet<DenseVector> ds = env.fromElements(  testvec3, testvec4);

	
	@Test
	public void testMEAN() throws Exception {
		DataSet<DenseVector> dsMean = ds;
		
//		DenseVector testvec1e= new DenseVector(new double[]{4.0,3.0,1.0, 3.0});
//		DenseVector testvec2e= new DenseVector(new double[]{1.0,7.0,6.0, 1.0});
		DenseVector testvec3e= new DenseVector(new double[]{0.0,5.0,(5.0+2.0)/3, 2.0});
		DenseVector testvec4e= new DenseVector(new double[]{6.5,2.5,0.5, 0.5});
//		DenseVector testvec5e= new DenseVector(new double[]{6.5,1.0,0.5, 0.5});
		
		
		DataSet<DenseVector> dsExpected = env.fromElements(  testvec3e, testvec4e);

		try {
			dsMean = Imputer.impute(ds, Strategy.MEAN, 0);
		} catch (Exception e) {
			fail("MEAN could not be calculated");

		}
		boolean fails=false;
		for(DenseVector v: dsExpected.collect()){
			if(!dsMean.collect().contains(v)){
				fails=true;
			};
		}
		
		if(fails){
			fail("MEAN could not be calculated columnwise");
			System.out.println("dsmean: "); dsMean.print();
			System.out.println("dsexpected: " );dsExpected.print();
		}
	}

	
	
	@Test
	public void testMEDIAN() throws Exception {
		DataSet<DenseVector> dsMedian = ds;
		
		DenseVector testvec3e= new DenseVector(new double[]{0.0,5.0,2.0, 2.0});
		DenseVector testvec4e= new DenseVector(new double[]{6.5,0.5,0.5, 0.5});
		
		
		DataSet<DenseVector> dsExpected = env.fromElements(  testvec3e, testvec4e);

		try {
			dsMedian = Imputer.impute(ds, Strategy.MEDIAN, 0);
		} catch (Exception e) {
			fail("MEDIAN could not be calculated");

		}
		boolean fails=false;
		for(DenseVector v: dsExpected.collect()){
			if(!dsMedian.collect().contains(v)){
				fails=true;
			};
		}
		
		if(fails){
			fail("MEDIAN could not be calculated columnwise");
			System.out.println("dsmedian: "); dsMedian.print();
			System.out.println("dsexpected: " );dsExpected.print();
		}
	}

	
	@Test
	public void testMOSTFREQUENT() throws Exception {
		DataSet<DenseVector> dsMost = env.fromElements( testvec2, testvec1, testvec4);;
		
		DenseVector testvec2e= new DenseVector(new double[]{1.0,7.0,1.0, 1.0});
		DenseVector testvec1e= new DenseVector(new double[]{3.0,3.0,1.0, 3.0});
		DenseVector testvec4e= new DenseVector(new double[]{6.5,0.5,0.5, 0.5});
		
		
		DataSet<DenseVector> dsExpected = env.fromElements( testvec2e, testvec1e, testvec4e);

		try {
			dsMost = Imputer.impute(dsMost, Strategy.MOST_FREQUENT, 0);
		} catch (Exception e) {
			fail("MOSTFREQUENT could not be calculated");

		}
		boolean fails=false;
		for(DenseVector v: dsExpected.collect()){
			if(!dsMost.collect().contains(v)){
				fails=true;
			};
		}
		
		if(fails){
			System.out.println("dsmostfrequent: "); dsMost.print();
			System.out.println("dsexpected: " );dsExpected.print();
			fail("MOSTFREQUENT could not be calculated columnwise");
		}
	}
	
	
}
