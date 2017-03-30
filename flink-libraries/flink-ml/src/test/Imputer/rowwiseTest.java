package Imputer;

import static org.junit.Assert.*;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.ml.math.DenseVector;
import org.junit.Test;

public class rowwiseTest {

	
	static DenseVector testvec1= new DenseVector(new double[]{Double.NaN,3.0,1.0, 3.0});
	static DenseVector testvec2= new DenseVector(new double[]{1.0,7.0,Double.NaN, 1.0});
	static DenseVector testvec3= new DenseVector(new double[]{0.0,5.0,Double.NaN, 2.0});
	static DenseVector testvec4= new DenseVector(new double[]{6.5,Double.NaN,0.5, 0.5});
	static DenseVector testvec5= new DenseVector(new double[]{6.5,1.0,0.5, 0.5});
	
	static ExecutionEnvironment env= ExecutionEnvironment.getExecutionEnvironment();
	static DataSet<DenseVector> ds = env.fromElements( testvec1, testvec2, testvec3, testvec4, testvec5);

	
	@Test
	public void testMEAN() throws Exception {
		DataSet<DenseVector> dsMean = ds;
		
		DenseVector testvec1e= new DenseVector(new double[]{14.0/4.0,3.0,1.0, 3.0});
		DenseVector testvec2e= new DenseVector(new double[]{1.0,7.0,2.0/3.0, 1.0});
		DenseVector testvec3e= new DenseVector(new double[]{0.0,5.0,2.0/3.0, 2.0});
		DenseVector testvec4e= new DenseVector(new double[]{6.5,4.0,0.5, 0.5});
		DenseVector testvec5e= new DenseVector(new double[]{6.5,1.0,0.5, 0.5});
		
		
		DataSet<DenseVector> dsExpected = env.fromElements(testvec1e, testvec2e, testvec3e, testvec4e, testvec5e);

		try {
			dsMean = Imputer.impute(ds, Strategy.MEAN, 1);
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
			fail("MEAN could not be calculated rowwise");
			System.out.println("dsmean: "); dsMean.print();
			System.out.println("dsexpected: " );dsExpected.print();
		}	
	}
	
	
	
	@Test
	public void testMEDIAN() throws Exception {
		DataSet<DenseVector> dsMedian = ds;
		
		DenseVector testvec1e= new DenseVector(new double[]{(7.5/2.0),3.0,1.0, 3.0});
		DenseVector testvec2e= new DenseVector(new double[]{1.0,7.0,0.5, 1.0});
		DenseVector testvec3e= new DenseVector(new double[]{0.0,5.0,0.5, 2.0});
		DenseVector testvec4e= new DenseVector(new double[]{6.5,4.0,0.5, 0.5});
		DenseVector testvec5e= new DenseVector(new double[]{6.5,1.0,0.5, 0.5});
		
		
		DataSet<DenseVector> dsExpected = env.fromElements(testvec1e, testvec2e, testvec3e, testvec4e, testvec5e);

		try {
			dsMedian = Imputer.impute(ds, Strategy.MEDIAN, 1);
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
			System.out.println("dsmedian: "); dsMedian.print();
			System.out.println("dsexpected: " );dsExpected.print();
			fail("MEDIAN could not be calculated rowwise");
		}	
	}

	
	
	@Test
	public void testMOSTFREQUENT() throws Exception {
		
		DenseVector testvec1= new DenseVector(new double[]{Double.NaN,3.0,1.0, Double.NaN});
		DenseVector testvec2= new DenseVector(new double[]{1.0,7.0,Double.NaN, 1.0});
		DenseVector testvec3= new DenseVector(new double[]{0.0,5.0,Double.NaN, 2.0});
		DenseVector testvec4= new DenseVector(new double[]{6.5,2.0,0.5, 0.5});
		DenseVector testvec5= new DenseVector(new double[]{6.5,1.0,0.5, 0.5});
		DataSet<DenseVector> dsMost = env.fromElements(testvec1, testvec2, testvec3, testvec4, testvec5);;
		
		
		DenseVector testvec1e= new DenseVector(new double[]{6.5,3.0,1.0, 0.5});
		DenseVector testvec2e= new DenseVector(new double[]{1.0,7.0,0.5, 1.0});
		DenseVector testvec3e= new DenseVector(new double[]{0.0,5.0,0.5, 2.0});
		DenseVector testvec4e= new DenseVector(new double[]{6.5,2.0,0.5, 0.5});
		DenseVector testvec5e= new DenseVector(new double[]{6.5,1.0,0.5, 0.5});
		
		
		DataSet<DenseVector> dsExpected = env.fromElements(testvec1e, testvec2e, testvec3e, testvec4e, testvec5e);

		try {
			dsMost = Imputer.impute(dsMost, Strategy.MOST_FREQUENT, 1);
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
			System.out.println("dsMost: "); dsMost.print();
			System.out.println("dsexpected: " );dsExpected.print();
			fail("MOSTFREQUENT could not be calculated rowwise");
		}	
	}
	

}
