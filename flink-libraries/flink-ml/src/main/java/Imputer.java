package Imputer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.apache.hadoop.mapreduce.Reducer;

import scala.collection.mutable.LinkedList;
import scala.reflect.internal.Trees.New;

public class Imputer {

	
	static ExecutionEnvironment env= ExecutionEnvironment.getExecutionEnvironment();
	
	private static double[] meansA;
	private static double[] medians;
	private static double[] mostValues;

	
	/**
	 * 
	 * @param sparseData
	 * @param strategy use MEAN, MEDIAN or the MOST_FREQUENT value to impute missing values
	 * @param axis 0: impute along columns, 1: imput along rows
	 * @return dataset without zeroes / missing values
	 * @throws Exception 
	 */
	public static DataSet<DenseVector> impute(DataSet<DenseVector> sparseData, Strategy strategy, int axis) throws Exception{
		double val;
		DataSet<DenseVector> ret = sparseData;
		if(axis==0){ //columnwise
			switch (strategy){
			case MEAN:
				ret=sparseData.map(new MapFunction<DenseVector, DenseVector>() {
					@Override
					public DenseVector map(DenseVector vec) throws Exception {
						for(int i = 0; i<vec.size();i++){
							if(Double.compare(Double.NaN, vec.apply(i))==0){
								vec.update(i, getValueMEAN(vec));
							}
						}
						return vec;
					}
				});
			break;
			case MEDIAN:
				ret=sparseData.map(new MapFunction<DenseVector, DenseVector>() {
					@Override
					public DenseVector map(DenseVector vec) throws Exception {
						for(int i = 0; i<vec.size();i++){
							if(Double.compare(Double.NaN, vec.apply(i))==0){
								vec.update(i, getValueMEDIAN(vec));
							}
						}
						return vec;
					}
				});
				
			break;
			case MOST_FREQUENT:
				ret=sparseData.map(new MapFunction<DenseVector, DenseVector>() {
					@Override
					public DenseVector map(DenseVector vec) throws Exception {
						for(int i = 0; i<vec.size();i++){
//							if(vec.apply(i)==Double.NaN){
							if(Double.compare(Double.NaN, vec.apply(i))==0){
								vec.update(i, getValueMOST(vec));
							}
						}
						return vec;
					}
				});
			break;
			}
		}
		
		else if(axis==1){//rowwise
			switch (strategy){
			case MEAN:
				getValue(sparseData, Strategy.MEAN);
				ret=sparseData.map(new MapFunction<DenseVector, DenseVector>() {
					double v;
					@Override
					public DenseVector map(DenseVector vec) {
						for(int i = 0; i<vec.size();i++){
							if(Double.compare(Double.NaN, vec.apply(i))==0){
								vec.update(i,  meansA[i]);
							}
						}
						return vec;
					}
				});
			break;
			case MEDIAN:
				System.out.println("im here calculating the median");
				getValue(ds, Strategy.MEDIAN);
				ret=sparseData.map(new MapFunction<DenseVector, DenseVector>() {
					@Override
					public DenseVector map(DenseVector vec) throws Exception {
						for(int i = 0; i<vec.size();i++){
							if(Double.compare(Double.NaN, vec.apply(i))==0){
								vec.update(i, medians[i]);
							}
						}
						return vec;
					}
				});
				
			break;
			case MOST_FREQUENT:
				getValue(ds, Strategy.MOST_FREQUENT);
				ret=sparseData.map(new MapFunction<DenseVector, DenseVector>() {
					@Override
					public DenseVector map(DenseVector vec) throws Exception {
						for(int i = 0; i<vec.size();i++){
							if(Double.compare(Double.NaN, vec.apply(i))==0){
								vec.update(i, mostValues[i] );
							}
						}
						return vec;
					}
				});
			break;
			}
		}
		return ret;
	}
	
	
	public static double getValueMEAN(DenseVector vec){
		int num=0;
		double sum=0;
		double v;
		for(int i = 0; i<vec.size(); i++){
			v=vec.apply(i);
			if(Double.compare(Double.NaN, v)!=0){
				num++;
				sum+=vec.apply(i);
			}
		}
		return sum/num;
	}
	public static double getValueMEDIAN(DenseVector vec){
		double ret=0;
		List<Double> numArray = new ArrayList<>();
		double val;
		for(int i =0; i< vec.size(); i++){
			val=vec.apply(i);
			if(Double.compare(Double.NaN, val)!=0){
				numArray.add(val);
			}
		}
		Collections.sort(numArray);
		int middle = numArray.size() / 2;
		if(numArray.size() % 2 == 0){
		 double medianA = numArray.get(middle);
		 double medianB = numArray.get(middle-1);
		 ret = (medianA + medianB) / 2d;
		} else{
		 ret = numArray.get(middle);
		}
		return ret;
	}
	public static double getValueMOST(DenseVector vec){
		double ret=0;
		HashMap<Double, Integer> frequencies= new HashMap<>();
		for(int i =0; i<vec.size();i++){
			double key= vec.apply(i);
			if(Double.compare(Double.NaN, key)!=0){
				if(frequencies.containsKey(key)){
					frequencies.put(key, frequencies.get(key)+1);
				}else{
					frequencies.put(key, 1);
				}
			}
		}
		int max=0;
		double maxKey = 0;
		for(double key: frequencies.keySet()){
			if(frequencies.get(key)>max){
				max=frequencies.get(key);
				maxKey=key;
			}
		}
		ret=maxKey;
		return ret;
		
	}
	
	public static int numOfElementsNotZero(DenseVector vec){
		int zeros=0;
		for(int i=0; i<vec.size(); i++){
			if(Double.compare(Double.NaN, vec.apply(i))==0){
				zeros++;
			}
		}
		return (vec.size()-zeros);
	}
	
	
	public static void getValue(DataSet<DenseVector> ds, Strategy strategy) throws Exception{
		//(entry, 1, dimension)
		DataSet<Tuple3<Double, Integer, Integer>> nonZeroTuples= ds.flatMap(new FlatMapFunction<DenseVector,Tuple3<Double, Integer, Integer>>() {
			@Override
			public void flatMap(DenseVector vec, Collector<Tuple3<Double,Integer, Integer>> col) throws Exception {
				double[] entries= vec.data();
				double entry;
				for(int i = 0; i<entries.length; i++){
					entry=entries[i];
					if(Double.compare(Double.NaN, entry)!=0){
						col.collect(new Tuple3<Double, Integer, Integer> (entry, 1, i) );
					}
				}
			}
		});
		
		List<Tuple2<Integer, List<Double>>> lists;
		DataSet<Tuple2<Integer, List<Double>>> nonZeros2= nonZeroTuples.map(new MapFunction<Tuple3<Double,Integer,Integer>, Tuple2<Integer, List<Double>>	>() {
			@Override
			public Tuple2<Integer, List<Double>> map(Tuple3<Double, Integer, Integer> t) throws Exception {
				return new Tuple2<Integer, List<Double>>(t.f2, Lists.newArrayList(t.f0));
			}
		});
		
		lists= nonZeros2.groupBy(0).reduce(new ReduceFunction<Tuple2<Integer, List<Double>>>() {
			List<Double> ret= new java.util.LinkedList<>();
			@Override
			public Tuple2<Integer, List<Double>> reduce(Tuple2<Integer, List<Double>> t1,
					Tuple2<Integer, List<Double>> t2) throws Exception {
				ret=ListUtils.union(t1.f1,t2.f1);
				return new Tuple2<Integer, List<Double>>(t1.f0, ret);
			}
		}).collect();
		
		switch(strategy){
		case MEAN:
			DataSet<Tuple3<Double, Integer, Integer>> infos= nonZeroTuples.groupBy(2).sum(0).andSum(1);
			meansA= new double[lists.size()];
			final String s = "hello";
			List<Tuple2<Integer, Double>> means= infos.map(new MapFunction<Tuple3<Double,Integer,Integer>, Tuple2<Integer, Double>>() {
				@Override
				public Tuple2<Integer, Double> map(Tuple3<Double, Integer, Integer> t) throws Exception {
					double mean= (double) t.f0/(double) t.f1;
					return new Tuple2<Integer, Double>(t.f2, mean);
				}
			}).collect();
			for(Tuple2<Integer, Double> t: means){
				meansA[t.f0]=t.f1;
			}
		break;
		case MEDIAN:
			double median;
			int size;
			//lists contains a list for every dimension in which all the values of the data set are written.
			// we will later sort every of those lists in order to determine the median.
			
			medians= new double[lists.size()];
			List<Double> l;
			for(Tuple2<Integer, List<Double>> t: lists){
				l= t.f1;
				Collections.sort(l);
				System.out.println("list "  + t.f0 + " ist "+ l.toString());
				size=l.size();
				if(size%2==0){
					median=(l.get(size/2) + l.get(size/2-1))/2d;
				}else{
					median=l.get(size/2);
				}
				medians[t.f0]=median;
			}
			for(int i = 0; i<medians.length; i++){
				System.out.println("median an " + i + " ist " + medians[i]);
			}
		break;
		case MOST_FREQUENT:
			
			double ret=0;
			double key;
			HashMap<Double, Integer> frequencies= new HashMap<>();
			mostValues= new double[lists.size()];
			for(Tuple2<Integer,List<Double>> t: lists){
				int max=0;
				double maxKey=0;
				// calculate frequencie hashmap for each dimension
				List<Double> list=t.f1;
				frequencies.clear();
				for(int j=0; j<list.size();j++){
					key= list.get(j);
					if(frequencies.containsKey(key)){
						frequencies.put(key, frequencies.get(key)+1);
					}else{
						frequencies.put(key, 1);
					}
				}
			
				//write most frequent values of each dimension into mostValues list
				for(double k: frequencies.keySet()){
					if(frequencies.get(k)>max){
						max=frequencies.get(k);
						maxKey=k;
					}
				}
				mostValues[t.f0]=maxKey;
			
			}
		break;
		}
	}
		
	
}
