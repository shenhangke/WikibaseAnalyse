package org.shk.util;

import org.apache.spark.util.AccumulatorV2;

public class MeanAccumulator extends AccumulatorV2<Integer,Integer>{
	
	private int itemCount=0;
    private int totalCount=0;
    private int currentAverage=0;
	
	@Override
	public void add(Integer value) {
		// TODO Auto-generated method stub
		//System.out.println("the value is : "+value);
		this.totalCount+=value;
		//System.out.println("the total value is: "+this.totalCount);
		this.itemCount++;
		this.currentAverage=totalCount/itemCount;
	}

	@Override
	public AccumulatorV2<Integer, Integer> copy() {
		// TODO Auto-generated method stub
		MeanAccumulator mean=new MeanAccumulator();
		mean.currentAverage=this.currentAverage;
		mean.itemCount=this.itemCount;
		mean.totalCount=this.totalCount;
		return mean;
	}

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
		return currentAverage==0;
	}

	@Override
	public void merge(AccumulatorV2<Integer, Integer> value) {
		// TODO Auto-generated method stub
		MeanAccumulator mean=(MeanAccumulator)value;
		this.itemCount+=mean.itemCount;
		this.totalCount+=mean.totalCount;
		this.currentAverage=this.totalCount/this.itemCount;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.itemCount=0;
		this.totalCount=0;
		this.currentAverage=0;
	}

	@Override
	public Integer value() {
		// TODO Auto-generated method stub
		return this.currentAverage;
	}

}
