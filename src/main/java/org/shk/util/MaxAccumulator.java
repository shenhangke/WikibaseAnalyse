package org.shk.util;

import org.apache.spark.util.AccumulatorV2;

public class MaxAccumulator extends AccumulatorV2<Long,Long>{
	
	private Long innerData=new Long(0);

	@Override
	public void add(Long value) {
		//innerData=innerData+value;
		//System.out.println("value"+value);
		//System.out.println("innerData"+this.innerData);
		if(value>=this.innerData){
			this.innerData=value;
			//System.out.println("the innerData is: "+this.innerData.toString());
		}
	}

	@Override
	public AccumulatorV2<Long, Long> copy() {
		MaxAccumulator newAcc=new MaxAccumulator();
		newAcc.innerData=this.innerData;
		return newAcc;
	}

	@Override
	public boolean isZero() {
		return innerData==0;
	}

	@Override
	public void merge(AccumulatorV2<Long, Long> value) {
		//To get the max value
		if(this.innerData<((MaxAccumulator)value).innerData){
			this.innerData=((MaxAccumulator)value).innerData;
		}
	}

	@Override
	public void reset() {
		this.innerData=0l;
		
	}

	@Override
	public Long value() {
		return this.innerData;
	}

}
