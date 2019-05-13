package org.shk.util;

import org.apache.spark.util.AccumulatorV2;

public class MinAccumulator extends AccumulatorV2<Long, Long>{
	
	private Long innerData=Long.MAX_VALUE;

	@Override
	public void add(Long value) {
		if(value<this.innerData){
			this.innerData=value;
		}
		
	}

	@Override
	public AccumulatorV2<Long, Long> copy() {
		MinAccumulator newMinAccumulator=new MinAccumulator();
		newMinAccumulator.innerData=this.innerData;
		return newMinAccumulator;
	}

	@Override
	public boolean isZero() {
		return this.innerData==Long.MAX_VALUE;
	}

	@Override
	public void merge(AccumulatorV2<Long, Long> value) {
		if(value.value()<this.innerData){
			this.innerData=value.value();
		}
	}

	@Override
	public void reset() {
		this.innerData=Long.MAX_VALUE;
		
	}

	@Override
	public Long value() {
		return this.innerData;
	}

}
