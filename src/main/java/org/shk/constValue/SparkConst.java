package org.shk.constValue;

import org.apache.spark.sql.SparkSession;

public class SparkConst {
	public static final SparkSession MainSession=SparkSession.builder().appName("shk_WikiAnalyse").config("spark.executor.memory","7g").getOrCreate();
	//public static final SparkSession TestSession=SparkSession.builder().master("local[3]").appName("shk_WikiAnalyse").getOrCreate();
}
