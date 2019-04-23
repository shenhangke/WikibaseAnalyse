package org.shk.constValue;

import org.apache.spark.sql.SparkSession;

public class SparkConst {
	public static final SparkSession MainSession=SparkSession.builder().appName("shk_WikiAnalyse").getOrCreate();
}
