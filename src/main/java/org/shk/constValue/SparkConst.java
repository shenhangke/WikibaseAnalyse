package org.shk.constValue;

import org.apache.spark.sql.SparkSession;

public class SparkConst {
	public enum FileOrDatabase{
		File,Database;
	} 
	
	public static final SparkSession MainSession=SparkSession.builder().master("local").appName("shk_WikiAnalyse").config("spark.executor.memory","8g").
			config("spark.executor.extraJavaOptions","-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps").getOrCreate();
	//public static final SparkSession TestSession=SparkSession.builder().master("local[3]").appName("shk_WikiAnalyse").getOrCreate();
}
