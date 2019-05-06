package org.shk.constValue;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConst {
	public enum FileOrDatabase{
		File,Database;
	} 
	
	public static final SparkSession MainSession=SparkSession.builder().master("yarn").appName("shk_WikiAnalyse").config("spark.executor.memory","7g").
			config("spark.executor.extraJavaOptions","-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps").getOrCreate();
	
	static{
		JavaSparkContext javaSparkcontext=new JavaSparkContext(MainSession.sparkContext());
		javaSparkcontext.setLogLevel("WARN");
	}
	//public static final SparkSession TestSession=SparkSession.builder().master("local[3]").appName("shk_WikiAnalyse").getOrCreate();
}
