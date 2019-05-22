package org.shk.constValue;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConst {
	public enum FileOrDatabase{
		File,Database;
	} 
	
	public static final boolean RunOnLocal=false;
	
	public static final SparkSession MainSession=SparkSession.builder().appName("shk_WikiAnalyse").getOrCreate();
	
	/*public static final SparkSession LocalSession=SparkSession.builder().appName("shk_WikiAnalyse").master("local[3]").config("spark.executor.memory","3g").
			config("spark.driver.memory","4g").getOrCreate();*/
	
	static{
		JavaSparkContext javaSparkcontext=new JavaSparkContext(MainSession.sparkContext());
		javaSparkcontext.setLogLevel("WARN");
	}
	//public static final SparkSession TestSession=SparkSession.builder().master("local[3]").appName("shk_WikiAnalyse").getOrCreate();
}
