package org.shk.test;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.shk.constValue.SparkConst;
import org.spark_project.dmg.pmml.DataType;



public class TestExeOrder {
	public static String TestFilePath="D:\\MyEclpse WorkSpace\\DataProject_Data\\PropertyInfoFile";
	
	public static void main(String[] args) {
		TestReadDirFile();
	}
	
	public static void TestReadDirFile(){
		Dataset<String> originData = SparkConst.MainSession.read().textFile(TestFilePath);
		originData.show();
	}
}
