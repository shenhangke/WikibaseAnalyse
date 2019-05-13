package org.shk.test;

import java.util.List;
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
import org.shk.util.EncodingDetect;
import org.spark_project.dmg.pmml.DataType;



public class TestExeOrder {
	public static String TestFilePath="D:\\MyEclpse WorkSpace\\DataProject_Data\\PropertyInfoFile\\PropertyInfoFile";
	
	public static void main(String[] args) throws Exception {
		//TestSaveDatasetToCsv("D:\\MyEclpse WorkSpace\\DataProject_Data\\TestData\\testCsv");
		//TestSparkCharSet();
		System.out.println(EncodingDetect.codeString("D:\\MyEclpse WorkSpace\\DataProject_Data\\TestData\\123_utf8.txt"));
	}
	
	public static void TestReadDirFile(){
		Dataset<String> originData = SparkConst.MainSession.read().textFile(TestFilePath);
		originData.show();
	}
	
	public static int GetPropertyCount(){
		Dataset<String> originData=SparkConst.MainSession.read().textFile(TestFilePath);
		List<String> originList=originData.collectAsList();
		return originList.size();
	}
	
	public static void TestSaveDatasetToCsv(String savePath){
		JavaSparkContext tempContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
		ArrayList<String> serList=new ArrayList<String>();
		for(int i=0;i<1000;i++){
			String tempStr=i+"_test&"+i;
			serList.add(tempStr);
		}
		JavaRDD<String> listParResult = tempContext.parallelize(serList);
		JavaRDD<Row> rowRdd = listParResult.map(new Function<String,Row>(){

			@Override
			public Row call(String value) throws Exception {
				// TODO Auto-generated method stub
				String[] splitArr = value.split("_");
				return RowFactory.create(splitArr);
			}
			
		});
		StructField index=new StructField("index", DataTypes.StringType, true, Metadata.empty());
		StructField value=new StructField("value", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={index,value};
		StructType schema=DataTypes.createStructType(fieldList);
		SparkConst.MainSession.createDataFrame(rowRdd, schema).show();/*.write().mode(SaveMode.Overwrite).csv(savePath);*/
	}
	
	public static void TestSparkCharSet(){
		SparkConst.MainSession.read().text("D:\\MyEclpse WorkSpace\\DataProject_Data\\TestData\\123_utf8.txt").show();
		
	}
}

