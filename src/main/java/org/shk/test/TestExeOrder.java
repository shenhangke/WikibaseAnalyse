package org.shk.test;

import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.shk.DataAny.AnalyseItemData;
import org.shk.DataAny.AnalyseItemData.CountInfo;
import org.shk.DataAny.DataAnalyse;
import org.shk.constValue.FileConstValue;
import org.shk.constValue.SparkConst;
import org.shk.util.EncodingDetect;
import org.spark_project.dmg.pmml.DataType;

import DatabaseUtil.JDBCUtil;



public class TestExeOrder {
	public static String TestFilePath="D:\\MyEclpse WorkSpace\\DataProject_Data\\PropertyInfoFile\\PropertyInfoFile";
	
	public static void main(String[] args) throws Exception {
		//TestSaveDatasetToCsv("D:\\MyEclpse WorkSpace\\DataProject_Data\\TestData\\testCsv");
		//TestSparkCharSet();
		//System.out.println(EncodingDetect.codeString("D:\\MyEclpse WorkSpace\\DataProject_Data\\TestData\\123_utf8.txt"));
		//TestSotreInDatabaseEncoding("D:\\MyEclpse WorkSpace\\DataProject_Data\\TestData\\part-01351-4182331a-6b39-4788-a2f8-3f33968cd9a9-c000.csv","");
		//System.out.println((double)(18555611.0/20991701.0));
		//System.out.println((int)(127/64));
		/*String test="1233"+FileConstValue.StrSeparator+"234324";
		String[] testArr=test.split(FileConstValue.StrSeparator);
		System.out.println(testArr.length);
		for(int i=0;i<testArr.length;i++){
			System.out.println(testArr[i]);
		}*/
		
		AnalyseItemData anaItem=new AnalyseItemData(SparkConst.MainSession);
		DataAnalyse dataAna=new DataAnalyse(SparkConst.MainSession);
		CountInfo maxIdNum = anaItem.getMaxIdNum(anaItem.getItemDataItem(dataAna.extractDataItem(FileConstValue.DivideFilePath)));
		System.out.println(maxIdNum.getMaxIdValue());
		System.out.println(maxIdNum.getMaxItemCount());
		System.out.println(maxIdNum.getMinIdValue());
		//System.out.println(maxIdNum.getMaxIdValue());
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
	
	public static void TestSotreInDatabaseEncoding(String filePath,String tableName){
		Dataset<Row> readFromCsvDataset = SparkConst.MainSession.read().csv(filePath);
		readFromCsvDataset=readFromCsvDataset.withColumnRenamed("_c0", "QIndex");
		readFromCsvDataset=readFromCsvDataset.withColumnRenamed("_c1", "ID");
		readFromCsvDataset=readFromCsvDataset.withColumnRenamed("_c2", "Name");
		readFromCsvDataset=readFromCsvDataset.withColumnRenamed("_c3", "Description");
		JavaRDD<Row> testDatasetRdd = readFromCsvDataset.map(new MapFunction<Row,Row>(){

			@Override
			public Row call(Row value) throws Exception {
				// TODO Auto-generated method stub
				String IDStr=value.getString(1).substring(1,value.getString(1).length());
				Integer ID=Integer.parseInt(IDStr);
				return RowFactory.create(Integer.parseInt(value.getString(0)),ID,value.getString(2),value.getString(3));
			}},Encoders.bean(Row.class)).javaRDD();
		StructField QIndex=new StructField("QIndex", DataTypes.IntegerType, true, Metadata.empty());
		StructField IDF=new StructField("ID", DataTypes.IntegerType, true, Metadata.empty());
		StructField name=new StructField("Name", DataTypes.StringType, true, Metadata.empty());
		StructField description=new StructField("Description", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={QIndex,IDF,name,description};
		StructType schema=DataTypes.createStructType(fieldList);
		SparkConst.MainSession.createDataFrame(testDatasetRdd, schema).write().mode(SaveMode.Overwrite).jdbc(DatabaseUtil.JDBCUtil.DB_URL, JDBCUtil.ItemInfo, JDBCUtil.GetWriteProperties(JDBCUtil.ItemInfo));
		//readFromCsvDataset.write().mode(SaveMode.Overwrite).jdbc(DatabaseUtil.JDBCUtil.DB_URL, JDBCUtil.ItemInfo, JDBCUtil.GetWriteProperties(JDBCUtil.ItemInfo));
	}
	
}

