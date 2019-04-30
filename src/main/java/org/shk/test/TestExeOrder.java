package org.shk.test;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
	public static void main(String[] args) {
		JavaSparkContext context=new JavaSparkContext(SparkConst.MainSession.sparkContext());
		context.setLogLevel("WARN");
		ArrayList<Integer> aArray=new ArrayList<Integer>();
		System.out.println("program has start");
		for(int i=0;i<1;i++){
			aArray.add(new Integer(i));
		}
		JavaRDD<Integer> testResult = context.parallelize(aArray);
		JavaRDD<Row> mapRdd = testResult.map(new Function<Integer, Row>() {

			@Override
			public Row call(Integer v1) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("the executor is executor");
				//return RowFactory.create(new Integer(v1),new Integer(2));
				return RowFactory.create(String.valueOf(v1),",123");
			}
			
		});
		System.out.println("from order,the executor has finish");
		//To verify the type of save file
		mapRdd.saveAsTextFile("D:\\MyEclpse WorkSpace\\DataAny\\test\\TestRdd");
		StructField first=new StructField("first", DataTypes.StringType, true, Metadata.empty());
		StructField secord=new StructField("secord", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={first,secord};
		StructType schema=DataTypes.createStructType(fieldList);
		SparkConst.MainSession.createDataFrame(mapRdd, schema).write().mode(SaveMode.Overwrite).text("D:\\MyEclpse WorkSpace\\DataAny\\test\\TestDataFrame");
	}
}
