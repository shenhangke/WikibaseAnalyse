package org.shk.DataAny;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.shk.JsonParse.Item;
import org.shk.JsonParse.ParseItem;
import org.shk.JsonParse.Item.EntityType;
import org.shk.constValue.FileConstValue;
import org.shk.constValue.SparkConst;
import org.shk.util.EncodingDetect;

import com.google.protobuf.NullValue;

import DatabaseUtil.JDBCUtil;
import scala.Tuple2;

public class DataAnalyse implements Serializable{

	private static final long serialVersionUID = 1L;

	private SparkSession session=null;
	
	private static int Count=0;
	
	public DataAnalyse(){
		this.session=SparkConst.MainSession;
	}
	
	public DataAnalyse(SparkSession session){
		this.session=session;
	}
	/**
	 * 
	 * Description  this function will delete unnecessary char and return correct item dataset 
	 * @param fileName
	 * @return
	 * Return type: Dataset<Item>
	 */
	public Dataset<Item> extractDataItem(String filePath){
		//TestCode encodeing
		//============================================================================================//
		/**
		 * test the originData encode type
		 */
		Dataset<String> originFileDataStr=this.session.read().textFile(filePath);
		originFileDataStr.filter(new FilterFunction<String>() {

			@Override
			public boolean call(String value) throws Exception {
				//System.out.println(EncodingDetect.getEncoding(value));
				return false;
			}
		}).count();
		//==============================================================================================//
		//this.session.sparkContext().hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class, 1);
		JavaSparkContext tempJavaContext=new JavaSparkContext(this.session.sparkContext());
		JavaPairRDD<LongWritable, Text> hadoopFile = tempJavaContext.hadoopFile(filePath, TextInputFormat.class, LongWritable.class, Text.class,1);
		JavaRDD<String> hadoopFileString = hadoopFile.map(new Function<Tuple2<LongWritable,Text>, String>() {

			@Override
			public String call(Tuple2<LongWritable, Text> value) throws Exception {
				return new String(value._2.getBytes(),0,value._2.getLength(),"UTF-8");
			}
		});
		Dataset<String> originFileData = SparkConst.MainSession.createDataset(hadoopFileString.rdd(), Encoders.STRING());
		Dataset<Item> preHandledData=originFileData.filter(new FilterFunction<String>() {
			/*
			 * (non-Javadoc) delete the superfluous data
			 * @see org.apache.spark.api.java.function.FilterFunction#call(java.lang.Object)
			 */
			public boolean call(String value) throws Exception {
				if((value.trim().equals("["))||(value.trim().equals("]"))){
					return false;
				}else return true;
			}
		}).map(new MapFunction<String,Item>(){
			public Item call(String value) throws Exception {
				// TODO Auto-generated method stub
				//System.out.println("current line number: "+Count);
				//Count++;
				//String aLine=value.substring(0,value.length()-1).trim();
				String aLine="";
				if(value.endsWith(",")){
					aLine=value.substring(0,value.length()-1).trim();
				}else{
					aLine=value;
				}
				Item item= ParseItem.ParseJsonToItem(aLine);
				//System.out.println(item.entityId);
				return item;
			}},Encoders.bean(Item.class));
		//preHandledData.cache();
		//System.out.println(preHandledData.count());
		return preHandledData;
	}
	
	public void HandleData(Dataset<Item> handledData){
		//extract the tuple
	}
	
}
