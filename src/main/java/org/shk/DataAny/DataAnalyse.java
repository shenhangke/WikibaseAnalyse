package org.shk.DataAny;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
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

import com.google.protobuf.NullValue;

import DatabaseUtil.JDBCUtil;

public class DataAnalyse implements Serializable{

	private static final long serialVersionUID = 1L;

	private SparkSession session=null;
	
	private static int Count=0;
	
	public static void main(String[] args) {
		DataAnalyse aData=new DataAnalyse();
		Dataset<Item> originData=aData.PreHandleData(FileConstValue.SourceFilePath);
		//aData.handlePropertyData(originData);
		aData.propertyExtract(originData);
	}
	
	public DataAnalyse(){
		this.session=SparkSession.builder().
				appName("WikiAnalyse").master("local[3]").config("spark.driver.memory","5g").
				config("spark.driver.cores",3).config("spark.executor.memory","1g").getOrCreate();
	}
	
	public DataAnalyse(SparkSession session){
		this.session=session;
	}
	
	public void propertyExtract(Dataset<Item> originData){
		JavaRDD<Row> aProItemRDD=originData.filter(new FilterFunction<Item>() {

			public boolean call(Item value) throws Exception {
				// TODO Auto-generated method stub
				if(value.type==Item.EntityType.Property){
					System.out.println("--------------"+value.entityId+"------is property");
					return true;
				}else{
					return false;
				}
			}
			
		}).map(new MapFunction<Item,Row>() {

			public Row call(Item value) throws Exception {
				// TODO Auto-generated method stub
				String proId=value.entityId;
				System.out.println("the proId is: "+proId);
				//String item=value.originJson;
				//System.out.println("the item is: "+item);
				return RowFactory.create(proId);
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		
		StructField proIdTitle=new StructField("PID", DataTypes.StringType, false, Metadata.empty());
		//StructField itemTitle=new StructField("Item", DataTypes.StringType, false, Metadata.empty());
		ArrayList<StructField> aTitleList=new ArrayList<StructField>();
		aTitleList.add(proIdTitle);
		//aTitleList.add(itemTitle);
		StructType schema=DataTypes.createStructType(aTitleList);
		Dataset<Row> proItem=this.session.createDataFrame(aProItemRDD, schema);
		proItem.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, JDBCUtil.PropertyItem, JDBCUtil.GetWriteProperties());
	}
	
	
	public Dataset<Item> PreHandleData(String fileName){
		Dataset<String> originFileData=this.session.read().textFile(fileName).persist(StorageLevel.MEMORY_AND_DISK());
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
				Count++;
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
	
	/**
	 * 
	 * Description extract relation from data 
	 * @param originData
	 * Return type: void
	 */
	public void handlePropertyData(Dataset<Item> originData){
		//extract the tuple from Property
		Dataset<Item> propertyItem=originData.filter(new FilterFunction<Item>() {

			public boolean call(Item value) throws Exception {
				// TODO Auto-generated method stub
				//System.out.println("the entityType is: "+value.type);
				//System.out.println("the entityId is: "+value.entityId);
				if((value.type==Item.EntityType.Property)&&(!value.entityId.equals(""))){
					System.out.println("not filter this item");
					return true;
				}else{
					//System.out.println("filter this item");
					return false;
				}
			}
		
		});
		List<Item> propertyCollect= propertyItem.collectAsList();
		System.out.println(propertyCollect.size());
		
		JavaRDD<Row> propertyRdd=propertyItem.map(new MapFunction<Item,Row>(){

			public Row call(Item aItem) throws Exception {
				// TODO Auto-generated method stub
				String propertyId=aItem.entityId;
				String propertyName=JDBCUtil.NULLVALUE;
				if(aItem.labels!=null){
					boolean isFindEnLan=false;
					Item.LanItem bakLanItem=null;
					boolean hasBak=false;
					for(Entry<String,Item.LanItem> entry:aItem.labels.entrySet()){
						if(entry.getKey().toLowerCase().equals("en")){
							propertyName=entry.getValue().value;
							isFindEnLan=true;
							break;
						}
						if(!hasBak){
							bakLanItem=entry.getValue();
							hasBak=true;
						}
					}
					if(isFindEnLan==false){
						if(bakLanItem!=null){
							propertyName=bakLanItem.value;
						}else{
							System.out.println("the property label is null,the id is: "+propertyId);
						}
					}
				}
				return RowFactory.create(propertyId,propertyName,propertyId+"_LinkInfo");
			}
		},Encoders.bean(Row.class)).javaRDD();
		
		StructField propertyIdTitle=new StructField("PropertyID", DataTypes.StringType, false, Metadata.empty());
		StructField propertyNameTitle=new StructField("PropertyName",DataTypes.StringType,true,Metadata.empty());
		StructField LinkInfoName=new StructField("EntityListTableName", DataTypes.StringType, true, Metadata.empty());
		ArrayList<StructField> fields=new ArrayList<StructField>();
		fields.add(propertyIdTitle);
		fields.add(propertyNameTitle);
		fields.add(LinkInfoName);
		StructType schema=DataTypes.createStructType(fields);
		Dataset<Row> databaseFrame=this.session.createDataFrame(propertyRdd, schema);
		databaseFrame.show();
		databaseFrame.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL,JDBCUtil.PropertyInfoTable,JDBCUtil.GetWriteProperties());
	}
	
}
