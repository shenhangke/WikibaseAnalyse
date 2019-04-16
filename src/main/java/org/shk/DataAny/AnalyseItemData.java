package org.shk.DataAny;

import java.util.List;
import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.shk.DataAny.AnalysePropertyData.DataType;
import org.shk.JsonParse.Item;
import org.shk.JsonParse.Item.Property.PropertyInfo;
import org.shk.constValue.FileConstValue;

import DatabaseUtil.JDBCUtil;
import DatabaseUtil.PropertyDatabaseUtil;
import scala.Tuple2;
import shapeless.newtype;

public class AnalyseItemData implements Serializable{

	private static final long serialVersionUID = 1L;
	private SparkSession session=null; 
	
	public AnalyseItemData(SparkSession session) {
		if(session!=null){
			this.session=session;
		}else{
			throw new NullPointerException("the session is null");
		}
	}
	
	public Dataset<Item> filterItemLine(Dataset<Item> originData){
		return originData.filter(new FilterFunction<Item>() {

			public boolean call(Item item) throws Exception {
				if(item.type==Item.EntityType.Item){
					return true;
				}else{
					return false;
				}
			}
		});
	}
	
	private StructType getSparkWriteToFileSchema(){
		StructField content=new StructField("content", DataTypes.StringType, true, Metadata.empty());
		StructField[] contentList={content};
		StructType schema=DataTypes.createStructType(contentList);
		return schema;
	}
	
	/**
	 * 
	 * Description 
	 * @param originData it could comes from DataAnalyse.PreHandleData
	 * @return
	 * Return type: Dataset<Row>
	 */
	public Dataset<Row> itemInfoAnalyse(Dataset<Item> originData,String tableName){
		System.out.println("the store file path is: "+this.getStoreFilePath(tableName));
		JavaRDD<Row> itemInfoRdd = originData.map(new MapFunction<Item,Row>(){

			public Row call(Item item) throws Exception {
				// TODO Auto-generated method stub
				String entityID=item.entityId;
				Item.LanItem aLabelItem=item.labels.get("en");
				if(aLabelItem==null){
					for(Entry<String,Item.LanItem> aLineLanItem:item.labels.entrySet()){
						aLabelItem=aLineLanItem.getValue();
						break;
					}
				}
				String pName=null;
				if(aLabelItem!=null){
					pName=aLabelItem.value;
				}
				Item.LanItem aDes=item.descriptions.get("en");
				if(aDes==null){
					for(Entry<String,Item.LanItem> aLineLanItem:item.descriptions.entrySet()){
						aDes=aLineLanItem.getValue();
						break;
					}
				}
				String pDescription=null;
				if(aDes!=null){
					pDescription=aDes.value;
				}
				return RowFactory.create(entityID,pName,pDescription);
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		StructField pIDField=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
		StructField pNameField=new StructField("PName", DataTypes.StringType, true, Metadata.empty());
		StructField pDescriptionField=new StructField("PDescription", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={pIDField,pNameField,pDescriptionField};
		StructType schema=DataTypes.createStructType(fieldList);
		Dataset<Row> infoData = this.session.createDataFrame(itemInfoRdd, schema);
		if(this.isWriteToFile(tableName)){
			//infoData.javaRDD().saveAsTextFile(this.getStoreFilePath(tableName));
			JavaRDD<Row> witeToFileRdd = infoData.map(new MapFunction<Row,Row>(){

				@Override
				public Row call(Row row) throws Exception {
					String multiToSingle="";
					for(int i=0;i<row.size();i++){
						if(i==row.size()-1){
							multiToSingle=multiToSingle+row.getString(i);
						}else{
							multiToSingle=multiToSingle+row.getString(i)+"&&&&";
						}
					}
					return RowFactory.create(multiToSingle);
				}
				
			},Encoders.bean(Row.class)).javaRDD();
			this.session.createDataFrame(witeToFileRdd, this.getSparkWriteToFileSchema()).write().mode(SaveMode.Overwrite).text(this.getStoreFilePath(tableName));
		}else{
			infoData.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
		}
		return infoData;
	}
	
	public Dataset<Row> itemAliasAnalyse(Dataset<Item> originData,String tableName){
		JavaRDD<Row> itemAliasRdd = originData.filter(new FilterFunction<Item>() {
			
			public boolean call(Item aLineItem) throws Exception {
				if(aLineItem.aliases.get("en")==null){
					return false;
				}else{
					return true;
				}
			}
		}).flatMap(new FlatMapFunction<Item,Row>() {

			public Iterator<Row> call(Item value) throws Exception {
				String PID=value.entityId.trim();
				ArrayList<Row> aAliasList=new ArrayList<Row>();
				Item.LanAliaseItem aAliasItem=value.aliases.get("en");
				for(int i=0;i<aAliasItem.itemList.size();i++){
					String tempAlias=aAliasItem.itemList.get(i).value.trim();
					Row aTempLine = RowFactory.create(PID,tempAlias);
					aAliasList.add(aTempLine);
				}
				return aAliasList.iterator();
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		StructField PIDField=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
		StructField AliasName=new StructField("PAlias", DataTypes.StringType, true, Metadata.empty());
		StructField[] aFieldList={PIDField,AliasName};
		StructType schema=DataTypes.createStructType(aFieldList);
		Dataset<Row> result = this.session.createDataFrame(itemAliasRdd, schema);
		result.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
		return result;
	}
	
	public static void createItemContainerTable(String tableName) throws SQLException{
		Connection connection=null;
		Statement statement=null;
		try{
			connection=JDBCUtil.GetConnection();
			statement=connection.createStatement();
			String argu="";
			for(int i=0;i<94;i++){
				argu+="Col_"+i+" BIGINT(64) signed,";
			}
			argu=argu.substring(0,argu.length()-1);
			argu="PID varchar(10) not null primary key,"+argu;
			String dropDatabase="drop table if exists "+tableName;
			String exeStr="Create table "+tableName+" ("+argu+");";
			statement.execute(dropDatabase);
			statement.execute(exeStr);
		}finally{
			//JDBCUtil.CloseResource(connection, statement, null);
			if(connection!=null){
				connection.close();
			}
			if(statement!=null){
				statement.close();
			}
		}
	}
	
	private boolean isWriteToFile(String url){
		return url.contains(FileConstValue.PrefixSaveToFile);
	}
	
	private String getStoreFilePath(String url){
		System.out.println("the url is: "+url);
		String[] aStrList=url.split(":");
		if(aStrList.length==3){
			System.out.println("the length is 2");
			return aStrList[1]+":"+aStrList[2];
		}else{
			System.out.println("the str length is : "+aStrList.length);
			return null;
		}
	}
	
	public Dataset<Row> itemContainerAnalyse(Dataset<Item> originData,String tableName){
		JavaRDD<Row> containRDD = originData.map(new MapFunction<Item,Row>() {

			public Row call(Item originItem) throws Exception {
				long[] aIniArr=new long[AnalysePropertyData.ContainerColCount];
				for(int i=0;i<AnalysePropertyData.ContainerColCount;i++){
					aIniArr[i]=0x0000000000000000L;
				}
				for(Entry<String,Item.Property> entry:originItem.claims.entrySet()){
					int propertyIndex=PropertyDatabaseUtil.GetPropertyIndex(entry.getKey());
					//System.out.println("the property ID is: "+propertyIndex);
					if(propertyIndex==-1){
						//System.out.println("get property index error");
						continue;
					}else{
						int segment=(propertyIndex%64)==0?(propertyIndex/64-1):(propertyIndex/64);
						//System.out.println("the segment is: "+segment);
						int index=(propertyIndex%64)==0?64:(propertyIndex%64);
						//System.out.println("the index is: "+index);
						aIniArr[segment]|=AnalysePropertyData.CodeArr[index-1];
						//System.out.println("the aIniArr is: "+Long.toBinaryString(aIniArr[segment]));
					}
				}
				
				//System.out.println(Long.toHexString(aIniArr[35]));
				Long[] containerLongArr=new Long[AnalysePropertyData.ContainerColCount];
				for(int i=0;i<AnalysePropertyData.ContainerColCount;i++){
					containerLongArr[i]=new Long(aIniArr[i]);
				}
				int iniArrLength=aIniArr.length;  //94
				Object[] aReusltRowArr=new Object[1+AnalysePropertyData.ContainerColCount];
				aReusltRowArr[0]=originItem.entityId;
				System.arraycopy(containerLongArr, 0, aReusltRowArr, 1, iniArrLength);
				return RowFactory.create(aReusltRowArr);
			}
		}, Encoders.bean(Row.class)).javaRDD();
		
		ArrayList<StructField> aFieldList=new ArrayList<StructField>();
		StructField PID=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
		aFieldList.add(PID);
		for(int i=0;i<AnalysePropertyData.ContainerColCount;i++){
			StructField aTempCol=new StructField("Col_"+i, DataTypes.LongType, true, Metadata.empty());
			aFieldList.add(aTempCol);
		}
		StructType schema = DataTypes.createStructType(aFieldList);
		Dataset<Row> containerResult = this.session.createDataFrame(containRDD, schema);
		containerResult.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, 
				JDBCUtil.GetWriteProperties(tableName));
		return containerResult;
	}
	
	/**
	 * 
	 * Description check the dataType of item 
	 * @param originData
	 * Return type: void
	 */
	public void AnalyseTypeInfo(Dataset<Item> originData,String dataTypeTableName){
		JavaRDD<Row> dateTypeRdd = originData.flatMap(new FlatMapFunction<Item,String>() {

			@Override
			public Iterator<String> call(Item item) throws Exception {
				ArrayList<String> aRowList=new ArrayList<String>();
				for(Entry<String,Item.Property> entry:item.claims.entrySet()){
					ArrayList<PropertyInfo> propertyInfoList = entry.getValue().propertyInfos;
					if(propertyInfoList!=null){
						for(int i=0;i<propertyInfoList.size();i++){
							PropertyInfo propertyInfo = propertyInfoList.get(i);
							if(propertyInfo.mainSnak!=null){
								if((propertyInfo.mainSnak.dataType!=null)&&(!propertyInfo.mainSnak.dataType.equals(""))){
									aRowList.add(propertyInfo.mainSnak.dataType);
								}
							}
						}
					}
				}
				if(aRowList.size()>0){
					return aRowList.iterator();
				}else{
					System.out.println("map to String error,the return is null");
					aRowList.add("null");
					return aRowList.iterator();
				}
			}
		}, Encoders.STRING()).javaRDD().mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String value) throws Exception {
				// TODO Auto-generated method stub
				if(value==null){
					System.out.println("the map to pair error,the value is null");
				}
				return new Tuple2<String, String>(value, value);
			}
		}).groupByKey().map(new Function<Tuple2<String,Iterable<String>>, Row>() {
			@Override
			public Row call(Tuple2<String, Iterable<String>> value) throws Exception {
				// TODO Auto-generated method stub
				if(value._1==null){
					System.out.println("the value._1 is null");
				}
				Byte type=new Byte((byte)0);
				return RowFactory.create(value._1,type);
			}
		});
		
		StructField type=new StructField("Type", DataTypes.StringType, true, Metadata.empty());
		StructField dataType=new StructField("DataType", DataTypes.ByteType, true, Metadata.empty());
		StructField[] fieldList={type,dataType};
		StructType schema=DataTypes.createStructType(fieldList);
		this.session.createDataFrame(dateTypeRdd, schema).write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, dataTypeTableName, JDBCUtil.GetWriteProperties(dataTypeTableName));
		
		JavaRDD<Row> typeRdd = originData.flatMap(new FlatMapFunction<Item,String>() {

			@Override
			public Iterator<String> call(Item item) throws Exception {
				ArrayList<String> aRowList=new ArrayList<String>();
				for(Entry<String,Item.Property> entry:item.claims.entrySet()){
					ArrayList<PropertyInfo> propertyInfoList = entry.getValue().propertyInfos;
					if(propertyInfoList!=null){
						for(int i=0;i<propertyInfoList.size();i++){
							PropertyInfo propertyInfo = propertyInfoList.get(i);
							if(propertyInfo.mainSnak!=null){
								if((propertyInfo.mainSnak.dataValue!=null)&&(propertyInfo.mainSnak.dataValue.type!=null)&&(!propertyInfo.mainSnak.dataValue.type.equals(""))){
									aRowList.add(propertyInfo.mainSnak.dataValue.type);
								}
							}
						}
					}
				}
				if(aRowList.size()>0){
					return aRowList.iterator();
				}else{
					aRowList.add("null");
					return aRowList.iterator();
				}
			}
		}, Encoders.STRING()).javaRDD().mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String value) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, String>(value, value);
			}
		}).groupByKey().map(new Function<Tuple2<String,Iterable<String>>, Row>() {
			@Override
			public Row call(Tuple2<String, Iterable<String>> value) throws Exception {
				// TODO Auto-generated method stub
				Byte type=new Byte((byte)1);
				return RowFactory.create(value._1,type);
			}
		});
		
		this.session.createDataFrame(typeRdd, schema).write().mode(SaveMode.Append).jdbc(JDBCUtil.DB_URL, dataTypeTableName, JDBCUtil.GetWriteProperties(dataTypeTableName));
	}
	
}
