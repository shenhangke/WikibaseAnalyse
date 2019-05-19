package org.shk.DataAny;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
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
import org.shk.JsonParse.Item;
import org.shk.JsonParse.Item.LanItem;
import org.shk.JsonParse.Item.Property;
import org.shk.JsonParse.Item.Property.PropertyInfo;
import org.shk.JsonParse.ParseItem;
import org.shk.constValue.FileConstValue;
import org.shk.constValue.SparkConst;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;

import DatabaseUtil.JDBCUtil;
import DatabaseUtil.PropertyDatabaseUtil;

public class AnalysePropertyData implements Serializable{
	
	public static final long[] CodeArr=new long[64];
	public static final int ContainerColCount=94;
	
	private static int DebugCount=0;
	public static final HashMap<String,DataType> dataTypeTrans=new HashMap<String,DataType>();
	public static final HashMap<String,ValueType> valueTypeTrans=new HashMap<String,ValueType>(); 
	public static final HashMap<String,ClaimType> ClaimTypeTrans=new HashMap<String, ClaimType>();
	
	static {
		long iniLong=0x0000000000000001L;
		for(int i=0;i<64;i++){
			CodeArr[i]=iniLong<<i;
			//System.out.println(Long.toBinaryString(CodeArr[i]));
		}
		dataTypeTrans.put("url", DataType.URL);
		dataTypeTrans.put("wikibase-item", DataType.WikiItem);
		dataTypeTrans.put("wikibase-property", DataType.WikiProperty);
		dataTypeTrans.put("external-id", DataType.ExternalID);
		dataTypeTrans.put("quantity", DataType.Quantity);
		dataTypeTrans.put("commonsMedia", DataType.CommonMedia);
		dataTypeTrans.put("wikibase-form", DataType.WikiForm);
		dataTypeTrans.put("wikibase-lexeme", DataType.WikiLexeme);
		dataTypeTrans.put("wikibase-sense", DataType.WikiSense);
		dataTypeTrans.put("math", DataType.Math);
		dataTypeTrans.put("time", DataType.Time);
		dataTypeTrans.put("monolingualtext", DataType.MONOLINGUAL);
		dataTypeTrans.put("string", DataType.STRING);
		
		valueTypeTrans.put("string", ValueType.STRING);
		valueTypeTrans.put("wikibase-entityid", ValueType.WIKIENTITY);
		valueTypeTrans.put("monolingualtext", ValueType.MONOLINGUAL);
		valueTypeTrans.put("quantity", ValueType.QUANTITY);
		valueTypeTrans.put("time", ValueType.TIME);
		
		ClaimTypeTrans.put("statement", ClaimType.STATEMENT);
		ClaimTypeTrans.put("claim", ClaimType.CLAIM);
	}
	
	public enum DataType{
		URL("url",(byte)0),WikiItem("wikibase-item",(byte)1),WikiProperty("wikibase-property",(byte)2),
		ExternalID("external-id",(byte)3),Quantity("quantity",(byte)4),CommonMedia("commonsMedia",(byte)5),
		WikiForm("wikibase-form",(byte)6),WikiLexeme("wikibase-lexeme",(byte)7),WikiSense("wikibase-sense",(byte)8),
		Math("math",(byte)9),Time("time",(byte)10),MONOLINGUAL("monolingualtext",(byte)11),STRING("string",(byte)12);
		
		private byte realValue=-1;
		private String strValue="";
		
		private DataType(String typeString,byte realValue){
			this.realValue=realValue;
			this.strValue=typeString;
		}
		
		public byte getRealValue(){
			return this.realValue;
		}
	}
	
	public enum ValueType{
		STRING((byte)0),WIKIENTITY((byte)1),MONOLINGUAL((byte)2),
		QUANTITY((byte)3),TIME((byte)4);
		
		private byte realValue=0;
		
		private ValueType(byte realValue) {
			this.realValue=realValue;
		}
		
		public byte getReadValue(){
			return this.realValue;
		}
		
	}
	
	public enum ClaimType{
		STATEMENT((byte)0),CLAIM((byte)1);
		
		private byte realValue=-1;
		
		private ClaimType(byte realValue) {
			this.realValue=realValue;
		}
		
		public byte getRealValue(){
			return this.realValue;
		}
	}
	
	public class ValuePair{
		public String para_1="";
		public String para_2="";
		public ValuePair(String para_1,String para_2) {
			this.para_1=para_1;
			this.para_2=para_2;
		}
	}
	
	private static final long serialVersionUID = 1L;
	private SparkSession session=null;
	
	public AnalysePropertyData(SparkSession session) throws Exception {
		if(session!=null){
			this.session=session;
		}else{
			throw new Exception("the session is null,construct failed");
		}
	}
	
	public Dataset<Item> readPropertyDataFromFile(String path){
		DataAnalyse analysor=new DataAnalyse(this.session);
		Dataset<Item> originData=analysor.extractDataItem(path);
		return originData.filter(new FilterFunction<Item>() {

			@Override
			public boolean call(Item value) throws Exception {
				if(value.type==Item.EntityType.Property){
					//To confirm the entity is property type
					return true;
				}else{
					return false;
				}
			}
		});
	}
	
	/**
	 * 
	 * Description 
	 * @param originData
	 * @param fileOrDatabase
	 * @param argu
	 * @return
	 * Return type: Dataset<Row>
	 */
	public Dataset<Row> infoAnalyse(Dataset<Item> originData,SparkConst.FileOrDatabase fileOrDatabase,String argu){
		Dataset<String> infoDataset = originData.map(new MapFunction<Item,String>(){

			@Override
			public String call(Item value) throws Exception {
				//for the property,the id attribute is necessary 
				String pId=value.entityId;
				String pLabel="";
				//there is a situation that the label value is empty
				Item.LanItem labelItem=value.labels.get("en");
				if(labelItem==null){
					for(Entry<String,LanItem> lanItem:value.labels.entrySet()){
						pLabel=lanItem.getValue().value;
						break;
					}
				}else{
					pLabel=labelItem.value;
				}
				if((pLabel.trim().equals(""))||(pLabel==null)){
					pLabel="null";
				}
				String pDescription="";
				Item.LanItem descriptionItem=value.descriptions.get("en");
				if(descriptionItem==null){
					for(Entry<String,LanItem> desItem:value.descriptions.entrySet()){
						pDescription=desItem.getValue().value;
					}
				}else{
					pDescription=descriptionItem.value;
				}
				if((pDescription==null)||(pDescription.trim().equals(""))){
					pDescription="null";
				}
				//return RowFactory.create(pId,pLabel,pDescription);
				return pId+FileConstValue.StrSeparator+pLabel+FileConstValue.StrSeparator+pDescription;
			}
			//the name is nessary
		}, Encoders.STRING());
		
		if(fileOrDatabase==SparkConst.FileOrDatabase.File){
			File storeFile=new File(argu);
			if(storeFile.exists()){
				storeFile.delete();
			}
			infoDataset.write().mode(SaveMode.Overwrite).text(argu);
		}else{
			//Store in DataBase if executor in local or remote server has mysql
		}
		
		return infoDataset.map(new MapFunction<String,Row>(){

			@Override
			public Row call(String value) throws Exception {
				String[] realValueArr=value.split(FileConstValue.StrSeparator);
				if(realValueArr.length==3){
					return RowFactory.create(realValueArr);
				}else{
					System.out.println("the info str is incorrect");
					return null;
				}
			}
			
		}, Encoders.bean(Row.class));
		
	}
	
	/**
	 * 
	 * Description 
	 * @param originDataFromDatabase
	 * @param writeFileOrDataBase
	 * @param argu
	 * Return type: void
	 */
	public void propertyInfoAnalyse(Dataset<Row> originDataFromDatabase,SparkConst.FileOrDatabase writeFileOrDataBase,String argu){
		//Dataset<Row> originData=this.session.read().jdbc(JDBCUtil.DB_URL, JDBCUtil.PropertyItem,JDBCUtil.GetReadProperties(JDBCUtil.PropertyItem));
		Dataset<Item> originItem=originDataFromDatabase.map(new MapFunction<Row,Item>() {

			public Item call(Row aRow) throws Exception {
				return ParseItem.ParseJsonToItem(aRow.getString(1));
			}
		}, Encoders.bean(Item.class));
		JavaRDD<Row> PropertyInfo=originItem.map(new MapFunction<Item,Row>() {

			public Row call(Item aSingleOriginItem) throws Exception {
				String propertyIdString=aSingleOriginItem.entityId.trim();
				String propertyName="";
				Item.LanItem aLanItem=aSingleOriginItem.labels.get("en");
				if(aLanItem==null){
					for(Entry<String,Item.LanItem> aLinelanItem:aSingleOriginItem.labels.entrySet()){
						aLanItem=aLinelanItem.getValue();
						break;
					}
				}
				if(aLanItem!=null){
					//no Item
					propertyName=aLanItem.value.trim();
				}
				String description="";
				/*
				 * if the desctiption is null,the field is Null
				 */
				Item.LanItem aDesItem=aSingleOriginItem.descriptions.get("en");
				if(aDesItem==null){
					description=null;
				}else{
					description=aDesItem.value.trim();
				}
				Object[] aRowList={propertyIdString,propertyName,description};
				return RowFactory.create(aRowList);
			}
		}, Encoders.bean(Row.class)).javaRDD();
		
		StructField PID=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
		StructField PName=new StructField("PName", DataTypes.StringType, true, Metadata.empty());
		StructField PDes=new StructField("PDescription", DataTypes.StringType, true, Metadata.empty());
		StructField[] aFieldList={PID,PName,PDes};
		StructType schema=DataTypes.createStructType(aFieldList);
		Dataset<Row> handleResult = this.session.createDataFrame(PropertyInfo, schema);
		if(writeFileOrDataBase==SparkConst.FileOrDatabase.File){
			File outputFile=new File(argu);
			if(outputFile.exists()){
				outputFile.delete();
			}
			handleResult.write().text(argu);
		}else{
			handleResult.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, argu, JDBCUtil.GetWriteProperties(argu));
		}
	}
	
	public void propertyInfoAnalyse(){
		//this.propertyInfoAnalyse(this.readDataFromDatabase(JDBCUtil.PropertyItem));
	}
	
	public void propertyAliasAnalyse(Dataset<Row> originDataFromDatabase){
		JavaRDD<Row> PropertyAlias=originDataFromDatabase.map(new MapFunction<Row,Item>(){

			public Item call(Row aLineOriginItem) throws Exception {
				return ParseItem.ParseJsonToItem(aLineOriginItem.getString(1));
			}
			
		}, Encoders.bean(Item.class)).filter(new FilterFunction<Item>() {
			
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
		Dataset<Row> result = this.session.createDataFrame(PropertyAlias, schema);
		result.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, JDBCUtil.PropertyAlias, JDBCUtil.GetWriteProperties(JDBCUtil.PropertyAlias));
	}
	
	public void propertyAliasAnalyse(){
		//this.propertyAliasAnalyse(this.readDataFromDatabase(JDBCUtil.PropertyItem));
	}
	
	public void propertyContainProAnalyse(Dataset<Row> originData){
		JavaRDD<Row> containerRdd = originData.map(new MapFunction<Row,Item>(){

			public Item call(Row originLine) throws Exception {
				return ParseItem.ParseJsonToItem(originLine.getString(1));
			}
			
		},Encoders.bean(Item.class)).map(new MapFunction<Item,Row>() {

			public Row call(Item originItem) throws Exception {
				long[] aIniArr=new long[ContainerColCount];
				for(int i=0;i<ContainerColCount;i++){
					aIniArr[i]=0x0000000000000000L;
				}
				for(Entry<String,Item.Property> entry:originItem.claims.entrySet()){
					int propertyIndex=PropertyDatabaseUtil.GetPropertyIndex(entry.getKey(),false);
					//System.out.println("the property ID is: "+propertyIndex);
					if(propertyIndex==-1){
						//System.out.println("get property index error");
						continue;
					}else{
						int segment=(propertyIndex%64)==0?(propertyIndex/64-1):(propertyIndex/64);
						//System.out.println("the segment is: "+segment);
						int index=(propertyIndex%64)==0?64:(propertyIndex%64);
						//System.out.println("the index is: "+index);
						aIniArr[segment]|=CodeArr[index-1];
						//System.out.println("the aIniArr is: "+Long.toBinaryString(aIniArr[segment]));
					}
				}
				
				//System.out.println(Long.toHexString(aIniArr[35]));
				Long[] containerLongArr=new Long[ContainerColCount];
				for(int i=0;i<ContainerColCount;i++){
					containerLongArr[i]=new Long(aIniArr[i]);
				}
				int iniArrLength=aIniArr.length;  //94
				Object[] aReusltRowArr=new Object[1+ContainerColCount];
				aReusltRowArr[0]=originItem.entityId;
				System.arraycopy(containerLongArr, 0, aReusltRowArr, 1, iniArrLength);
				return RowFactory.create(aReusltRowArr);
			}
		}, Encoders.bean(Row.class)).javaRDD();
		
		ArrayList<StructField> aFieldList=new ArrayList<StructField>();
		StructField PID=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
		aFieldList.add(PID);
		for(int i=0;i<ContainerColCount;i++){
			StructField aTempCol=new StructField("Col_"+i, DataTypes.LongType, true, Metadata.empty());
			aFieldList.add(aTempCol);
		}
		StructType schema = DataTypes.createStructType(aFieldList);
		Dataset<Row> containerResult = this.session.createDataFrame(containerRdd, schema);
		containerResult.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, JDBCUtil.PropertryContainTable, 
				JDBCUtil.GetWriteProperties(JDBCUtil.PropertryContainTable));
		/**
		 * debug code
		 */
		/*containerResult.foreach(new ForeachFunction<Row>() {

			public void call(Row value) throws Exception {
				if(DebugCount==0){
					System.out.println(Long.toBinaryString(value.getLong(37)));
					System.out.println(value.getLong(37));
				}
				DebugCount++;
			}
		});*/
		
	}
	
	public void propertyContainProAnalyse(){
		//this.propertyContainProAnalyse(this.readDataFromDatabase(JDBCUtil.PropertyItem));
	}
	
	private ValuePair analyseDataValue(String jsonStr,ValueType type){
		ValuePair valuePair=new ValuePair("", "");
		//System.out.println(jsonStr);
		//JSONObject value=JSONObject.parseObject(jsonStr);
		switch(type){
			case STRING:{
				valuePair.para_1=jsonStr;
				break;
			}
			case WIKIENTITY:{
				JSONObject entityValue=JSONObject.parseObject(jsonStr);
				valuePair.para_1=entityValue.getString("entity-type");
				valuePair.para_2=entityValue.getString("numeric-id");
				break;
			}
			case MONOLINGUAL:{
				JSONObject entityValue=JSONObject.parseObject(jsonStr);
				valuePair.para_1=entityValue.getString("language");
				valuePair.para_2=entityValue.getString("text");
				break;
			}
			case QUANTITY:{
				JSONObject entityValue=JSONObject.parseObject(jsonStr);
				for(Entry<String,Object> entry:entityValue.entrySet()){
					valuePair.para_1+=(String)entry.getKey()+",";
					valuePair.para_2+=(String)entry.getValue()+",";
				}
				valuePair.para_1=valuePair.para_1.substring(0,valuePair.para_1.length()-1);
				valuePair.para_2=valuePair.para_2.substring(0,valuePair.para_2.length()-1);
				break;
			}
			case TIME:{
				JSONObject entityValue=JSONObject.parseObject(jsonStr);
				for(Entry<String,Object> entry:entityValue.entrySet()){
					valuePair.para_1+=(String)entry.getKey()+",";
					valuePair.para_2+=(String)entry.getValue().toString()+",";
				}
				valuePair.para_1=valuePair.para_1.substring(0,valuePair.para_1.length()-1);
				valuePair.para_2=valuePair.para_2.substring(0,valuePair.para_2.length()-1);
				break;
			}
		}
		return valuePair;
	}
	
	
	public void propertyMainSnakAnalyse(Dataset<Row> originData,String tableName){
		JavaRDD<Row> resultRDD = originData.map(new MapFunction<Row,Item>(){

			public Item call(Row aOriginLine) throws Exception {
				return ParseItem.ParseJsonToItem(aOriginLine.getString(1));
			}
			
		}, Encoders.bean(Item.class)).flatMap(new FlatMapFunction<Item,Row>() {

			public Iterator<Row> call(Item aOriginLineItem) throws Exception {
				ArrayList<Row> aMainSnakList=new ArrayList<Row>();
				String originPID=aOriginLineItem.entityId;
				for(Entry<String,Item.Property> entry:aOriginLineItem.claims.entrySet()){
					Property tempProperty = entry.getValue();
					String propertyName=tempProperty.propertyName;
					for(int i=0;i<tempProperty.propertyInfos.size();i++){
						PropertyInfo tempPropertyInfo = tempProperty.propertyInfos.get(i);
						String ID=tempPropertyInfo.id;
						Byte snakType=new Byte(tempPropertyInfo.mainSnak.snakType.getRealValue());
						Byte dataType=null;
						if(tempPropertyInfo.mainSnak.dataType!=null){
							try{
								dataType=new Byte(dataTypeTrans.get(tempPropertyInfo.mainSnak.dataType).getRealValue());
							}catch(Exception e){
								System.out.println("error"+tempPropertyInfo.mainSnak.dataType);
								continue;
							}
						}
						Byte valueType=null;
						if(tempPropertyInfo.mainSnak.dataValue!=null){
							valueType=new Byte(valueTypeTrans.get(tempPropertyInfo.mainSnak.dataValue.type).getReadValue());
						}
						ValuePair aValues=new ValuePair("", "");
						if(valueType!=null){
							aValues=AnalysePropertyData.this.analyseDataValue(tempPropertyInfo.mainSnak.dataValue.value, 
									valueTypeTrans.get(tempPropertyInfo.mainSnak.dataValue.type));
						}
						Byte claimType=null;
						if(tempPropertyInfo.type!=null){
							claimType=new Byte(ClaimTypeTrans.get(tempPropertyInfo.type.getDes()).getRealValue());
						}else{
							System.out.println("the type of claim is null");
						}
						Row aSingleLine = RowFactory.create(ID,originPID,propertyName,snakType,dataType,valueType,aValues.para_1,aValues.para_2,claimType);
						aMainSnakList.add(aSingleLine);
					}
				}
				return aMainSnakList.iterator();
			}
		}, Encoders.bean(Row.class)).javaRDD();
		StructField ID=new StructField("ID", DataTypes.StringType, false, Metadata.empty());
		StructField originPID=new StructField("OriginPID", DataTypes.StringType, false, Metadata.empty());
		StructField pointPID=new StructField("PointPID", DataTypes.StringType, false, Metadata.empty());
		StructField snakType=new StructField("SnakType", DataTypes.ByteType, true, Metadata.empty());
		StructField dataType=new StructField("DataType", DataTypes.ByteType, true, Metadata.empty());
		StructField valueType=new StructField("ValueType", DataTypes.ByteType, true, Metadata.empty());
		StructField value_para1=new StructField("Value_para1", DataTypes.StringType, true, Metadata.empty());
		StructField value_para2=new StructField("Value_para2", DataTypes.StringType, true, Metadata.empty());
		StructField claimTypeField=new StructField("ClaimType", DataTypes.ByteType, true, Metadata.empty());
		StructField[] fieldList={ID,originPID,pointPID,snakType,dataType,valueType,value_para1,value_para2,claimTypeField};
		StructType schema=DataTypes.createStructType(fieldList);
		Dataset<Row> result = this.session.createDataFrame(resultRDD, schema);
		result.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
	}
	
	public void propertyMainSnakAnalyse(){
		//this.propertyMainSnakAnalyse(this.readDataFromDatabase(JDBCUtil.PropertyItem),JDBCUtil.PropertyMainSnak);
	}
}
