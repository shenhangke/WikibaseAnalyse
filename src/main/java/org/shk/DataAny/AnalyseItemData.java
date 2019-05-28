package org.shk.DataAny;

import java.util.List;
import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.swing.text.DefaultEditorKit.CutAction;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
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
import org.apache.spark.util.LongAccumulator;
import org.shk.DataAny.AnalysePropertyData.DataType;
import org.shk.JsonParse.Item;
import org.shk.JsonParse.Item.Property.PropertyInfo;
import org.shk.constValue.FileConstValue;
import org.shk.constValue.SparkConst;
import org.shk.util.MaxAccumulator;
import org.shk.util.MeanAccumulator;
import org.shk.util.MinAccumulator;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

import DatabaseUtil.JDBCUtil;
import DatabaseUtil.PropertyDatabaseUtil;
import DatabaseUtil.WikibaseInfoConst;
import scala.Tuple2;
import shapeless.newtype;

public class AnalyseItemData implements Serializable{

	private static final long serialVersionUID = 1L;
	private SparkSession session=null; 
	
	public static class CountInfo implements Serializable{
		private static final long serialVersionUID = 1L;
		private long maxIdValue=0;
		private long maxItemCount=0;
		private long minIdValue=0;
		private long maxNameLength=0;
		
		public void setMaxIdValue(long value){
			this.maxIdValue=value;
		}
		
		public void setMaxItemCount(long value){
			this.maxItemCount=value;
		}
		
		public long getMaxIdValue(){
			return this.maxIdValue;
		}
		
		public long getMaxItemCount(){
			return this.maxItemCount;
		}
		
		public void setMinIdValue(long value){
			this.minIdValue=value;
		}
		
		public long getMinIdValue(){
			return this.minIdValue;
		}

		public long getMaxNameLength() {
			return maxNameLength;
		}

		public void setMaxNameLength(long maxNameLength) {
			this.maxNameLength = maxNameLength;
		}
	} 
	
	public static class ContainerBitInfo implements Serializable{

		private static final long serialVersionUID = 1L;
		
		private int segment=0;
		private Long num=0l;
		
		public int getSegment() {
			return segment;
		}
		public void setSegment(int segment) {
			this.segment = segment;
		}
		public Long getNum() {
			return num;
		}
		public void setNum(Long num) {
			this.num = num;
		}
		
	}
	
	public AnalyseItemData(SparkSession session) {
		if(session!=null){
			this.session=session;
		}else{
			throw new NullPointerException("the session is null");
		}
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
	

	
	public Dataset<Item> getItemDataItem(Dataset<Item> originData){
		//there is no need to trigger the calculate
		return originData.filter(new FilterFunction<Item>() {

			@Override
			public boolean call(Item value) throws Exception {
				if(value.type==Item.EntityType.Item){
					return true;
				}else{
					return false;
				}
			}
			
		});
	}
	
	/**
	 * 
	 * Description  Get the max Id count and the item count 
	 * @param originData
	 * @return
	 * Return type: CountInfo
	 */
	public CountInfo getMaxIdNum(Dataset<Item> originData){
		final MaxAccumulator maxIdNumAcc=new MaxAccumulator();
		JavaSparkContext tempContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
		tempContext.sc().register(maxIdNumAcc, "maxIdNumAcc");
		final LongAccumulator countAcc=new LongAccumulator();
		tempContext.sc().register(countAcc,"countAcc");
		final MinAccumulator minIdNumAcc=new MinAccumulator();
		tempContext.sc().register(minIdNumAcc,"minIdNumAcc");
		final MaxAccumulator maxNameLength=new MaxAccumulator();
		tempContext.sc().register(maxNameLength,"maxNameLength");
		originData.foreach(new ForeachFunction<Item>() {
			
			@Override
			public void call(Item value) throws Exception {
				// TODO Auto-generated method stub
				if(!value.entityId.isEmpty()){
					if(value.entityId.toUpperCase().contains("Q")){
						String entityIdNum=value.entityId.substring(1, value.entityId.length());
						long entityIdNumInt=Long.parseLong(entityIdNum);
						maxIdNumAcc.add(entityIdNumInt);
						countAcc.add(1);
						minIdNumAcc.add(entityIdNumInt);
						/**
						 * In order to get the maximum name length,we need to calculate.
						 */
						String name="";
						if(value.labels.get("en")!=null){
							name=value.labels.get("en").value;
						}else{
							//use the first label as entity's name
							for(Entry<String,Item.LanItem> entry:value.labels.entrySet()){
								name=entry.getValue().value;
								break;
							}
						}
						if(!name.trim().equals("")){
							System.out.println("to get the max name length,the name is: "+name);
							System.out.println("the max name length is: "+name.length());
							maxNameLength.add((long)name.length());
						}
					}
				}
			}
		});
		CountInfo returnValue=new CountInfo();
		returnValue.setMaxIdValue(maxIdNumAcc.value());
		returnValue.setMaxItemCount(countAcc.value());
		returnValue.setMaxNameLength(maxNameLength.value());
		returnValue.setMinIdValue(minIdNumAcc.value());
		return returnValue;
		//return maxIdNumAcc.value();
	}
	
	private int IdToIndex(double ratio,int currentID,int minID){
		return (int)(ratio*((double)(currentID-minID)));
	}
	
	/**
	 * 
	 * Description
	 * @param originItemData
	 * @param filePath the path which store the result,if this parament is "" or null,this function will not store any result
	 * @return
	 * Return type: Dataset<Row>
	 */
	public Dataset<Row> getItemInfo(Dataset<Item> originItemData,String filePath){
		//the countInfo has the real value,beacause the getMaxIdNum has trigger the calculate
		CountInfo countInfo = this.getMaxIdNum(originItemData);  //there has executed the action
		System.out.println("the maxIdNum is: "+countInfo.maxIdValue); //the id 
		System.out.println("the maxItemCount is: "+countInfo.maxItemCount);
		System.out.println("the minIdNum is: "+countInfo.minIdValue);
		System.out.println("the maxNameLength is: "+countInfo.maxNameLength);
		//calculate the ratio
		/**
		 * I need to map the all of item id to the block which start 0 to itemCount
		 * the format is: norY=a+k(Y-Min)
		 */
		//double ratio=(double)((countInfo.maxItemCount)/(countInfo.maxIdValue-countInfo.minIdValue));
		double ratio=((double)countInfo.maxItemCount)/((double)(countInfo.maxIdValue-countInfo.minIdValue));
		System.out.println("the ratio is : "+ratio);
		JavaSparkContext tempContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
		//broadcast this ratio
		final Broadcast<Double> ratioBroadcast=tempContext.broadcast(ratio);
		final Broadcast<Long> minIdValue=tempContext.broadcast(countInfo.minIdValue);
		//handle the data
		Dataset<Row> itemInfoOrigin = originItemData.map(new MapFunction<Item,Row>(){

			@Override
			public Row call(Item value) throws Exception {
				//calculate the index
				Double execotorRatio=ratioBroadcast.value();
				if(value.entityId.toUpperCase().contains("Q")){
					//System.out.println("the entity id contain Q");
					String idNum=value.entityId.substring(1,value.entityId.length());
					//System.out.println("the idNum is: "+idNum);
					long idNumLong=Long.parseLong(idNum);
					//the index
					long index=(long)((ratioBroadcast.value()*((double)(idNumLong-minIdValue.value()))));  //it need to be verify
					//String indexStr=String.valueOf(index);
					//the id
					String entityId=value.entityId;
					//the name
					String name="";
					if(value.labels.get("en")!=null){
						name=value.labels.get("en").value;
					}else{
						for(Entry<String,Item.LanItem> entry:value.labels.entrySet()){
							name=entry.getValue().value;
							break;
						}
					}
					//description
					String description="";
					if(value.descriptions.get("en")!=null){
						description=value.descriptions.get("en").value;
					}else{
						for(Entry<String,Item.LanItem> entry:value.descriptions.entrySet()){
							description=entry.getValue().value;
							break;
						}
					}
					return RowFactory.create(index,entityId,name,description);
				}else{
					return null;
				}
				
			}
			
		}, Encoders.bean(Row.class));
		File storeFile=new File(filePath);
		if(storeFile.exists()){
			storeFile.delete();
		}
		StructField qIndex=new StructField("QIndex", DataTypes.LongType, false, Metadata.empty());
		StructField qId=new StructField("QId", DataTypes.StringType, false, Metadata.empty());
		StructField name=new StructField("Name", DataTypes.StringType, false, Metadata.empty());
		StructField description=new StructField("Description", DataTypes.StringType, false, Metadata.empty());
		StructField[] fieldList={qIndex,qId,name,description};
		StructType schema=DataTypes.createStructType(fieldList);
		Dataset<Row> itemInfoResult=SparkConst.MainSession.createDataFrame(itemInfoOrigin.javaRDD(), schema);
		if(!filePath.isEmpty()){
			itemInfoResult.write().mode(SaveMode.Overwrite).csv(filePath);
		}
		return itemInfoResult;
	}
	
	private ContainerBitInfo getSegmentNum(int index){
		ContainerBitInfo containerInfo=new ContainerBitInfo();
		//judge the current property belong to which col
		containerInfo.segment=(int)(index/64);
		long baseLong=0x0000000000000001;
		baseLong=baseLong<<(index%64);
		containerInfo.num=baseLong;
		return containerInfo;
	}
	
	/**
	 * 
	 * Description To get what item contain property,this function will calculator the container information 
	 * @param propertyInfoFilePath the type of file which store the property info is csv 
	 * @param originData
	 * @return
	 * @throws Exception
	 * Return type: Dataset<Row>
	 */
	public Dataset<Row> getItemContainer(String propertyInfoFilePath,Dataset<Item> originData,String dirToStoreResult) throws Exception{
		File propertyFile=new File(propertyInfoFilePath);
		if(false){
			System.out.println("the property file is not exists,getItemContainer exit with exception");
			throw new Exception("the property file is not exists");
		}else{
			Dataset<Row> propertyOriginData = SparkConst.MainSession.read().csv(propertyInfoFilePath);
			//To store the propertyId and index
			/**
			 * this hashmap store the info like (Id,index)
			 */
			HashMap<String,Integer> propertyIndexInfoMap=new HashMap<String, Integer>();
			List<Row> propertyOriginDataList = propertyOriginData.collectAsList();
			for(int i=0;i<propertyOriginDataList.size();i++){
				propertyIndexInfoMap.put(propertyOriginDataList.get(i).getString(1), Integer.parseInt(propertyOriginDataList.get(i).getString(0)));
			}
			//create a broadcast to broadcast the hashMap
			JavaSparkContext tempJavaContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
			//any executor could to get property index info from propertyIndexInfo
			final Broadcast<HashMap<String,Integer>> propertyIndexInfo=tempJavaContext.broadcast(propertyIndexInfoMap);
			JavaRDD<Row> containerInfoRdd = originData.map(new MapFunction<Item,Row>(){

				@Override
				public Row call(Item value) throws Exception {
					long[] propertyContainerInfo=new long[27];
					for(int i=0;i<propertyContainerInfo.length;i++){
						propertyContainerInfo[i]=0x0000000000000000;
					}
					for(Entry<String,Item.Property> entry:value.claims.entrySet()){
						if(propertyIndexInfo.value().get(entry.getKey())!=null){
							Integer propertyIndex=propertyIndexInfo.value().get(entry.getKey());
							ContainerBitInfo itemContainerNumInfo=AnalyseItemData.this.getSegmentNum(propertyIndex);
							propertyContainerInfo[itemContainerNumInfo.segment]=
									propertyContainerInfo[itemContainerNumInfo.segment]|itemContainerNumInfo.num;
						}else{
							continue;
						}
					}
					Object[] rowResult=new Object[28];
					Integer id=Integer.parseInt(value.entityId.substring(1, value.entityId.length()));
					rowResult[0]=id;
					for(int i=1;i<28;i++){
						rowResult[i]=new Long(propertyContainerInfo[i-1]);
					}
					return RowFactory.create(rowResult);
				}
				
			}, Encoders.bean(Row.class)).javaRDD();
			StructField[] fieldList=new StructField[28];
			fieldList[0]=new StructField("ID", DataTypes.IntegerType, false, Metadata.empty());
			for(int i=1;i<28;i++){
				fieldList[i]=new StructField("Col_"+i, DataTypes.LongType, false, Metadata.empty());
			}
			StructType schema=DataTypes.createStructType(fieldList);
			Dataset<Row> containerInfo = SparkConst.MainSession.createDataFrame(containerInfoRdd, schema);
			if(dirToStoreResult!=""){
				containerInfo.write().mode(SaveMode.Overwrite).csv(dirToStoreResult);
			}else{
				containerInfo.count();
			}
			return containerInfo;
		}
	} 
	
	public Dataset<Row> calEntityAliasToID(Dataset<Item> originData,String dirToStore){
		final MaxAccumulator maxAliasLength=new MaxAccumulator();
		JavaSparkContext tempContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
		tempContext.sc().register(maxAliasLength, "maxAliasLength");
		originData=originData.filter(new FilterFunction<Item>() {

			@Override
			public boolean call(Item value) throws Exception {
				return value.aliases.get("en")==null?false:true;
			}
			
		});
		JavaRDD<Row> originItemToRow = originData.flatMap(new FlatMapFunction<Item,Row>(){

			@Override
			public Iterator<Row> call(Item item) throws Exception {
				if(item.aliases.get("en")!=null){
					ArrayList<Row> aRowList=new ArrayList<Row>();
					for(int i=0;i<item.aliases.get("en").itemList.size();i++){
						aRowList.add(RowFactory.create(item.aliases.get("en").itemList.get(i).value,item.entityId));
					}
					return aRowList.size()>0?aRowList.iterator():null;
				}else{
					return null;
				}
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		JavaPairRDD<String, Iterable<Integer>> aliasKeyPairRdd = originItemToRow.mapToPair(new PairFunction<Row, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(Row value) throws Exception {
				Integer entityId=Integer.parseInt(value.getString(1).substring(1,value.getString(1).length()));
				return new Tuple2<String,Integer>(value.getString(0),entityId);
				
			}
		}).groupByKey();
		JavaRDD<Row> resultRdd = aliasKeyPairRdd.map(new Function<Tuple2<String,Iterable<Integer>>, Row>() {

			@Override
			public Row call(Tuple2<String, Iterable<Integer>> value) throws Exception {
				String idArrStr="";
				for(Integer id:value._2){
					idArrStr+=String.valueOf(id)+FileConstValue.StrSeparator;
				}
				idArrStr=idArrStr.substring(0,idArrStr.length()-FileConstValue.StrSeparator.length());
				maxAliasLength.add((long)(value._1.length()));
				return RowFactory.create(value._1,idArrStr);
			}
		});
		StructField alias=new StructField("alias", DataTypes.StringType, false, Metadata.empty());
		StructField idArr=new StructField("idArr", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={alias,idArr};
		StructType schema=DataTypes.createStructType(fieldList);
		Dataset<Row> aliasResult=SparkConst.MainSession.createDataFrame(resultRdd, schema);
		aliasResult.write().mode(SaveMode.Overwrite).csv(dirToStore);
		System.out.println(maxAliasLength.value());
		return aliasResult;
	}
	
	private int getMainSnakCount(Item item){
		int count=0;
		for(Entry<String,Item.Property> claimProperty:item.claims.entrySet()){
			for(int i=0;i<claimProperty.getValue().propertyInfos.size();i++){
				count++;
			}
		}
		return count;
	}
	
	public int getMainsnakPropertyMeanCount(Dataset<Item> originData){
		final MeanAccumulator meanAcc=new MeanAccumulator();
		JavaSparkContext tempContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
		tempContext.sc().register(meanAcc, "meanAcc");
		originData.foreach(new ForeachFunction<Item>() {

			@Override
			public void call(Item value) throws Exception {
				int mainSnakCount=AnalyseItemData.this.getMainSnakCount(value);
				meanAcc.add(new Integer(mainSnakCount));
			}
		});
		return meanAcc.value();
	}
	
	private HashMap<String,Byte> getTypeInfoFromDatabase(String originFilePath){
		List<Row> originDataList = SparkConst.MainSession.read().csv(originFilePath).collectAsList();
		if(originDataList.size()==0){
			return null;
		}
		HashMap<String,Byte> result=new HashMap<String, Byte>();
		for(int i=0;i<originDataList.size();i++){
			result.put(originDataList.get(i).getString(1), new Byte(Byte.parseByte(originDataList.get(i).getString(0))));
		}
		return result;
	} 
	
	public void getMainSnakInfoAndStoreToFile(Dataset<Item> originData,String preFixStoreFileName,String dataTypeFilePath
			,String typeFilePath) throws Exception{
		JavaSparkContext tempContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
		//map dataType and type to broadcast
		HashMap<String, Byte> dataTypeMap = getTypeInfoFromDatabase(dataTypeFilePath);
		if(dataTypeMap==null){
			throw new Exception("the dataType hashMap is null");
		}
		HashMap<String, Byte> typeMap = getTypeInfoFromDatabase(typeFilePath);
		if(typeMap==null){
			throw new Exception("the Type hashMap is null");
		}
		final Broadcast<HashMap<String,Byte>> dataTypeBroadcast=tempContext.broadcast(dataTypeMap);
		final Broadcast<HashMap<String,Byte>> typeBroadcast=tempContext.broadcast(typeMap);
		for(int i=1;i<=WikibaseInfoConst.tableCount;i++){
			int startIndex=((i-1)*(WikibaseInfoConst.wiki_2015_50g_maxItemCount/WikibaseInfoConst.tableCount))+1;
			int endIndex=0;
			if(i==WikibaseInfoConst.tableCount){
				endIndex=WikibaseInfoConst.wiki_2015_50g_maxItemCount;
			}else{
				endIndex=i*(WikibaseInfoConst.wiki_2015_50g_maxItemCount/WikibaseInfoConst.tableCount);
			}
			final Broadcast<Integer> startIndexBroadcast=tempContext.broadcast(new Integer(startIndex));
			final Broadcast<Integer> endIndexBroadcast=tempContext.broadcast(new Integer(endIndex));
			JavaRDD<Row> mainSnakPart = originData.filter(new FilterFunction<Item>(){

				@Override
				public boolean call(Item value) throws Exception {
					int currenID=Integer.parseInt(value.entityId.substring(1,value.entityId.length()));
					int index=AnalyseItemData.this.IdToIndex(WikibaseInfoConst.ratio, currenID, 
							WikibaseInfoConst.minItemId);
					if((index>endIndexBroadcast.value())||(index<startIndexBroadcast.value())){
						return false;
					}else{
						return true;
					}
				}
				
			}).flatMap(new FlatMapFunction<Item,Row>() {

				@Override
				public Iterator<Row> call(Item item) throws Exception {
					ArrayList<Row> mainSnakList=new ArrayList<Row>();
					for(Entry<String,Item.Property> itemMainSnak:item.claims.entrySet()){
						for(int j=0;j<itemMainSnak.getValue().propertyInfos.size();j++){
							if(itemMainSnak.getValue().propertyInfos.get(j).mainSnak.snakType!=Item.MainSnakType.Value){
								continue;
							}
							//if(itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataType)
							Integer ID=new Integer(Integer.parseInt(item.entityId.substring(1,item.entityId.length())));
							Integer PID=new Integer(Integer.parseInt(itemMainSnak.getKey().substring(1,itemMainSnak.getKey().length())));
							Byte snakType=itemMainSnak.getValue().propertyInfos.get(j).mainSnak.snakType.getRealValue();
							Byte dataType=dataTypeBroadcast.getValue().get(itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataType);
							if(dataType==null){
								/*System.out.println(itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataType);
								dataType=10;*/
								if(itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataType==null){
									continue;
								}else{
									//throw new Exception("dataType is null,the dataType name is: "+itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataType);
									dataType=10;
								}
							}
							Byte type=typeBroadcast.getValue().get(itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataValue.type);
							String valueNameArr="";
							String valueArr="";
							try{
								JSONObject mainSnakValue=(JSONObject)JSONObject.parse(itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataValue.value);
								for(Entry<String,Object> dataValue:mainSnakValue.entrySet()){
									valueNameArr+=dataValue.getKey()+FileConstValue.StrSeparator;
									valueArr+=dataValue.getValue().toString()+FileConstValue.StrSeparator;
								}
								valueNameArr=valueNameArr.substring(0,valueNameArr.length()-FileConstValue.StrSeparator.length());
								valueArr=valueArr.substring(0,valueArr.length()-FileConstValue.StrSeparator.length());
								//System.out.println("has more than one value,the name is: "+valueNameArr);
							}catch(ClassCastException e){
								valueArr=itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataValue.value;
								valueNameArr="value";
							}catch(JSONException e){
								/*System.out.println(itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataValue.value);
								System.out.println(item.entityId);
								throw e;*/
								valueArr=itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataValue.value;
								valueNameArr="value";
							}catch(Exception e){
								valueArr=itemMainSnak.getValue().propertyInfos.get(j).mainSnak.dataValue.value;
								valueNameArr="value";
							}
							if(valueNameArr.contains(",")){
								valueNameArr.replace(",", "!!!!!!");
							}
							if(valueArr.contains(",")){
								valueArr.replace(",", "!!!!!!");
							}
							mainSnakList.add(RowFactory.create(ID,PID,snakType,dataType,type,valueNameArr,valueArr));
						}
					}
					return mainSnakList.iterator();
				}
			
			}, Encoders.bean(Row.class)).javaRDD();
			StructField ID=new StructField("QID", DataTypes.IntegerType, false, Metadata.empty());
			StructField propertyId=new StructField("propertyId", DataTypes.IntegerType, false, Metadata.empty());
			StructField snakType=new StructField("snakType", DataTypes.ByteType, false, Metadata.empty());
			StructField dataType=new StructField("dataType", DataTypes.ByteType, false, Metadata.empty());
			StructField type=new StructField("type", DataTypes.ByteType, false, Metadata.empty());
			StructField nameArr=new StructField("nameArr", DataTypes.StringType, false, Metadata.empty());
			StructField valueArr=new StructField("valueArr", DataTypes.StringType, false, Metadata.empty());
			StructField[] fieldList={ID,propertyId,snakType,dataType,type,nameArr,valueArr};
			StructType schema=DataTypes.createStructType(fieldList);
			SparkConst.MainSession.createDataFrame(mainSnakPart, schema).write().mode(SaveMode.Overwrite).csv(preFixStoreFileName+"_"+i);
		}
	}
	
	public void getMainSnakDataTypeInfoCountAndName(Dataset<Item> originData,String dataTypeStoreFilePath,String typeStoreFilePath){
		originData=originData.filter(new FilterFunction<Item>(){

			@Override
			public boolean call(Item value) throws Exception {
				int idNum=Integer.parseInt(value.entityId.substring(1, value.entityId.length()));
				if(idNum>=10000000){
					return true;
				}else{
					return false;
				}
			}
			
		});
		
		JavaRDD<Row> dataTypeAndTypeRdd = originData.flatMap(new FlatMapFunction<Item,Row>() {

			@Override
			public Iterator<Row> call(Item item) throws Exception {
				ArrayList<Row> dataTypeAndTypeNameList=new ArrayList<Row>();
				for(Entry<String,Item.Property> itemMainSnak:item.claims.entrySet()){
					for(int i=0;i<itemMainSnak.getValue().propertyInfos.size();i++){
						if(itemMainSnak.getValue().propertyInfos.get(i).mainSnak.snakType!=Item.MainSnakType.Value){
							continue;
						}
						if(itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataType==null){
							continue;
						}
						if(itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataValue.type==null){
							continue;
						}
						String dataTypeName=itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataType;
						String typeName=itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataValue.type;
						dataTypeAndTypeNameList.add(RowFactory.create(dataTypeName,typeName));
					}
				}
				return dataTypeAndTypeNameList.iterator();
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		JavaRDD<Row> dataTypeNames = dataTypeAndTypeRdd.mapToPair(new PairFunction<Row, String, String>() {

			@Override
			public Tuple2<String, String> call(Row t) throws Exception {
				return new Tuple2<String, String>(t.getString(0),t.getString(0));
			}
			
		}).groupByKey().map(new Function<Tuple2<String,Iterable<String>>, Row>() {

			@Override
			public Row call(Tuple2<String, Iterable<String>> value) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(value._1);
			}
			
		});
		StructField dataTypeName=new StructField("name", DataTypes.StringType, true, Metadata.empty());
		StructField[] dataTypeFieldList={dataTypeName};
		StructType schema=DataTypes.createStructType(dataTypeFieldList);
		SparkConst.MainSession.createDataFrame(dataTypeNames, schema).write().mode(SaveMode.Overwrite).csv(dataTypeStoreFilePath);
		
		/*System.gc();
		
		JavaRDD<Row> typeNames = dataTypeAndTypeRdd.mapToPair(new PairFunction<Row, String, Row>() {

			@Override
			public Tuple2<String, Row> call(Row t) throws Exception {
				return new Tuple2<String, Row>(t.getString(1),t);
			}
			
		}).groupByKey().map(new Function<Tuple2<String,Iterable<Row>>, Row>() {

			@Override
			public Row call(Tuple2<String, Iterable<Row>> value) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(value._1);
			}
			
		});
		
		SparkConst.MainSession.createDataFrame(typeNames, schema).write().mode(SaveMode.Overwrite).csv(typeStoreFilePath);
		System.gc();*/
		
		
	}
	
	public void getTypeInfo(Dataset<Item> originData,String storeFilePath){
		originData=originData.filter(new FilterFunction<Item>(){

			@Override
			public boolean call(Item value) throws Exception {
				int idNum=Integer.parseInt(value.entityId.substring(1, value.entityId.length()));
				if(idNum>=10000000){
					return true;
				}else{
					return false;
				}
			}
			
		});
		
		JavaRDD<Row> dataTypeAndTypeRdd = originData.flatMap(new FlatMapFunction<Item,Row>() {

			@Override
			public Iterator<Row> call(Item item) throws Exception {
				ArrayList<Row> dataTypeAndTypeNameList=new ArrayList<Row>();
				for(Entry<String,Item.Property> itemMainSnak:item.claims.entrySet()){
					for(int i=0;i<itemMainSnak.getValue().propertyInfos.size();i++){
						if(itemMainSnak.getValue().propertyInfos.get(i).mainSnak.snakType!=Item.MainSnakType.Value){
							continue;
						}
						if(itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataType==null){
							continue;
						}
						if(itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataValue.type==null){
							continue;
						}
						String dataTypeName=itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataType;
						String typeName=itemMainSnak.getValue().propertyInfos.get(i).mainSnak.dataValue.type;
						dataTypeAndTypeNameList.add(RowFactory.create(dataTypeName,typeName));
					}
				}
				return dataTypeAndTypeNameList.iterator();
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		JavaRDD<Row> dataTypeNames = dataTypeAndTypeRdd.mapToPair(new PairFunction<Row, String, String>() {

			@Override
			public Tuple2<String, String> call(Row t) throws Exception {
				return new Tuple2<String, String>(t.getString(1),t.getString(1));
			}
			
		}).groupByKey().map(new Function<Tuple2<String,Iterable<String>>, Row>() {

			@Override
			public Row call(Tuple2<String, Iterable<String>> value) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(value._1);
			}
			
		});
		StructField dataTypeName=new StructField("name", DataTypes.StringType, true, Metadata.empty());
		StructField[] dataTypeFieldList={dataTypeName};
		StructType schema=DataTypes.createStructType(dataTypeFieldList);
		SparkConst.MainSession.createDataFrame(dataTypeNames, schema).write().mode(SaveMode.Overwrite).csv(storeFilePath);
	}
	
	
}
