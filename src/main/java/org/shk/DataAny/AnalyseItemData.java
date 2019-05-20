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
import org.shk.util.MinAccumulator;

import DatabaseUtil.JDBCUtil;
import DatabaseUtil.PropertyDatabaseUtil;
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
	
}
