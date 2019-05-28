package DatabaseUtil;

import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
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
import org.shk.constValue.FileConstValue;
import org.shk.constValue.SparkConst;

import breeze.linalg.split;

public class ItemDataBaseUtil implements Serializable{

	private static final long serialVersionUID = 1L;
	private SparkSession session=null;
	
	
	
	public ItemDataBaseUtil(SparkSession session) {
		if(session==null){
			throw new NullPointerException("the session is null");
		}else{
			this.session=session;
		}
	}

	public ArrayList<String> getFileList(String dirName){
		File rootDir=new File(dirName);
		if(!rootDir.exists()){
			return null;
		}else{
			ArrayList<String> aFileNameList=new ArrayList<String>();
			File[] files = rootDir.listFiles();
			for(File aTempFile:files){
				if(aTempFile.isFile()){
					aFileNameList.add(aTempFile.getAbsolutePath());
				}
			}
			return aFileNameList;
		}
	}
	
	public void handleItemInfoFromFile(ArrayList<String> fileList){
		int handleFileCount=0;
		Iterator<String> fileIterator = fileList.iterator();
		while(fileIterator.hasNext()){
			this.handleItemInfoFromFile(fileIterator.next());
			//handleFileCount++;
			System.out.println(handleFileCount++);
		}
	}
	
	public void handleItemInfoFromFile(String fileName){
		if(this.session!=null){
			Dataset<String> textFileDataset = this.session.read().textFile(fileName);
			JavaRDD<Row> itemInfoOriginData = textFileDataset.map(new MapFunction<String,Row>(){

				@Override
				public Row call(String value) throws Exception {
					String[] splitArr = value.split("&&&&");
					if(splitArr.length==3){
						return RowFactory.create(splitArr);
					}else{
						System.out.println("the item info length is not 3");
						return null;
					}
				}
				
			}, Encoders.bean(Row.class)).javaRDD();
			StructField id=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
			StructField name=new StructField("PName", DataTypes.StringType, true, Metadata.empty());
			StructField description=new StructField("PDescription", DataTypes.StringType, true, Metadata.empty());
			StructField[] fieldList={id,name,description};
			StructType schema=DataTypes.createStructType(fieldList);
			this.session.createDataFrame(itemInfoOriginData, schema)
			.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL,JDBCUtil.ItemInfo, JDBCUtil.GetWriteProperties(JDBCUtil.ItemInfo));
		}
	}
	
	public static void CreateItemContainerTable() throws SQLException{
		Connection connection=null;
		Statement statement=null;
		try{
			connection=JDBCUtil.GetConnection();
			statement=connection.createStatement();
			String argu="";
			for(int i=0;i<27;i++){
				argu+="Col_"+i+" BIGINT(64) signed,";
			}
			argu=argu.substring(0,argu.length()-1);
			argu="ID INTEGER signed not null primary key,"+argu;
			String dropExistsTable="drop table if exists "+JDBCUtil.ItemContainer;
			String createTable="create table "+JDBCUtil.ItemContainer+" ("+argu+");";
			System.out.println(createTable);
			statement.execute(dropExistsTable);
			statement.execute(createTable);
		}finally{
			if(connection!=null){
				connection.close();
			}
			if(statement!=null){
				statement.close();
			}
		}
	}
	
	public static void ImportInfoDataToDatabase(String dirPath,String tableName){
		Dataset<Row> originData = SparkConst.MainSession.read().csv(dirPath);
		JavaRDD<Row> handledDataRdd = originData.map(new MapFunction<Row,Row>(){

			@Override
			public Row call(Row value) throws Exception {
				// TODO Auto-generated method stub
				String idNumStr=value.getString(1).substring(1,value.getString(1).length());
				Integer idNum=Integer.parseInt(idNumStr);
				return RowFactory.create(Integer.parseInt(value.getString(0)),idNum,value.getString(2),value.getString(3));
			}}, Encoders.bean(Row.class)).javaRDD();
		StructField qIndex=new StructField("QIndex", DataTypes.IntegerType, false, Metadata.empty());
		StructField id=new StructField("ID", DataTypes.IntegerType, false, Metadata.empty());
		StructField name=new StructField("Name", DataTypes.StringType, true, Metadata.empty());
		StructField description=new StructField("Description", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={qIndex,id,name,description};
		StructType schema=DataTypes.createStructType(fieldList);
		SparkConst.MainSession.createDataFrame(handledDataRdd, schema).write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
	}
	
	public static void ImpoertContainerDataToDatabase(String dirPath,String tableName){
		Dataset<Row> originData=SparkConst.MainSession.read().csv(dirPath);
		JavaRDD<Row> resultRdd = originData.map(new MapFunction<Row,Row>(){

			@Override
			public Row call(Row value) throws Exception {
				// TODO Auto-generated method stub
				Object[] rowList=new Object[28];
				rowList[0]=(Integer)(new Integer(Integer.parseInt(value.getString(0))));
				for(int i=1;i<28;i++){
					rowList[i]=(Long)(new Long(Long.parseLong(value.getString(i))));
				}
				return RowFactory.create(rowList);
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		StructField[] fieldList=new StructField[28];
		fieldList[0]=new StructField("ID", DataTypes.IntegerType, false, Metadata.empty());
		for(int i=0;i<27;i++){
			fieldList[i+1]=new StructField("Col_"+i, DataTypes.LongType, false, Metadata.empty());
		}
		StructType schema=DataTypes.createStructType(fieldList);
		SparkConst.MainSession.createDataFrame(resultRdd, schema).write().
		mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
	}
	
	
	public static void ImportAliasDataToDatabase(String dirPath,String tableName){
		JavaRDD<Row> originData=SparkConst.MainSession.read().csv(dirPath).javaRDD();
		StructField entityName=new StructField("alias", DataTypes.StringType, false, Metadata.empty());
		StructField IdArr=new StructField("IdArr", DataTypes.StringType, false, Metadata.empty());
		StructField[] fieldList={entityName,IdArr};
		StructType schema=DataTypes.createStructType(fieldList);
		SparkConst.MainSession.createDataFrame(originData, schema).write().
		mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
	}
	
	public static void CreateMainSnakTables(int tableCount) throws SQLException{
		Connection connection=null;
		Statement state=null;
		try{
			connection=JDBCUtil.GetConnection();
			state=connection.createStatement();
			for(int i=0;i<tableCount;i++){
				String createTableSql="create table mainSnak_"+i+" "
						+ "(QID int signed,propertyId smallint signed,snakType tinyint signed,dataType tinyint signed,"
						+ "type tinyint signed,"
						+ "nameArr text,valueArr text,key ItemAndPropertyID (QID,propertyId))";
				state.execute(createTableSql);
			}
		}finally{
			JDBCUtil.CloseResource(connection, state, null);
			//state.close();
		}
	}
	
	public static void ImportDataTypeInfoToDatabase(String[] fileDir,String tableName){
		StructField dataTypeName=new StructField("dataTypeName", DataTypes.StringType, false, Metadata.empty());
		StructField[] fieldList={dataTypeName};
		StructType schema=DataTypes.createStructType(fieldList);
		ArrayList<Dataset<Row>> rddList=new ArrayList<Dataset<Row>>();
		for(int i=0;i<fileDir.length;i++){
			JavaRDD<Row> tempPart = SparkConst.MainSession.read().csv(fileDir[i]).javaRDD();
			//rddList.add(tempPart);
			rddList.add(SparkConst.MainSession.createDataFrame(tempPart, schema));
		}
		Dataset<Row> totalRow=rddList.get(0);
		for(int i=1;i<rddList.size();i++){
			totalRow=totalRow.union(rddList.get(i));
		}
		//totalRow.distinct().show();
		totalRow.distinct().write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
		
	}
	
	public static void ImportTypeInfoToDatabase(String[] files,String tableName){
		StructField dataTypeName=new StructField("typeName", DataTypes.StringType, false, Metadata.empty());
		StructField[] fieldList={dataTypeName};
		StructType schema=DataTypes.createStructType(fieldList);
		ArrayList<Dataset<Row>> rddList=new ArrayList<Dataset<Row>>();
		for(int i=0;i<files.length;i++){
			JavaRDD<Row> tempPart = SparkConst.MainSession.read().csv(files[i]).javaRDD();
			//rddList.add(tempPart);
			rddList.add(SparkConst.MainSession.createDataFrame(tempPart, schema));
		}
		Dataset<Row> totalRow=rddList.get(0);
		for(int i=1;i<rddList.size();i++){
			totalRow=totalRow.union(rddList.get(i));
		}
		totalRow.distinct().write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
	}
	
	public static void storeDataToFile(String tableName,String filePath){
		SparkConst.MainSession.read().
		jdbc(JDBCUtil.DB_URL, tableName, JDBCUtil.GetReadProperties(tableName)).write().mode(SaveMode.Overwrite)
		.csv(filePath);
	}
	
	
	public static void ImportMainSnakToDatabase(String tableName,String filePath){
		StructField ID=new StructField("QID", DataTypes.IntegerType, false, Metadata.empty());
		StructField propertyId=new StructField("propertyId", DataTypes.IntegerType, false, Metadata.empty());
		StructField snakType=new StructField("snakType", DataTypes.ByteType, false, Metadata.empty());
		StructField dataType=new StructField("dataType", DataTypes.ByteType, false, Metadata.empty());
		StructField type=new StructField("type", DataTypes.ByteType, false, Metadata.empty());
		StructField nameArr=new StructField("nameArr", DataTypes.StringType, true, Metadata.empty());
		StructField valueArr=new StructField("valueArr", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={ID,propertyId,snakType,dataType,type,nameArr,valueArr};
		StructType schema=DataTypes.createStructType(fieldList);
		JavaRDD<Row> originDataRdd = SparkConst.MainSession.read().csv(filePath).filter(new FilterFunction<Row>(){

			@Override
			public boolean call(Row value) throws Exception {
				try{
					Integer ID=new Integer(Integer.parseInt(value.getString(0)));
				}catch(Exception e){
					System.out.println("filter the value 0 is: "+value.getString(0));
					return false;
				}
				return true;
			}
			
		}).map(new MapFunction<Row,Row>(){

			@Override
			public Row call(Row value) throws Exception {
				Integer ID=0;
				try{
					ID=new Integer(Integer.parseInt(value.getString(0)));
				}catch(Exception e){
					System.out.println(value.getString(0));
					return null;
					//throw new Exception("the ID has illage input,the input is: "+value.getString(0));
				}
				Integer propertyID=new Integer(Integer.parseInt(value.getString(1)));
				Byte snakType=new Byte(Byte.parseByte(value.getString(2)));
				Byte dataType=new Byte(Byte.parseByte(value.getString(3)));
				Byte type=new Byte(Byte.parseByte(value.getString(4)));
				String nameArr=value.getString(5);
				String valueArr=value.getString(6);
				return RowFactory.create(ID,propertyID,snakType,dataType,type,nameArr,valueArr);
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		SparkConst.MainSession.createDataFrame(originDataRdd, schema).write().mode(SaveMode.Overwrite).jdbc(
				JDBCUtil.DB_URL, tableName, JDBCUtil.GetWriteProperties(tableName));
	}
	
	
	public static void main(String[] args) throws SQLException {
		/**
		 * Create the container table
		 */
		//CreateItemContainerTable();
		
		/**
		 * Import data to itemInfo table
		 */
		System.out.println("import start...");
		//ImportInfoDataToDatabase("D:\\MyEclpse WorkSpace\\DataProject_Data\\ItemInfoFile\\ItemInfoFile",JDBCUtil.ItemInfo);
		//ImpoertContainerDataToDatabase("D:\\MyEclpse WorkSpace\\DataProject_Data\\ItemContainerInfo\\ItemContainerInfo",JDBCUtil.ItemContainer);
		//ImportAliasDataToDatabase("D:\\MyEclpse WorkSpace\\DataProject_Data\\ItemAliasInfo\\ItemAliasInfo",JDBCUtil.ItemAlias);
		/*String[] dirList={"D:\\MyEclpse WorkSpace\\DataProject_Data\\DataTypeNames_minto10000000\\DataTypeNames","D:\\MyEclpse WorkSpace\\DataProject_Data\\DataTypeNames_maxto10000000"};
		ImportDataTypeInfoToDatabase(dirList,JDBCUtil.DataTypeNameTable);*/
		//CreateMainSnakTables(WikibaseInfoConst.tableCount);
		/*String[] dirList={"D:\\MyEclpse WorkSpace\\DataProject_Data\\TypeNames_minto10000000",
				"D:\\MyEclpse WorkSpace\\DataProject_Data\\TypeNames_maxto10000000"};
		ImportTypeInfoToDatabase(dirList,JDBCUtil.TypeInfo);*/
		//storeDataToFile(JDBCUtil.DataTypeNameTable,"D:\\MyEclpse WorkSpace\\DataAny\\Data\\DataType");
		ImportMainSnakToDatabase(JDBCUtil.PrefixMainSnak+"0","D:\\MyEclpse WorkSpace\\DataProject_Data\\mainSnak\\mainSnak_1");
		System.out.println("import finish");
	}
	
}
