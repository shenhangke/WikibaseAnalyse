package DatabaseUtil;

import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
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
	
	public static void ImportDataToDatabase(String dirPath,String tableName){
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
	
	public static void main(String[] args) throws SQLException {
		/**
		 * Create the container table
		 */
		//CreateItemContainerTable();
		
		/**
		 * Import data to itemInfo table
		 */
		System.out.println("import start...");
		ImportDataToDatabase("D:\\MyEclpse WorkSpace\\DataProject_Data\\ItemInfoFile\\ItemInfoFile",JDBCUtil.ItemInfo);
		System.out.println("import finish");
	}
}
