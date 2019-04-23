package DatabaseUtil;

import java.io.File;
import java.io.Serializable;
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
	
	/*public static void main(String[] args) {
		ItemDataBaseUtil aItemUtil=new ItemDataBaseUtil(SparkConst.MainSession);
		ArrayList<String> fileList = aItemUtil.getFileList(FileConstValue.HandledItemInfoFileDir);
		aItemUtil.handleItemInfoFromFile(fileList);
	}*/
}
