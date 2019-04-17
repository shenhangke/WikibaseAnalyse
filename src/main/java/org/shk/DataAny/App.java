package org.shk.DataAny;

import java.awt.List;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.shk.DataAny.AnalysePropertyData.DataType;
import org.shk.JsonParse.Item;
import org.shk.JsonParse.ParseItem;
import org.shk.constValue.FileConstValue;
import org.shk.fileUtil.FileSplitUtil;

import DatabaseUtil.JDBCUtil;
import DatabaseUtil.PropertyDatabaseUtil;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

/**
 * Hello world!
 *
 */

class FatherNode{
	public Integer a=0; 
}

class childNode extends FatherNode{
	public Integer b=0;
}

class Father{
	public ArrayList<FatherNode> nodeList=new ArrayList<FatherNode>();
}

class Child extends Father{
	
}

class Test{
	static int a=0;
	
	static{
		a=2;
	}
}

public class App 
{
    public static void main( String[] args ) throws Exception
    {	
    	/*SparkSession session=SparkSession.builder().
				appName("WikiAnalyse").master("local[3]").config("spark.driver.memory","8g").
				config("spark.driver.cores",3).config("spark.executor.memory","2g").getOrCreate();*/
    	
    	SparkSession session=SparkSession.builder().appName("shk_WikiAnalyse").getOrCreate();
    	AnalyseItemData itemDataAnalysor=new AnalyseItemData(session);
    	DataAnalyse dataPreHandler=new DataAnalyse(session);
    	Dataset<Item> originData=itemDataAnalysor.filterItemLine(dataPreHandler.PreHandleData(FileConstValue.ServerFileName));
    	//itemDataAnalysor.itemAliasAnalyse(originData, FileConstValue.PrefixSaveToFile+FileConstValue.ServerItemAlias_WritePath);
    	itemDataAnalysor.itemContainerAnalyse(originData, FileConstValue.PrefixSaveToFile+FileConstValue.ServerItemContainer_WritePath);
    	//itemDataAnalysor.AnalyseTypeInfo(originData, FileConstValue.PrefixSaveToFile+FileConstValue.ServerItemTypeInfo_WritePath);
    	//itemDataAnalysor.itemInfoAnalyse(originData, FileConstValue.PrefixSaveToFile+FileConstValue.ServerInfoFile);
    	/*AnalyseItemData itemDataAnalysor=new AnalyseItemData(session);
    	DataAnalyse dataPreHandler=new DataAnalyse(session);
    	Dataset<Item> originData=itemDataAnalysor.filterItemLine(dataPreHandler.PreHandleData(FileConstValue.DivideFilePath));*/
    	//originData.persist(StorageLevel.MEMORY_AND_DISK());
    	//originData.show();
    	//itemDataAnalysor.itemInfoAnalyse(originData, JDBCUtil.ItemInfo);
    	//itemDataAnalysor.itemAliasAnalyse(originData, JDBCUtil.ItemAlias);
    	//itemDataAnalysor.itemContainerAnalyse(originData, JDBCUtil.ItemContainer);
    	//itemDataAnalysor.AnalyseTypeInfo(originData, JDBCUtil.ItemTypeAnaTable);
    	/*Thread handleItemInfoThread=new Thread(){
    		@Override
    		public void run() {
    			itemDataAnalysor.itemInfoAnalyse(originData, JDBCUtil.ItemInfo);
    		}
    	};
    	handleItemInfoThread.start();
    	Thread handleItemAliasThread=new Thread(){
    		@Override
    		public void run() {
    			itemDataAnalysor.itemAliasAnalyse(originData, JDBCUtil.ItemAlias);
    		}
    	};
    	handleItemAliasThread.start();
    	Thread handleItemContainerThread=new Thread(){
    		@Override
    		public void run() {
    			itemDataAnalysor.itemContainerAnalyse(originData, JDBCUtil.ItemContainer);
    		}
    	};
    	handleItemContainerThread.start();*/
    	/*Thread handleItemTypeAnaThread=new Thread(){
    		@Override
    		public void run() {
    			itemDataAnalysor.AnalyseTypeInfo(originData, JDBCUtil.ItemTypeAnaTable);
    		}
    	};
    	handleItemTypeAnaThread.start();*/
    }
    
    public static void aTest(String a){
    	a="234";
    	return;
    }
}

