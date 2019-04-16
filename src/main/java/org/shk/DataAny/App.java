package org.shk.DataAny;

import java.awt.List;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
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
    	/*  	
        SparkSession session=SparkSession.builder().master("local").appName("Test").getOrCreate();
        StructField aTempField=new StructField("name",DataTypes.StringType,true,Metadata.empty());
        StructField aTempField_1=new StructField("id",DataTypes.IntegerType,true,Metadata.empty());
        StructField[] fieldArr={aTempField,aTempField_1};
        JavaRDD<String> aRdd=session.read().textFile("D:\\MyEclpse WorkSpace\\DataAny\\Test.txt").javaRDD();
        StructType schema=DataTypes.createStructType(fieldArr);
        JavaRDD<Row> aRddRow=aRdd.map(new Function<String, Row>() {

			public Row call(String v1) throws Exception {
				// TODO Auto-generated method stub
				String[] aStrArr=v1.split(" ");
				return org.apache.spark.sql.RowFactory.create(aStrArr[0],aStrArr[1]);
			}
		});
        
        Dataset<Row> aOriginDataset=session.createDataFrame(aRddRow, schema);
        
        ArrayList<Row> aRowList=new ArrayList<Row>();
        Row a_1=org.apache.spark.sql.RowFactory.create("shenhangke",2);
        Row a_2=RowFactory.create("qyw",3);
        aRowList.add(a_1);
        aRowList.add(a_2);
        
        Dataset<Row> aOriginDataset_byRow=session.createDataFrame(aRowList, schema);
        aOriginDataset_byRow.show();
        aOriginDataset_byRow.printSchema();
        
        StructField aTempField_2=new StructField("test", DataTypes.IntegerType, true, Metadata.empty());
        StructField[] fieldArr_1={aTempField,aTempField_1,aTempField_2};
        StructType schema_1=DataTypes.createStructType(fieldArr_1);
        int[] intArr={1,2};
        Object[] aObjArr={"shenhhh",new Integer(1),new Integer(1)};
        
        //===============================================================//
        Object[] valueNamesCol={"shenhangketest"};
		Object[] valuesCol={1,2};
		int namesCount=valueNamesCol.length;
		int valuesCount=valuesCol.length;
		//the capacity need to add
		Object[] aLine=Arrays.copyOf(valueNamesCol, namesCount+valuesCount);
		System.arraycopy(valuesCol, 0, aLine, namesCount, valuesCount);
        //===============================================================//
        
        Row b_1=RowFactory.create(aLine);
        ArrayList<Row> aRowList_1=new ArrayList<Row>();
        aRowList_1.add(b_1);
        Dataset<Row> aOri=session.createDataFrame(aRowList_1, schema_1);
        aOri.show();
        aOri.printSchema();
        
        java.util.List<Row> collectAsList = aOri.collectAsList();
        String aFirst=collectAsList.get(0).getString(0);
        System.out.println(aFirst);
        aOri.createOrReplaceTempView("test");
        Dataset<Row> aquery=session.sql("select name from test");
        aquery.show();
        aquery.printSchema(); 
        */
    	//FileSplitUtil.writeFileFormTail(FileConstValue.SourceFilePath, FileConstValue.tailFileTest, 10000);
    	//System.out.println(PropertyDatabaseUtil.MaxLengthOfItemName());
    	//PropertyDatabaseUtil.StoreDataValueType(FileConstValue.SourceFileDir+"monolingualtext.json");
    	//System.out.println(PropertyDatabaseUtil.MaxMainSnakIdLenth());
    	SparkSession session=SparkSession.builder().
				appName("WikiAnalyse").master("local[3]").config("spark.driver.memory","8g").
				config("spark.driver.cores",3).config("spark.executor.memory","2g").getOrCreate();
    	AnalyseItemData itemDataAnalysor=new AnalyseItemData(session);
    	DataAnalyse dataPreHandler=new DataAnalyse(session);
    	Dataset<Item> originData=itemDataAnalysor.filterItemLine(dataPreHandler.PreHandleData(FileConstValue.DivideFilePath));
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
    	Thread handleItemTypeAnaThread=new Thread(){
    		@Override
    		public void run() {
    			itemDataAnalysor.AnalyseTypeInfo(originData, JDBCUtil.ItemTypeAnaTable);
    		}
    	};
    	handleItemTypeAnaThread.start();
    	/*JavaSparkContext javaContext=new JavaSparkContext(session.sparkContext());
    	ArrayList<String> aStringList=new ArrayList<String>();
    	aStringList.add("qu");
    	aStringList.add("er");
    	aStringList.add("qu");
    	aStringList.add("er");
    	aStringList.add("qu");
    	JavaRDD<String> listRdd = javaContext.parallelize(aStringList);
    	JavaRDD<Row> listRow = listRdd.map(new Function<String, Row>() {

			public Row call(String v1) throws Exception {
				return RowFactory.create(v1,1);
			}
    		
    	});
    	StructField field_1=new StructField("type", DataTypes.StringType, true, Metadata.empty());
    	StructField field_2=new StructField("num", DataTypes.IntegerType, true, Metadata.empty());
    	StructField[] fieldList={field_1};
    	StructType schema=DataTypes.createStructType(fieldList);
    	JavaPairRDD<String, Integer> pair = listRow.mapToPair(new PairFunction<Row, String, Integer>() {

			public Tuple2<String, Integer> call(Row t) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String,Integer>(t.getString(0),t.getInt(1));
			}
		});
    	JavaPairRDD<String, Iterable<Integer>> group=pair.groupByKey();
    	JavaRDD<Row> testGroup = group.map(new Function<Tuple2<String,Iterable<Integer>>, Row>() {

			public Row call(Tuple2<String, Iterable<Integer>> v1) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(v1._1);
			}
		});
    	Dataset<Row> result=session.createDataFrame(testGroup, schema);
    	result.show();
    	Iterator<StructField> iterator = result.schema().toIterator();
    	if(iterator.hasNext()){
    		System.out.println(iterator.next().name());
    	}*/
    	//PropertyDatabaseUtil.CreateContainPropertyTable();
    	//System.out.println(Long.SIZE);
    	//PropertyDatabaseUtil.TestOverflow();
    	//PropertyDatabaseUtil.StoreDataValueType(FileConstValue.SourceFileDir+"TEstDT.txt");
    	//AnalyseItemData.createItemContainerTable(JDBCUtil.ItemContainer);
    	
    }
    
    public static void aTest(String a){
    	a="234";
    	return;
    }
}

