package org.shk.DataAny;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.shk.JsonParse.Item;
import org.shk.constValue.FileConstValue;
import org.shk.constValue.SparkConst;

public class App 
{
    public static void main( String[] args ) throws Exception
    {	
    	JavaSparkContext javaContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
    	javaContext.setLogLevel("WARN");
    	DataAnalyse dataAnalyse=new DataAnalyse(SparkConst.MainSession);
    	Dataset<Item> originDataset=dataAnalyse.PreHandleData(FileConstValue.ServerFileName);
    	AnalyseItemData analyseItem=new AnalyseItemData(SparkConst.MainSession);
    	/*
    	 * load class PropertyDatabaseUtil to load the static code
    	 */
    	Class.forName("DatabaseUtil.PropertyDatabaseUtil");
    	/*analyseItem.itemAliasAnalyse(originDataset, FileConstValue.PrefixSaveToFile+
    			FileConstValue.ServerItemAlias_WritePath);*/
    	analyseItem.AnalyseTypeInfo(originDataset, FileConstValue.PrefixSaveToFile+
    			FileConstValue.ServerItemTypeInfo_WritePath);
    }
}

