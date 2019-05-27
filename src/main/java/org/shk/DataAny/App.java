package org.shk.DataAny;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.shk.JsonParse.Item;
import org.shk.constValue.FileConstValue;
import org.shk.constValue.SparkConst;
import org.shk.constValue.SparkConst.FileOrDatabase;
import org.shk.getHardWareInfo.HardUtil;
import org.shk.util.EncodingDetect;
import org.apache.commons.io.FileUtils;

public class App 
{
    public static void main( String[] args ) throws Exception
    {	
    	//==========================================================================================//
    	/**
    	 * run code
    	 */
    	DataAnalyse originDataAna=new DataAnalyse(SparkConst.MainSession);
    	AnalyseItemData itemAna=new AnalyseItemData(SparkConst.MainSession);
    	
    	/**
    	 * Analyzing the itemInfo
    	 */
    	/*itemAna.getItemInfo(itemAna.getItemDataItem(originDataAna.extractDataItem(FileConstValue.ServerOriginFileName)), 
    			FileConstValue.ServerItemInfoPath);*/
    	
    	/**
    	 * Analyzing the containerInfo
    	 */
    	//Because the program which is storing the iteminfo to database,there need to generate a new propertyInfo file
    	/*itemAna.getItemContainer(FileConstValue.ServerPropertyInfoFileDir, 
    			itemAna.getItemDataItem(originDataAna.extractDataItem(FileConstValue.ServerOriginFileName)),
    			FileConstValue.ServerItemContainer_WritePath);*/
    	
    	
    	/**
    	 * Analyzing the alias info
    	 */
    	/*itemAna.calEntityAliasToID(itemAna.getItemDataItem(originDataAna.extractDataItem(FileConstValue.ServerOriginFileName)), 
    			FileConstValue.ServerItemAlias_WritePath);*/
    	
    	/**
    	 * calculate the average mainsnak count
    	 */
    	/*System.out.println(itemAna.getMainsnakPropertyMeanCount(
    			itemAna.getItemDataItem(originDataAna.extractDataItem(FileConstValue.ServerOriginFileName))));*/
    	
    	/**
    	 * get the dataType and type 
    	 */
    	/*itemAna.getTypeInfo(itemAna.getItemDataItem(originDataAna.extractDataItem(FileConstValue.ServerOriginFileName)),
    			FileConstValue.ServerTypeNameDir);*/
    	itemAna.getMainSnakInfoAndStoreToFile(itemAna.getItemDataItem(originDataAna.extractDataItem(FileConstValue.ServerOriginFileName)), 
    			FileConstValue.MainSnakPreFix, 
    			FileConstValue.DataTypeFilePath, 
    			FileConstValue.TypeFilePath);
    	//============================================================================================//
    }
}

