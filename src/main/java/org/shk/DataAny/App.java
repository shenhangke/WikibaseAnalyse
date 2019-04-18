package org.shk.DataAny;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.shk.constValue.SparkConst;
import org.shk.fileUtil.FileSplitUtil;

import DatabaseUtil.JDBCUtil;
import DatabaseUtil.PropertyDatabaseUtil;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

public class App 
{
    public static void main( String[] args ) throws Exception
    {	
    	DataAnalyse dataAnalyse=new DataAnalyse(SparkConst.MainSession);
    	Dataset<Item> originDataset=dataAnalyse.PreHandleData(FileConstValue.ServerFileName);
    	AnalyseItemData analyseItem=new AnalyseItemData(SparkConst.MainSession);
    	/*
    	 * load class PropertyDatabaseUtil to load the static code
    	 */
    	Class.forName("DatabaseUtil.PropertyDatabaseUtil");
    	analyseItem.itemAliasAnalyse(originDataset, FileConstValue.PrefixSaveToFile+
    			FileConstValue.ServerItemAlias_WritePath);
    	analyseItem.itemContainerAnalyse(originDataset, FileConstValue.PrefixSaveToFile+
    			FileConstValue.ServerItemContainer_WritePath);
    }
}

