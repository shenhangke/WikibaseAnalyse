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

public class App 
{
    public static void main( String[] args ) throws Exception
    {	
    	DataAnalyse originDataAna=new DataAnalyse(SparkConst.MainSession);
    	AnalysePropertyData analyser=new AnalysePropertyData(SparkConst.MainSession);
    	Dataset<Item> originData=analyser.readPropertyDataFromFile(FileConstValue.ServerOriginFileName);
    	analyser.infoAnalyse(originData, FileOrDatabase.File, FileConstValue.ServerPropertyInfoWritePath);
    }
}

