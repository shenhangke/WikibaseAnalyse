package DatabaseUtil;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.AnalysisException;
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
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import org.shk.DataAny.App;
import org.shk.JsonParse.Item;
import org.shk.JsonParse.Item.Property;
import org.shk.JsonParse.Item.Property.PropertyInfo;
import org.shk.JsonParse.ParseItem;
import org.shk.constValue.FileConstValue;
import org.shk.constValue.SparkConst;
import org.shk.util.MaxAccumulator;

import com.alibaba.fastjson.JSONObject;

public class PropertyDatabaseUtil {
	private static final String GetLineFromDatabaseStatement="select Item from "+JDBCUtil.PropertyItem+" where PID=?;";
	public static Broadcast<HashMap<String, Integer>> BroadcastIndex=null;
	
	/*static{
		Dataset<String> inputPropertyInfo=SparkConst.MainSession.read().textFile(FileConstValue.ServerPropertyDataFileReadPath);
		JavaRDD<Row> handledRdd = inputPropertyInfo.map(new MapFunction<String,Row>(){

			@Override
			public Row call(String value) throws Exception {
				String[] propertyList=value.split(",");
				if(propertyList.length==2){
					for(int i=0;i<propertyList.length;i++){
						propertyList[i]=propertyList[i].replace("[", "");
						propertyList[i]=propertyList[i].replace("]", "");
					}
					return RowFactory.create(propertyList);
				}else{
					return RowFactory.create("","");
				}
			}
		}, Encoders.bean(Row.class)).javaRDD();
		
		StructField index=new StructField("PIndex", DataTypes.StringType, true, Metadata.empty());
		StructField id=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={index,id};
		StructType schema=DataTypes.createStructType(fieldList);
		try{
			List<Row> propertyIndexList = SparkConst.MainSession.createDataFrame(handledRdd, schema).collectAsList();
			HashMap<String,Integer> propertyIndex=new HashMap<String,Integer>();
			for(int i=0;i<propertyIndexList.size();i++){
				propertyIndex.put(propertyIndexList.get(i).getString(1), 
						new Integer(propertyIndexList.get(i).getString(0)));
			}
			JavaSparkContext javaContext=
					new JavaSparkContext(SparkConst.MainSession.sparkContext());
			BroadcastIndex= javaContext.broadcast(propertyIndex);
			//SparkConst.MainSession.sparkContext().broadcast(propertyIndex, evidence$11)
		}catch(NumberFormatException e){
			System.out.println("transform string index to int index error,the erroe message is: "+e.getMessage());
		}catch(Exception e){
			System.out.println("init propertyUtil error,the error message is: "+e.getMessage());
		}
	}*/
	
	public static void WriteOneLineItemTOFile(String fileName,String pID) throws SQLException{
		Connection connection=null;
		PreparedStatement statement=null;
		ResultSet queryResult=null;
		try{
			String result="";
			connection=JDBCUtil.GetConnection();
			statement=connection.prepareStatement(GetLineFromDatabaseStatement);
			statement.setString(1, pID);
			queryResult=statement.executeQuery();
			if(queryResult!=null){
				if(true){
					if(queryResult.next()!=false){
						result=queryResult.getString(1);
					}
				}
			}
			if(result!=""){
				File outFile=new File(fileName);
				if(!outFile.exists()){
					outFile.createNewFile();
				}
				FileUtils.writeStringToFile(outFile, result, "UTF-8");
			}
		}catch(Exception e){
			System.out.println("Wirte line to file form database failed,the error message is: "+e.getMessage());
		}finally{
			JDBCUtil.CloseResource(connection, statement, queryResult);
		}
	}
	
	public static void StoreDataValueType(String outFileName) throws SQLException{
		Connection connection=null;
		PreparedStatement statement=null;
		ResultSet queryResult=null;
		try{
			connection=JDBCUtil.GetConnection();
			String model="select Item from "+JDBCUtil.PropertyItem;
			statement=connection.prepareStatement(model);
			queryResult=statement.executeQuery();
			int count=0;
			//System.out.println(queryResult.);
			if(queryResult.next()){
				do{
					System.out.println(count);
					count++;
					//Count the types of data
					Item originItem=ParseItem.ParseJsonToItem(queryResult.getString(1));
					if(originItem!=null){
						Map<String,Item.Property> claims=originItem.claims;
						if(claims==null){
							System.out.println("the claims is null");
							continue;
						}
						for(Entry<String,Item.Property> entry:claims.entrySet()){
							Property tempProperty = entry.getValue();
							if(tempProperty==null){
								System.out.println("the tempProperty is null");
							}
							for(int i=0;i<tempProperty.propertyInfos.size();i++){
								PropertyInfo propertyInfo = tempProperty.propertyInfos.get(i);
								if(propertyInfo.mainSnak.dataValue==null){
									continue;
								}
								if(propertyInfo.mainSnak.dataType.equals("math")){
									System.out.println(propertyInfo.mainSnak.dataValue.value);
								}
								String tempType=propertyInfo.mainSnak.dataValue.type;
								System.out.println(propertyInfo.mainSnak.dataType);
								PreparedStatement queryTypeExists=connection.prepareStatement("select type from TypeOfProperty where type=?");
								queryTypeExists.setString(1, tempType);
								ResultSet queryExistsResult=queryTypeExists.executeQuery();
								try{
									if(!queryExistsResult.next()){
										//add the type to database
										PreparedStatement addTypeToDatabasestatement=connection.prepareStatement("insert into TypeOfProperty (type) values (?);");
										addTypeToDatabasestatement.setString(1, tempType);
										addTypeToDatabasestatement.execute();
										if(addTypeToDatabasestatement!=null){
											addTypeToDatabasestatement.close();
										}
									}
								}finally{
									if(queryExistsResult!=null){
										queryExistsResult.close();
									} 
									if(queryTypeExists!=null){
										queryTypeExists.close();
									}
								}
							}
						}
					}
				}while(queryResult.next());
			}
		}catch(Exception e){
			System.out.println("the failed reason is: "+e.getMessage());
		}finally{
			JDBCUtil.CloseResource(connection, statement, queryResult);
			System.out.println("add type has finish");
		}
	}
	
	public static int MaxLengthOfItemName() throws SQLException{
		Connection connection=null;
		PreparedStatement statement=null;
		ResultSet queryResult=null;
		try{
			connection=JDBCUtil.GetConnection();
			String model="select Item from "+JDBCUtil.PropertyItem;
			statement=connection.prepareStatement(model);
			queryResult=statement.executeQuery();
			int max=-1;
			if(queryResult.next()){
				do{
					//JSONObject originJson=JSONObject.parseObject(queryResult.getString(1));
					Item originItem=ParseItem.ParseJsonToItem(queryResult.getString(1));
					String propertyName="";
					if(originItem!=null){
						Item.LanAliaseItem aTempLanItem=originItem.aliases.get("en");
						if(aTempLanItem!=null){
							propertyName=aTempLanItem.itemList.get(0).value;
						}else{
							for(Entry<String,Item.LanAliaseItem> entry:originItem.aliases.entrySet()){
								propertyName=entry.getValue().itemList.get(0).value;
								break;
							}
						}
						if(propertyName.length()>max){
							max=propertyName.length();
						}
					}
				}while(queryResult.next());
			}
			return max;
		}catch(Exception e){
			System.out.println("get max length error,the error message is: "+e.getMessage());
			return -1;
		}finally{
			JDBCUtil.CloseResource(connection, statement, queryResult);
		}
		
	}
	
	public static void CreateContainPropertyTable() throws SQLException{
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
			argu="PID varchar(6) not null primary key,"+argu;
			String dropDatabase="drop table if exists "+JDBCUtil.PropertryContainTable;
			String exeStr="Create table "+JDBCUtil.PropertryContainTable+" ("+argu+");";
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
	
	public static int MaxMainSnakIdLenth() throws SQLException, ParseException{
		Connection connection=null;
		PreparedStatement statement=null;
		ResultSet queryResult=null;
		try{
			connection=JDBCUtil.GetConnection();
			String model="select Item from "+JDBCUtil.PropertyItem;
			statement=connection.prepareStatement(model);
			queryResult=statement.executeQuery();
			int max=-1;
			if(queryResult.next()){
				do{
					Item originItem=ParseItem.ParseJsonToItem(queryResult.getString(1));
					for(Entry<String,Item.Property> entry:originItem.claims.entrySet()){
						Property tempProperty = entry.getValue();
						for(int i=0;i<tempProperty.propertyInfos.size();i++){
							PropertyInfo tempPropertyInfo = tempProperty.propertyInfos.get(i);
							String propertyId=tempPropertyInfo.id;
							if(propertyId.length()>max){
								max=propertyId.length();
							}
						}
					}
				}while(queryResult.next());
			}
			return max;
		}finally{
			JDBCUtil.CloseResource(connection, statement, queryResult);
		}
	}
	
	private static int GetPropertyIndex(String PropertyID) throws SQLException{
		Connection connection=null;
		PreparedStatement statement=null;
		ResultSet queryResult=null;
		try{
			connection=JDBCUtil.GetConnection();
			String model="select PIndex from "+JDBCUtil.PropertyInfoTable+" where PID=?;";
			statement=connection.prepareStatement(model);
			statement.setString(1, PropertyID);
			queryResult=statement.executeQuery();
			if(queryResult.next()){
				return queryResult.getInt(1);
			}else{
				return -1;
			}
		}catch(Exception e){
			System.out.println("get propertyIndex error,the error message is: "+e.getMessage());
			return -1;
		}
		finally{
			JDBCUtil.CloseResource(connection, statement, queryResult);
		}
	}
	
	
	
	
	public static int GetPropertyIndex(String PropertyId,boolean readFromFile) throws SQLException, AnalysisException{
		if(!readFromFile){
			return GetPropertyIndex(PropertyId);
		}else{
			if(BroadcastIndex==null){
				System.out.println("the broadcaseIndex is null");
			}else if(BroadcastIndex.value()==null){
				System.out.println("the broadcaseIndex.value() is null");
			}else if(BroadcastIndex.value().get(PropertyId)==null){
				System.out.println(PropertyId);
				System.out.println("get real index is null");
				return -1;
			}
			return BroadcastIndex.value().get(PropertyId);
		}
	}
	
	public static void WritePropertyIndexToFile(SparkSession session,String outputFileName){
		Dataset<Row> originInfoData = session.read().jdbc(JDBCUtil.DB_URL, JDBCUtil.PropertyInfoTable, JDBCUtil.GetReadProperties(JDBCUtil.PropertyInfoTable));
		JavaRDD<Row> filterColRdd = originInfoData.map(new MapFunction<Row,Row>(){

			@Override
			public Row call(Row line) throws Exception {
				return RowFactory.create(line.getInt(0),line.getString(1));
			}
			
		}, Encoders.bean(Row.class)).javaRDD();
		
		filterColRdd.saveAsTextFile(outputFileName);
		
		/*StructField index=new StructField("PIndex", DataTypes.StringType, true, Metadata.empty());
		StructField pId=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
		StructField[] fieldList={index,pId};
		StructType schema=DataTypes.createStructType(fieldList);*/
	}
	
	public static Dataset<Row> GetPropertyNameMaxLength(String path){
		JavaSparkContext tempJavaContext=new JavaSparkContext(SparkConst.MainSession.sparkContext());
		final MaxAccumulator maxAcc=new MaxAccumulator();
		tempJavaContext.sc().register(maxAcc, "maxAcc");
		//tempJavaContext.sc().register(indexAcc,"indexAcc");
		//MaxPropertyNameLength=tempJavaContext.broadcast(new Integer(0));
		Dataset<String> originData=SparkConst.MainSession.read().textFile(path);
		//List<String> originList = originData.collectAsList();
		//System.out.println("the origin list size is: "+originList.size());
		Dataset<String> specialCharToNormal = originData.map(new MapFunction<String,String>(){

			@Override
			public String call(String value) throws Exception {
				String returnResult=value.replaceAll("\\*\\*\\*\\*",FileConstValue.StrSeparator);
				//System.out.println("the return result is: "+returnResult);
				return returnResult;
			}
			
			
		}, Encoders.STRING());
		//List<String> normalList = specialCharToNormal.collectAsList();
		//System.out.println("the normal list size is: "+normalList.size());
		Dataset<String> filterDataset = specialCharToNormal.filter(new FilterFunction<String>() {
			
			@Override
			public boolean call(String value) throws Exception {
				String[] rowArr = value.split(FileConstValue.StrSeparator);
				if(rowArr.length==3){
					//System.out.println("the length is 3");
					return true;
				}else{
					//System.out.println("filter has filte the item");
					return false;
				}
			}
		});
		//List<String> filterResultList = filterDataset.collectAsList();
		//System.out.println("the filter result list size is: "+filterResultList.size());
		Dataset<Row> splitResultDataset = filterDataset.map(new MapFunction<String,Row>(){

			@Override
			public Row call(String value) throws Exception {
				String[] rowArr=value.split(FileConstValue.StrSeparator);
				//in case of exception,there need a judgment.
				if(rowArr.length==3){
					//String index=String.valueOf(indexBroadcast.value());
					//indexBroadcast.
					System.out.println("the name is: "+rowArr[0]);
					System.out.println("the name length is: "+rowArr[0].length());
					System.out.println("the acc value is: "+maxAcc.value());
					if(rowArr[0].length()>maxAcc.value()){
						maxAcc.add((long)rowArr[0].length());
					}
					return RowFactory.create(rowArr);
				}else{
					return null;
				}
			}
			
		}, Encoders.bean(Row.class)); 
		System.out.println("the split result count is: "+splitResultDataset.count());
		//List<Row> splitReusltList = splitResultDataset.collectAsList();
		//System.out.println("the split list size is:"+splitReusltList.size());
		System.out.println(maxAcc.value());
		JavaRDD<Row> handledInfoRdd=splitResultDataset.javaRDD();
		StructField ID=new StructField("ID",DataTypes.StringType, false, Metadata.empty());
		StructField name=new StructField("name",DataTypes.StringType, false, Metadata.empty());
		StructField description=new StructField("description",DataTypes.StringType, false, Metadata.empty());
		StructField[] fieldList={ID,name,description};
		StructType schema=DataTypes.createStructType(fieldList);
		return SparkConst.MainSession.createDataFrame(handledInfoRdd, schema);
		//return splitResultDataset;
	}
	
	public static void WritePropertyInfoToDatabase(Dataset<Row> originData,String tableName){
		StructField pIndex=new StructField("PIndex", DataTypes.ShortType, false, Metadata.empty());
		StructField ID=new StructField("ID",DataTypes.StringType, false, Metadata.empty());
		StructField name=new StructField("name",DataTypes.StringType, false, Metadata.empty());
		StructField description=new StructField("description",DataTypes.StringType, false, Metadata.empty());
		StructField[] fieldList={pIndex,ID,name,description};
		List<Row> originList = originData.collectAsList();
		List<Row> resultList=new ArrayList<Row>();
		int index=0;
		for(int i=0;i<originList.size();i++){
			resultList.add(RowFactory.create((short)index,(String)originList.get(i).get(0),(String)originList.get(i).get(1),(String)originList.get(i).get(2)));
			index++;
		}
		StructType schema=DataTypes.createStructType(fieldList);
		SparkConst.MainSession.createDataFrame(resultList, schema).write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, 
				tableName, JDBCUtil.GetWriteProperties(tableName));
	}
	
	public static void GeneratePropertyInfo(String tableName,String storeFilePath){
		Dataset<Row> databaseOriginData = SparkConst.MainSession.read().
				jdbc(JDBCUtil.DB_URL, JDBCUtil.PropertyInfoTable, JDBCUtil.GetReadProperties(tableName));
		databaseOriginData.write().mode(SaveMode.Overwrite).csv(storeFilePath);
	}
	
	public static void main(String[] args) {
		
		/**
		 * writeInfo to database
		 */
		//============================================================================================================
		//WritePropertyInfoToDatabase(GetPropertyNameMaxLength(FileConstValue.LocalPropertyInfoFileDir),JDBCUtil.PropertyInfoTable);
		//============================================================================================================
		
		/**
		 * gennerate the propertyInfo file 
		 */
		GeneratePropertyInfo(JDBCUtil.PropertyInfoTable,FileConstValue.localPropertyInfoTableFIleDir);
	}
	
}
