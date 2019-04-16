package DatabaseUtil;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.shk.JsonParse.Item;
import org.shk.JsonParse.Item.Property;
import org.shk.JsonParse.Item.Property.PropertyInfo;
import org.shk.JsonParse.ParseItem;

import com.alibaba.fastjson.JSONObject;

public class PropertyDatabaseUtil {
	private static final String GetLineFromDatabaseStatement="select Item from "+JDBCUtil.PropertyItem+" where PID=?;";
	
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
	
	public static int GetPropertyIndex(String PropertyID) throws SQLException{
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
	
}
