package DatabaseUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCUtil {
	
	/**
	 * Database login ingo
	 */
	static protected final String USER="root";
	static protected final String PASSWORD="5166266skf";
	
	/**
	 * Database info
	 */
	static public final String DataBaseName="wikibase";
	static public final String PropertyInfoTable="propertyInfo";
	static public final String PropertyItem="PropertyItem";
	static public final String PropertryContainTable="PropertyContainer";
	static public final String PropertyAlias="PropertyAlias";
	static public final String PropertyMainSnak="PMainSnak";
	static public final String ItemInfo="ItemInfo";
	static public final String ItemAlias="AliasMapToID";
	static public final String ItemContainer="ItemContainer";
	static public final String ItemTypeAnaTable="ItemType";
	static public final String DataTypeNameTable="dataTypeMap";
	static public final String TypeInfo="typeMap";
	static public final String PrefixMainSnak="mainsnak_";
	
	/**
	 * Database drive info
	 */
	public static String MysqlDriversClassName="com.mysql.jdbc.Driver"; 
	static public final String DB_URL="jdbc:mysql://localhost:3306/"+DataBaseName+"?useUnicode=true&characterEncoding=UTF8";
	
	static private boolean IsIni=false;
	
	public static String NULLVALUE="Empty";
	
	public static Properties GetWriteProperties(String OperationTableName){
		Properties config=new Properties();
		config.put("driver", MysqlDriversClassName);
		config.put("user", USER);
		config.put("password", PASSWORD);
		config.put("dbtable", OperationTableName);
		config.put("truncate", "true");
		return config;
	}
	
	public static Properties GetWriteProperties(){
		return GetWriteProperties(PropertyInfoTable);
	}
	
	public static Properties GetReadProperties(String operationTableName){
		Properties config=new Properties();
		config.put("driver", MysqlDriversClassName);
		config.put("user", USER);
		config.put("password", PASSWORD);
		config.put("dbtable", operationTableName);
		return config;
	}
	
	public static Connection GetConnection(){
		try{
			IniDatabase();
			return DriverManager.getConnection(DB_URL, USER, PASSWORD);
		}catch(Exception e){
			System.out.println("get connection error,the error message is: "+e.getMessage());
			return null;
		}
	}
	
	private static void IniDatabase() throws ClassNotFoundException{
		if(!IsIni){
			Class.forName(MysqlDriversClassName);
			IsIni=true;
		}
	}
	
	public static void CloseResource(Connection connection,PreparedStatement statement,ResultSet resultSet) throws SQLException{
		if(connection!=null){
			connection.close();
		}
		if(statement!=null){
			statement.close();
		}
		if(resultSet!=null){
			resultSet.close();
		}
	}
	
	public static void CloseResource(Connection connection,Statement statement,ResultSet resultSet) throws SQLException{
		if(connection!=null){
			connection.close();
		}
		if(statement!=null){
			statement.close();
		}
		if(resultSet!=null){
			resultSet.close();
		}
	}
}
