package org.shk.JsonParse;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSONObject;

public class ParsePropertyDataValue implements Serializable{

	private static final long serialVersionUID = 1L;
	
	public static enum Type{
		STRING((byte)1),ENTITYID((byte)2),MONLINGUAL((byte)3);
		
		private byte index=0;
		
		private Type(byte index) {
			this.index=index;
		}
		
		public byte toInt(){
			return this.index;
		}
	}
	
	public static class DataValue{
		public String para1="";
		public String para2="";
		public DataValue(String para1,String para2) {
			this.para1=para1;
			this.para2=para2;
		}
	}
	
	public static DataValue ParseDataValue(Type dataType,String valueJson){
		if(dataType==null){
			return null;
		}else{
			switch(dataType){
				case STRING:{
					return new DataValue(valueJson,"");
				}
				case ENTITYID:{
					JSONObject tempJson=JSONObject.parseObject(valueJson);
					return new DataValue(tempJson.getString("entity-type"), tempJson.getString("numeric-id"));
				}
				case MONLINGUAL:{
					JSONObject tempJson=JSONObject.parseObject(valueJson);
					return new DataValue(tempJson.getString("language"),tempJson.getString("text"));
				}
				default:return null;
			}
		}
		
	}
	
	
	public static Type GetDataValueType(String dataType){
		if(dataType.trim().equals("string")){
			return Type.STRING;
		}else if(dataType.trim().equals("wikibase-entityid")){
			return Type.ENTITYID;
		}else if(dataType.trim().equals("monolingualtext")){
			return Type.MONLINGUAL;
		}else{
			return null;
		}
	}
}
