package org.shk.constValue;

import java.util.Map;

public class FileConstValue {
	public static final String SourceFileDir="E:\\KgData\\";
	public static final String SourceFilePath=SourceFileDir+"latest-all.json";
	
	public static final String TESTFILEPATH="E:\\KgData\\Test_Parse.json";
	public static final String DivideFilePath=SourceFileDir+"div_1.json";
	public static final String TestSingleLine=SourceFileDir+"singleLine.json";
	
	public static final String tailFileTest=SourceFileDir+"tailFile.txt";
	
	public static final String PrefixSaveToFile="SaveToFile:";
	
	public static String ServerSourceDir="";
	public static String ServerFileName="";
	public static String ServerTestFileName="";
	public static String ServerInfoFile="";
	
	
	static{
		Map<String,String> env=System.getenv();
		ServerSourceDir=env.get("SHARED_DIR");
		if(ServerSourceDir==null){
			System.out.println("the ServerSourceDir is null");
			throw new NullPointerException("ServerSourceDir is null");
		}else{
			ServerFileName=ServerSourceDir+"/Datasets/wikidata/wikidata-20181203-all/wikidata-20181203-all.json";
			ServerTestFileName=ServerSourceDir+"/Datasets/wikidata/wikidata-20181203-all/head_10.txt";
			ServerInfoFile=ServerSourceDir+"/shenhangke/info.txt";
		}
		
	}
}
