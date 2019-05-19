package org.shk.constValue;

import java.util.Map;

public class FileConstValue {
	public static boolean Test=false; 
	
	public static final String SourceFileDir="D:\\MyEclpse WorkSpace\\DataProject_Data\\";
	public static final String SourceFilePath=SourceFileDir+"latest-all.json";
	
	public static final String TESTFILEPATH="E:\\KgData\\Test_Parse.json";
	public static final String DivideFilePath=SourceFileDir+"div_1.json";
	public static final String TestSingleLine=SourceFileDir+"singleLine.json";
	public static final String PropertyDataFileDir="D:\\MyEclpse WorkSpace\\DataAny\\Data\\PropertyData";
	public static final String PropertyDataFileReadPath="D:\\MyEclpse WorkSpace\\DataAny\\Data\\realData\\PropertyIndexInfo.txt";
	
	public static final String tailFileTest=SourceFileDir+"tailFile.txt";
	
	public static final String PrefixSaveToFile="SaveToFile:";
	
	public static String ServerSourceDir="";
	public static String ServerOriginFileName="";
	public static String ServerTestFileName="";
	public static String ServerInfoFile="";
	public static String ServerFileUrlSourceDir="";
	
	public static String ServerPropertyDataFileReadPath="";
	
	public static String ServerItemAlias_WritePath="";
	public static String ServerItemContainer_WritePath="";
	public static String ServerItemTypeInfo_WritePath="";
	
	public static String ServerPropertyInfoWritePath="";
	
	public static String HandledItemInfoFileDir="E:\\KgData\\ItemInfo\\runtimeData\\runtimeData";
	
	public static String StrSeparator="&&&&";
	
	public static String LocalPropertyInfoFileDir="D:\\MyEclpse WorkSpace\\DataProject_Data\\PropertyInfoFile\\PropertyInfoFile\\";
	public static String localPropertyInfoTableFIleDir="D:\\MyEclpse WorkSpace\\DataProject_Data\\LocalPropertyInfoTableCsv\\Info";
	
	public static String ServerPropertyInfoFileDir="";
	
	
	/**
	 * The path which is store item info relate to item 
	 */
	public static String ServerItemInfoPath="";
	
	
	static{
		Map<String,String> env=System.getenv();
		ServerSourceDir=env.get("SHARED_DIR");
		System.out.println("the shared_dir is: "+ServerSourceDir);
		
		if(!Test){
			if(ServerSourceDir==null){
				System.out.println("the ServerSourceDir is null");
				if(!SparkConst.RunOnLocal){
					throw new NullPointerException("ServerSourceDir is null");
				}
			}else{
				ServerFileUrlSourceDir="file://"+ServerSourceDir;
				ServerOriginFileName=ServerFileUrlSourceDir+"/Datasets/wikidata/wikidata-20150921-all/wikidata-20150921-all.json";
				ServerTestFileName=ServerFileUrlSourceDir+"/Datasets/wikidata/wikidata-20181203-all/head_10.txt";
				ServerInfoFile=ServerFileUrlSourceDir+"/Builds/shenhangke/runtimeData";
				ServerPropertyDataFileReadPath=ServerFileUrlSourceDir+"/Builds/shenhangke/Data/PropertyIndexInfo.txt";
				ServerItemAlias_WritePath=ServerFileUrlSourceDir+"/Builds/shenhangke/DataReposity/ItemAliasInfo";
				ServerItemContainer_WritePath=ServerFileUrlSourceDir+"/Builds/shenhangke/DataReposity/ItemContainerInfo";
				ServerItemTypeInfo_WritePath=ServerFileUrlSourceDir+"/Builds/shenhangke/DataReposity/ItemTypeAnalyseInfo";
				ServerPropertyInfoWritePath=ServerFileUrlSourceDir+"/Builds/shenhangke/DataReposity/PropertyInfoFile";
				ServerItemInfoPath=ServerFileUrlSourceDir+"/Builds/shenhangke/DataReposity/ItemInfoFile";
				ServerPropertyInfoFileDir=ServerFileUrlSourceDir+"/Builds/shenhangke/DataReposity/TempPropertyInfo/Info";
			}
		}else{
			ServerOriginFileName=DivideFilePath;
			ServerPropertyDataFileReadPath="D:\\MyEclpse WorkSpace\\DataAny\\Data\\realData\\PropertyIndexInfo.txt";
			ServerItemAlias_WritePath="E:\\KgData\\AnaData\\Alias";
			ServerItemContainer_WritePath="D:\\MyEclpse WorkSpace\\DataProject_Data\\TestData\\ItemContainer";
			ServerItemTypeInfo_WritePath="E:\\KgData\\AnaData\\TypeInfo";
			ServerItemInfoPath=SourceFileDir+"ItemInfoFile";
			ServerPropertyInfoFileDir="D:\\MyEclpse WorkSpace\\DataAny\\Data\\PropertyInfo\\Info";
		}
	}
}
