package org.shk.fileUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.shk.JsonParse.ParseItem;
import org.shk.constValue.FileConstValue;

/**
 * 
 * @author MoonKnight because the file scale may be massive,so,there need a process to divide it into smaller files. 
 * the parse function need achieve by myself,spark json couldn't do the same thing
 *
 */
public class FileSplitUtil implements Serializable{
	private static final long serialVersionUID = 1L;
	private SparkSession session=null;
	private String fileName="";
	private int fileDivCount=0;
	
	public static String ReadStringFromFile(String fileName) throws IOException{
		File aFile=new File(fileName);
		LineIterator aIter=FileUtils.lineIterator(aFile);
		return aIter.nextLine();
	}
	
	public FileSplitUtil(String fileName,int fileDivCount) {
		Builder aBuilder=new SparkSession.Builder();
		aBuilder.master("local[2]").appName("preTreatFile").
		config("spark.driver.memory","4g");
		this.session=aBuilder.getOrCreate();
		this.fileName=fileName;
		this.fileDivCount=fileDivCount;
		System.out.println("the file name is: "+fileName);
	}
	
	public void preHandleFile() throws FileNotFoundException{
		File aFile=new File(this.fileName);
		if(aFile.exists()){
			//this.session.sparkContext().accumulator(1, "123",)
			Dataset<String> originData=this.session.read().textFile(this.fileName);
			//test whether data will over
			//originData.printSchema();
			originData.show();
			originData.foreach(new ForeachFunction<String>() {
				
				public void call(String line) throws Exception {
					// TODO Auto-generated method stub
					//ParseItem.TestParseJson(line);
				}
			});
			//originData.write().json(FileConstValue.TestOutFile_1);
		}else{
			throw new FileNotFoundException("pretreat fail,because the file is not exists");
		}
	}
	
	public void preHandleFile(int lineCount,String saveFileName) throws IOException{
		File sourceFile=new File(this.fileName);
		if(sourceFile.exists()){
			LineIterator it=FileUtils.lineIterator(sourceFile);
			int index=0;
			File outFile=new File(saveFileName);
			if(!outFile.exists()){
				outFile.createNewFile();
			}
			while(it.hasNext()){
				String aLine=it.nextLine();
				if(index==0){
					index++;
					continue;
				}
				//aLine=aLine.substring(0, aLine.length()-1).trim()+System.getProperty("line.separator");
				FileUtils.writeStringToFile(outFile,aLine,"UTF-8",true);
				index++;
				System.out.println("handle line "+index);
				if(index>=lineCount){
					System.out.println("Handle finish");
					break;
				}
			}
		}
	}
	
	public static void writeFileFormTail(String sourceFile,String targetFile,long count){
		RandomAccessFile aFile=null;
		try{
			aFile=new RandomAccessFile(sourceFile, "r");
			long fileLength=aFile.length();
			long start=aFile.getFilePointer();
			long readIndex=start+fileLength-1;
			String aLine="";
			aFile.seek(readIndex);
			long currentCount=0;
			int c=-1;
			File aOutFile=new File(targetFile);
			if(!aOutFile.exists()){
				aOutFile.createNewFile();
			}
			while((readIndex>start)&&(currentCount<count)){
				if(aFile.readByte()=='\n'){
					aLine=aFile.readLine();
					aLine=(aLine==null)?"":new String(aLine.getBytes("UTF-8"),"UTF-8");
					if((!aLine.equals("]"))&&(!aLine.equals(""))){
						aLine=aLine.substring(0, aLine.length()-1).trim()+System.getProperty("line.separator");
						System.out.println("current count is: "+currentCount);
						System.out.println("line is: "+aLine);
						FileUtils.writeStringToFile(aOutFile, aLine,"UTF-8",true);
						currentCount++;
					}
				}
				readIndex--;
				aFile.seek(readIndex);
			}
		}catch(Exception e){
			System.out.println("read or write file failed,the exception is: "+e.getMessage());
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		FileSplitUtil fileUtil=new FileSplitUtil("F:\\KgData\\latest-all.json",0);
		fileUtil.preHandleFile(100000, "F:\\KgData\\div_To_zhao.json");
		
	}
}
