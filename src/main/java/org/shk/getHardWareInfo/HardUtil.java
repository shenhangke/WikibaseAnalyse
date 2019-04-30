package org.shk.getHardWareInfo;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class HardUtil {

	/**
	 * 
	 * Description return the cmd execute result 
	 * @param cmd
	 * @return 
	 * Return type: String
	 */
	public static String ExecuteLinuxCmd(String cmd)  {
		try {
			System.out.println("got cmd job : " + cmd);
			Runtime run = Runtime.getRuntime();
			Process process;
			process = run.exec(cmd);
			InputStream in = process.getInputStream();
			//BufferedReader bs = new BufferedReader(new InputStreamReader(in));
			StringBuffer out = new StringBuffer();
			byte[] b = new byte[8192];
			for (int n; (n = in.read(b)) != -1;) {
				out.append(new String(b, 0, n));
			}
			
			in.close();
			process.destroy();
			return out.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String GetSerialNumber(String cmd ,String record,String symbol) {
		String execResult = ExecuteLinuxCmd(cmd);
		String[] infos = execResult.split("\n");
		
		for(String info : infos) {
			info = info.trim();
			if(info.indexOf(record) != -1) {
				info.replace(" ", "");
				String[] sn = info.split(symbol);
				return sn[1];
			}
		}
		
		return null;
	}
	
	public static String GetCPUID(){
		String cpuid =GetSerialNumber("dmidecode -t processor | grep 'ID'", "ID",":");
		return cpuid;
	}

}
