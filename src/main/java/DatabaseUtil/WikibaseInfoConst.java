package DatabaseUtil;

import java.io.Serializable;

import org.shk.constValue.FileConstValue;

public class WikibaseInfoConst implements Serializable{
	
	private static final long serialVersionUID = 1L;

	public static int wiki_2015_50g_maxItemCount=18555611;
	
	public static final int tableCount=10;
	
	public static int maxItemId=20991702;
	
	public static int minItemId=1;
	
	public static final double ratio=((double)wiki_2015_50g_maxItemCount)/((double)(maxItemId-minItemId));
	
	static{
		if(FileConstValue.Test==true){
			wiki_2015_50g_maxItemCount=9999;
			maxItemId=83644;
			minItemId=8;
		}
	}

}
