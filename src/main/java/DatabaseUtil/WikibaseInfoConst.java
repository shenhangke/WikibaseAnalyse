package DatabaseUtil;

import java.io.Serializable;

public class WikibaseInfoConst implements Serializable{
	
	private static final long serialVersionUID = 1L;

	public static final int wiki_2015_50g_maxItemCount=18555611;
	
	public static final int tableCount=10;
	
	public static final int maxItemId=20991702;
	
	public static final int minItemId=1;
	
	public static final double ratio=((double)wiki_2015_50g_maxItemCount)/((double)(maxItemId-minItemId));

}
