package org.shk.JsonParse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author MoonKnight
 * 1.只要item的label和description不为空的话，那么在特定的语言中，这个label和description的组合一定是唯一的，因此，可以用这两个值的组合联合起来作为一个entity的唯一标识
 * 2.一个item中有一个或者多个陈述（statement），每一个陈述都由下面几种成员构成：
 * 	（1）一个属性，比如population，这个属性由P进行标注
 * 	（2）一个值，比如具体是多少
 * 	（3）可选的一个或者多个限定词（qualifiers），限定这个描述在一个一定的范围内
 * 	（4）可选的一个或者多个参考，可以看成是这个描述来源于哪里
 * 3.同一个属性可能会有多个不同的陈述，比如，人们可以有很多的孩子等
 * 4.属性也可能有其他的属性，datatype定义了用在这个属性上的值的类型，目前为止，datatype类型是预定义的
 * 5.claim和statement是一个东西，都由上面的东西组成
 * 5.每一个的实例（或者说一个条目）都有一个
 */
public class Item implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public enum EntityType{
		Item,Property;
	}
	
	public enum PropertyType{
		Statement("statement"),Claim("claim");
		
		private String des="";
		private PropertyType(String des) {
			this.des=des;
		}
		
		public String getDes(){
			return this.des;
		}
	}
	
	public enum RankType{
		Preferred,Normal,Deprecated;
	}
	
	public enum MainSnakType{
		Value((byte)0),NoValue((byte)1),SomeValue((byte)2);
		
		private byte realValue=-1;
		private MainSnakType(byte realValue) {
			this.realValue=realValue;
		}
		
		public byte getRealValue(){
			return this.realValue;
		}
	}
	
	//public String originJson="";
	public String entityId="";  
	public EntityType type=EntityType.Item;
	public String lastrevid="";
	public Date modified=null;
	public static class LanItem{
		public String lanName="";
		public String value="";
	}
	public Map<String,LanItem> labels=new HashMap<String,LanItem>();
	public Map<String,LanItem> descriptions=new HashMap<String,LanItem>();
	
	public static class LanAliaseItem{
		public String lanName="";
		public ArrayList<LanItem> itemList=new ArrayList<Item.LanItem>();
	}
	public Map<String,LanAliaseItem> aliases=new HashMap<String,LanAliaseItem>();
	
	public static class SiteLink{
		public String site="";
		public String title="";
		public ArrayList<String> badges=new ArrayList<String>();
		public String url="";
	}
	public Map<String,SiteLink> siteLinks=new HashMap<String,SiteLink>();
	static public class Property{
		public String propertyName="";
		static public class PropertyInfo{
			public String id="";
			/**
			 * this member is to represent the property is claim ot statement
			 * but,what is the different between them 
			 */
			public PropertyType type=PropertyType.Claim; 
			public RankType rank=RankType.Normal;
			//define the mainsnak
			//the mainsnak is to describe the value of property
			/**
			 * 
			 * @author MoonKnight
			 *this field provide some kind of information about the property
			 *there are three type of mainSnak,they are value,somevalue,novalue
			 *if the type is value,it represent that there is a special value for property
			 *if the type is novalue and somevalue,it represent that there are not a values associate with property
			 */
			static public class MainSnak{
				public MainSnakType snakType=MainSnakType.NoValue;
				public String property="";
				/**
				 * this filed represent how the value can be interpreted 
				 */
				public String dataType="";//because the kinds of type is too much,there use the string to represent type
				static public class DataValue{
					public String type="";
					/**
					 * because the value depend on the dataType field,so this value couldn't be decided now
					 */
					public String value="";
				}
				public DataValue dataValue=null;
				//public String Type="";
			}
			public MainSnak mainSnak=null;
			
			static class PropertyQualifier{
				public String property="";
				static class QualifierInfo extends MainSnak{
					public String hash="";
				}
				public ArrayList<QualifierInfo> qualifierInfos=new ArrayList<Item.Property.PropertyInfo.PropertyQualifier.QualifierInfo>();
			}
			public Map<String,PropertyQualifier> qualifiers=new HashMap<String, Item.Property.PropertyInfo.PropertyQualifier>();
			
			static class ReferenceInfo{
				String hash=null;
				static class RefSnakInfo{
					String propertyName="";
					static class SnakProperty extends MainSnak{
						
					}
					ArrayList<SnakProperty> SnakPropertyList=new ArrayList<SnakProperty>();
				}
				Map<String,RefSnakInfo> snaks=new HashMap<String, RefSnakInfo>();
				/**
				 * index for snaks
				 */
				ArrayList<String> snakOrder=new ArrayList<String>();
			}
			ArrayList<ReferenceInfo> references=new ArrayList<Item.Property.PropertyInfo.ReferenceInfo>();
		}
		
		public ArrayList<PropertyInfo> propertyInfos=new ArrayList<PropertyInfo>(); 
	}
	public Map<String,Property> claims=new HashMap<String,Property>();
}
