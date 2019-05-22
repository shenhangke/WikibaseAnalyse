package org.shk.JsonParse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.mortbay.util.ajax.JSON;
import org.shk.JsonParse.Item.LanItem;
import org.shk.JsonParse.Item.Property.PropertyInfo;
import org.shk.JsonParse.Item.Property.PropertyInfo.ReferenceInfo.RefSnakInfo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.gson.JsonObject;

public class ParseItem {
	public static Date ParseDateFromUTC(String UTCDate) throws ParseException{
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		return df.parse(UTCDate);
	}
	
	private static Item.MainSnakType ParseMainSnakType(String type){
		if(type.toLowerCase().trim().equals("value")){
			return Item.MainSnakType.Value;
		}else if(type.toLowerCase().trim().equals("somevalue")){
			return Item.MainSnakType.SomeValue;
		}else if(type.toLowerCase().trim().equals("novalue")){
			return Item.MainSnakType.NoValue;
		}else{
			return Item.MainSnakType.NoValue;
		}
	}
	
	public static Item ParseJsonToItem(String Json) throws ParseException{
		JSONObject aParseObj=JSONObject.parseObject(Json);
		Item aItem=new Item();
		//aItem.originJson=Json;
		aItem.entityId=aParseObj.getString("id");
		String type=aParseObj.getString("type");
		if(type.trim().toLowerCase().equals("item")){
			aItem.type=Item.EntityType.Item;
		}else if(type.trim().toLowerCase().equals("property")){
			aItem.type=Item.EntityType.Property;
		}
		aItem.lastrevid=aParseObj.getString("lastrevid");
		String modified=aParseObj.getString("modified");
		if(modified!=null){
			aItem.modified=ParseDateFromUTC(modified);
		}
		//parse Label
		JSONObject labels=aParseObj.getJSONObject("labels");
		if(labels!=null){
			for(Entry<String,Object> entry:labels.entrySet()){
				String lanName=entry.getKey();
				try{
					JSONObject labelValue=(JSONObject)entry.getValue();
					String value=labelValue.getString("value");
					Item.LanItem aTempLabelItem=new Item.LanItem();
					aTempLabelItem.lanName=lanName;
					aTempLabelItem.value=value;
					aItem.labels.put(lanName, aTempLabelItem);
				}catch(Exception e){
					System.out.println("set label error,the error message is: "+e.getMessage());
				}
			}
		}
		JSONObject descriptions=aParseObj.getJSONObject("descriptions");
		if(descriptions!=null){
			for(Entry<String,Object> entry:descriptions.entrySet()){
				String lanName=entry.getKey();
				try{
					JSONObject description=(JSONObject)entry.getValue();
					String value=description.getString("value");
					Item.LanItem aTempDesItem=new Item.LanItem();
					aTempDesItem.lanName=lanName;
					aTempDesItem.value=value;
					aItem.descriptions.put(lanName, aTempDesItem);
				}catch(Exception e){
					System.out.println("set description error,the error message is: "+e.getMessage());
				}
			}
		}
		JSONObject aliases=aParseObj.getJSONObject("aliases");
		if(aliases!=null){
			for(Entry<String,Object> entry:aliases.entrySet()){
				try{
					JSONArray aliase=(JSONArray)entry.getValue();
					String lanName=entry.getKey();
					Item.LanAliaseItem aTempLanAliase=new Item.LanAliaseItem();
					aTempLanAliase.lanName=lanName;
					for(int i=0;i<aliase.size();i++){
						JSONObject lanItem=aliase.getJSONObject(i);
						Item.LanItem aTempLanItem=new Item.LanItem();
						aTempLanItem.lanName=lanName;
						aTempLanItem.value=lanItem.getString("value");
						aTempLanAliase.itemList.add(aTempLanItem);
					}
					aItem.aliases.put(lanName,aTempLanAliase);
				}catch(Exception e){
					System.out.println("set aliases error,the error message is: "+e.getMessage());
				}
			}
		}
		//parse the site link field
		JSONObject siteLinks=aParseObj.getJSONObject("sitelinks");
		if(siteLinks!=null){
			try{
				for(Entry<String,Object> entry:siteLinks.entrySet()){
					Item.SiteLink aTempSiteLink=new Item.SiteLink();
					aTempSiteLink.site=entry.getKey();
					JSONObject siteLink=(JSONObject)entry.getValue();
					aTempSiteLink.title=siteLink.getString("title");
					aTempSiteLink.url=siteLink.getString("url");
					JSONArray badges=siteLink.getJSONArray("badges");
					if(badges!=null){
						for(int i=0;i<badges.size();i++){
							aTempSiteLink.badges.add(badges.getString(i));
						}
					}
					aItem.siteLinks.put(aTempSiteLink.site, aTempSiteLink);
				}
			}catch(Exception e){
				System.out.println("set sitelinks error,the error message is: "+e.getMessage());
			}
		}
		
		//parse claim
		JSONObject claims=aParseObj.getJSONObject("claims");
		if(claims!=null){
			for(Entry<String,Object> entry:claims.entrySet()){
				Item.Property aTempProperty=new Item.Property();
				aTempProperty.propertyName=entry.getKey();
				JSONArray propertyList=(JSONArray)entry.getValue();
				for(int i=0;i<propertyList.size();i++){
					JSONObject propertyInfo=propertyList.getJSONObject(i);
					Item.Property.PropertyInfo aTempPropertyInfo=new Item.Property.PropertyInfo();
					aTempPropertyInfo.id=propertyInfo.getString("id");
					if(propertyInfo.getString("type").toLowerCase().trim().equals("statement")){
						aTempPropertyInfo.type=Item.PropertyType.Statement;
					}else if(propertyInfo.getString("type").toLowerCase().trim().equals("statement")){
						aTempPropertyInfo.type=Item.PropertyType.Claim;
					}
					if(propertyInfo.getString("rank").toLowerCase().trim().equals("normal")){
						aTempPropertyInfo.rank=Item.RankType.Normal;
					}else if(propertyInfo.getString("rank").toLowerCase().trim().equals("preferred")){
						aTempPropertyInfo.rank=Item.RankType.Preferred;
					}else if(propertyInfo.getString("rank").toLowerCase().trim().equals("deprecated")){
						aTempPropertyInfo.rank=Item.RankType.Deprecated;
					}
					//parse mainsnak
					JSONObject mainSnak=propertyInfo.getJSONObject("mainsnak");
					if(mainSnak!=null){
						Item.Property.PropertyInfo.MainSnak aTempMainSnak=new Item.Property.PropertyInfo.MainSnak();
						if(mainSnak.getString("snaktype").toLowerCase().trim().equals("value")){
							aTempMainSnak.snakType=Item.MainSnakType.Value;
						}else if(mainSnak.getString("snaktype").toLowerCase().trim().equals("somevalue ")){
							aTempMainSnak.snakType=Item.MainSnakType.SomeValue;
						}else if(mainSnak.getString("snaktype").toLowerCase().trim().equals("novalue")){
							aTempMainSnak.snakType=Item.MainSnakType.NoValue;
						}
						aTempMainSnak.property=mainSnak.getString("property");
						aTempMainSnak.dataType=mainSnak.getString("datatype");
						//aTempMainSnak.Type=mainSnak.getString("")
						if(aTempMainSnak.snakType==Item.MainSnakType.Value){
							JSONObject dataValue=mainSnak.getJSONObject("datavalue");
							if(dataValue!=null){
								Item.Property.PropertyInfo.MainSnak.DataValue aTempDataValue=
										new Item.Property.PropertyInfo.MainSnak.DataValue();
								aTempDataValue.type=dataValue.getString("type");
								aTempDataValue.value=dataValue.getString("value");
								aTempMainSnak.dataValue=aTempDataValue;
							}	
						}
						aTempPropertyInfo.mainSnak=aTempMainSnak;
					}
					
					//parse qualifiers
					JSONObject qualifiers=propertyInfo.getJSONObject("qualifiers");
					if(qualifiers!=null){
						for(Entry<String, Object> entryQua:qualifiers.entrySet()){
							Item.Property.PropertyInfo.PropertyQualifier aTempQualifier=
									new Item.Property.PropertyInfo.PropertyQualifier();
							aTempQualifier.property=entryQua.getKey();
							//parse qua list
							JSONArray quaProList=(JSONArray)entryQua.getValue();
							if(quaProList!=null){
								for(int j=0;j<quaProList.size();j++){
									JSONObject quaInfo=quaProList.getJSONObject(j);
									if(quaInfo!=null){
										Item.Property.PropertyInfo.PropertyQualifier.QualifierInfo aTempQuaInfo=
												new Item.Property.PropertyInfo.PropertyQualifier.QualifierInfo();
										aTempQuaInfo.hash=quaInfo.getString("hash");
										aTempQuaInfo.snakType=ParseMainSnakType(quaInfo.getString("snaktype"));
										aTempQuaInfo.property=quaInfo.getString("property");
										aTempQuaInfo.dataType=quaInfo.getString("datatype");
										JSONObject dataValue=quaInfo.getJSONObject("datavalue");
										if(dataValue!=null){
											Item.Property.PropertyInfo.MainSnak.DataValue aTempdataValue=
													new Item.Property.PropertyInfo.MainSnak.DataValue();
											aTempdataValue.type=dataValue.getString("type");
											aTempdataValue.value=dataValue.getString("value");
											aTempQuaInfo.dataValue=aTempdataValue;
										}
										aTempQualifier.qualifierInfos.add(aTempQuaInfo);
									}
								}
							}
							aTempPropertyInfo.qualifiers.put(aTempQualifier.property, aTempQualifier);
						}
					}
					
					//parse referces
					JSONArray references=propertyInfo.getJSONArray("references");
					if(references!=null){
						for(int j=0;j<references.size();j++){
							JSONObject refInfo=references.getJSONObject(j);
							if(refInfo!=null){
								Item.Property.PropertyInfo.ReferenceInfo aTempRefInfo=
										new Item.Property.PropertyInfo.ReferenceInfo();
								aTempRefInfo.hash=refInfo.getString("hash");
								//parse the snak
								JSONObject snaks=refInfo.getJSONObject("snaks");
								if(snaks!=null){
									for(Entry<String,Object> refEntry:snaks.entrySet()){
										Item.Property.PropertyInfo.ReferenceInfo.RefSnakInfo aTempRefSnak=
												new Item.Property.PropertyInfo.ReferenceInfo.RefSnakInfo();
										aTempRefSnak.propertyName=refEntry.getKey();
										JSONArray snaksArr=(JSONArray)refEntry.getValue();
										if(snaksArr!=null){
											for(int k=0;k<snaksArr.size();k++){
												Item.Property.PropertyInfo.ReferenceInfo.RefSnakInfo.SnakProperty aTempSnakPro=
														new Item.Property.PropertyInfo.ReferenceInfo.RefSnakInfo.SnakProperty();
												JSONObject snakProperty=snaksArr.getJSONObject(k);
												aTempSnakPro.snakType=ParseMainSnakType(snakProperty.getString("snaktype"));
												aTempSnakPro.property=snakProperty.getString("property");
												aTempSnakPro.dataType=snakProperty.getString("datatype");
												
												JSONObject snakDataValue=snakProperty.getJSONObject("datavalue");
												if(snakDataValue!=null){
													aTempSnakPro.dataValue=new Item.Property.PropertyInfo.MainSnak.DataValue();
													aTempSnakPro.dataValue.type=snakDataValue.getString("type");
													aTempSnakPro.dataValue.value=snakDataValue.getString("value");
												}
												
												aTempRefSnak.SnakPropertyList.add(aTempSnakPro);
											}
											aTempRefInfo.snaks.put(aTempRefSnak.propertyName, aTempRefSnak);
										}
									}
								}
								aTempPropertyInfo.references.add(aTempRefInfo);
							}
						}
					}
					//add info to list
					aTempProperty.propertyInfos.add(aTempPropertyInfo);
				}
				aItem.claims.put(aTempProperty.propertyName, aTempProperty);
			}
		}
		return aItem;
	}
}
