	public Dataset<Row> itemContainerAnalyse(Dataset<Item> originData,String tableName){
		JavaRDD<Row> containRDD = originData.map(new MapFunction<Item,Row>() {

			public Row call(Item originItem) throws Exception {
				long[] aIniArr=new long[AnalysePropertyData.ContainerColCount];
				for(int i=0;i<AnalysePropertyData.ContainerColCount;i++){
					aIniArr[i]=0x0000000000000000L;
				}
				for(Entry<String,Item.Property> entry:originItem.claims.entrySet()){
					//System.out.println("the property key is: "+entry.getKey());
					int propertyIndex=PropertyDatabaseUtil.GetPropertyIndex(entry.getKey(),true);
					//System.out.println("the propertyIndex is: "+propertyIndex);
					//System.out.println("the property ID is: "+propertyIndex);
					if(propertyIndex==-1){
						//System.out.println("get property index error");
						continue;
					}else{
						int segment=(propertyIndex%64)==0?(propertyIndex/64-1):(propertyIndex/64);
						//System.out.println("the segment is: "+segment);
						int index=(propertyIndex%64)==0?64:(propertyIndex%64);
						//System.out.println("the index is: "+index);
						aIniArr[segment]|=AnalysePropertyData.CodeArr[index-1];
						//System.out.println("the aIniArr is: "+Long.toBinaryString(aIniArr[segment]));
					}
				}
				
				//System.out.println(Long.toHexString(aIniArr[35]));
				Long[] containerLongArr=new Long[AnalysePropertyData.ContainerColCount];
				for(int i=0;i<AnalysePropertyData.ContainerColCount;i++){
					containerLongArr[i]=new Long(aIniArr[i]);
				}
				int iniArrLength=aIniArr.length;  //94
				Object[] aReusltRowArr=new Object[1+AnalysePropertyData.ContainerColCount];
				aReusltRowArr[0]=originItem.entityId;
				System.arraycopy(containerLongArr, 0, aReusltRowArr, 1, iniArrLength);
				return RowFactory.create(aReusltRowArr);
			}
		}, Encoders.bean(Row.class)).javaRDD();
		
		ArrayList<StructField> aFieldList=new ArrayList<StructField>();
		StructField PID=new StructField("PID", DataTypes.StringType, true, Metadata.empty());
		aFieldList.add(PID);
		for(int i=0;i<AnalysePropertyData.ContainerColCount;i++){
			StructField aTempCol=new StructField("Col_"+i, DataTypes.LongType, true, Metadata.empty());
			aFieldList.add(aTempCol);
		}
		StructType schema = DataTypes.createStructType(aFieldList);
		Dataset<Row> containerResult = this.session.createDataFrame(containRDD, schema);
		containerResult.show();
		if(this.isWriteToFile(tableName)){
			System.out.println("handle file");
			File aFile=new File(this.getStoreFilePath(tableName));
			if(aFile.exists()){
				aFile.delete();
			}
			containRDD.saveAsTextFile(this.getStoreFilePath(tableName));
		}else{
			containerResult.write().mode(SaveMode.Overwrite).jdbc(JDBCUtil.DB_URL, tableName, 
					JDBCUtil.GetWriteProperties(tableName));
		}
		return containerResult;
	}