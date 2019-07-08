unit DataBaseUtil;

interface
uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes,Data.DB, Datasnap.DBClient, SimpleDS,
  Data.SqlExpr, Data.DBXMySQL, Data.DBXOracle, Data.Win.ADODB, Vcl.StdCtrls,WikibaseConst,Vcl.Dialogs,ErrorCode;

  const DBConnectionStr='Driver={MySQL ODBC 8.0 Unicode Driver};Server=127.0.0.1;Database=wikibase;User=root; Password=5166266skf;Option=3;';
  const SplitChar='&&&&';
  const ContainerCount=27;

type
  TDataBaseUtil=class
  public
    constructor Create(); overload;
    //constructor Create(Connection:TADOConnection); overload;
    destructor Destroy();override;
    function QueryEntityInfoByName(EntityName:String):TStringList;
    function QueryClassInfoByID(ID:Integer):TStringList;
    function CalculateIndex(ID:Integer):Integer;
    function CalculateTableIndex(Index:Integer):Integer;

    //if there are no items queried,function return nil
    function QueryRelatedEntityInfo(ID:Integer):TStringList;
    function QueryAllPropertyInfo(ID:Integer):TStringList;

    //the answer may have many possibilities,so this function could return a list
    //to corrrespond the every possibility.
    function GetQAAnswer(ObjectName:string;RelationName:string):TStringList;
  private
    Connection:TADOConnection;
    function QueryEntityInfo(sql:string):TStringList;
  end;

implementation
uses
  MainWin;
{ TDataBaseUtil }

function TDataBaseUtil.CalculateIndex(ID: Integer): Integer;
var
  ratio:Double;
  index:Integer;
  function getRatio():Double;
  var
    ItemCount,maxItemId,minItemId:Integer;
    ratio:Double;
  begin
    ItemCount:=WikibaseConst.wiki_2015_50g_maxItemCount;
    maxItemId:=WikibaseConst.maxItemId;
    minItemId:=WikibaseConst.minItenId;
    ratio:=ItemCount/(maxItemId-minItemId);
    result:=ratio;
  end;
begin
  ratio:=getRatio;
  index:=Trunc(ratio*(ID-WikibaseConst.minItenId));
  result:=index;
end;

function TDataBaseUtil.CalculateTableIndex(Index: Integer): Integer;
var
  meanCount:Integer;
  startIndex,endIndex,I:Integer;
begin
  meanCount:=Trunc(WikibaseConst.wiki_2015_50g_maxItemCount/WikibaseConst.tableCount);
  for I := 1 to WikibaseConst.tableCount do
  begin
    if i=WikibaseConst.tableCount then
    begin
      endIndex:=WikibaseConst.wiki_2015_50g_maxItemCount;
    end
    else
    begin
      endIndex:=i*meanCount;
    end;
    startIndex:=((i-1)*meanCount)+1;
    if (index>=startIndex) and (index<=endIndex) then
    begin
      Result:=i-1;
      Exit;
    end;
  end;
end;

constructor TDataBaseUtil.Create;
begin
  self.Connection:=TADOConnection.Create(nil);
  self.Connection.ConnectionString:=DBConnectionStr;
  self.Connection.LoginPrompt:=false;
  self.Connection.Connected:=True;
  //self.Connection.Provider:='MSDASQL';
end;

destructor TDataBaseUtil.Destroy;
begin
  inherited;
  self.Connection.Close;
end;

function TDataBaseUtil.GetQAAnswer(ObjectName, RelationName: string): TStringList;
var
  AdoQuery,AdoQuery_2,AdoQuery_3,AdoQuery_4:TADOQuery;
  tableInt:Integer;
  valueArr:TArray<string>;
  StringList:TStringList;
  sql:string;
begin
  result:=nil;
  AdoQuery:=TADOQuery.Create(nil);
  try
    AdoQuery.Connection:=self.Connection;
    //first to query whether object exists ot not
    AdoQuery.SQL.Add('select * from itemInfo where name="'+ObjectName+'"');
    AdoQuery.Open;
    if (AdoQuery.RecordCount>0) then
    begin
      AdoQuery_2:=TADOQuery.Create(nil);
      try
        //to confirm the property exists
        AdoQuery_2.Connection:=self.Connection;
        AdoQuery_2.SQL.Add('select * from propertyInfo where name="'+RelationName+'"');
        AdoQuery_2.Open;
        if AdoQuery_2.RecordCount>0 then
        begin
          //query the relation wheter match the entity
          StringList:=TStringList.Create;
          while not AdoQuery.Eof do
          begin
            while not AdoQuery_2.Eof do
            begin
              AdoQuery_3:=TADOQuery.Create(nil);
              try
                AdoQuery_3.Connection:=self.Connection;
                tableInt:=Self.CalculateTableIndex(self.CalculateIndex(StrToInt(AdoQuery.FieldByName('ID').AsString)));
                sql:='select * from MainSnak_'+inttoStr(tableInt)+
                //intTostr(self.CalculateTableIndex(self.CalculateIndex(StrToInt(AdoQuery.FieldByName('ID').AsString)))))+
                ' where QID='+AdoQuery.FieldByName('ID').AsString+' and propertyId='+
                AdoQuery_2.FieldByName('ID').AsString.Substring(1);
                AdoQuery_3.SQL.Add(sql);
                AdoQuery_3.Open;
                if AdoQuery_3.RecordCount>0 then
                begin
                 while not AdoQuery_3.Eof do
                 begin
                  valueArr:=AdoQuery_3.FieldByName('valueArr').AsString.Split(['&&&&']);
                  case StrToInt(AdoQuery_3.FieldByName('dataType').AsString) of
                    1:
                    begin
                      StringList.Add('latitude:'+valueArr[0]+' longtitude:'+valueArr[1]);
                    end;
                    2:
                    begin
                      StringList.Add('amount:'+valueArr[0]+' upperBound:'+valueArr[1]+' lowerBound:'+valueArr[2]);
                    end;
                    4:
                    begin
                      StringList.Add(valueArr[0]);
                    end;
                    5:
                    begin
                      AdoQuery_4:=TADOQuery.Create(nil);
                      try
                        AdoQuery_4.Connection:=self.Connection;
                        AdoQuery_4.SQL.Add('select Name from itemInfo where ID='+valueArr[1]);
                        AdoQuery_4.Open;
                        if AdoQuery_4.RecordCount>0 then
                        begin
                          StringList.Add(AdoQuery_4.FieldByName('Name').AsString);
                        end;
                      finally
                        AdoQuery_4.Free;
                      end;
                      //StringList.Add('entityId: '+valueArr[1]);
                    end;
                    7:
                    begin
                      StringList.Add('time: '+valueArr[0]);
                    end;
                    else
                    begin
                      StringList.Add('Unknow type: '+AdoQuery_3.FieldByName('valueArr').AsString);
                    end;
                 end;
                  AdoQuery_3.Next;
                 end;
                end;
              finally
                AdoQuery_3.Free
              end;
              AdoQuery_2.Next;
            end;
            AdoQuery.Next;
          end;
          Result:=StringList;
        end
        else
        begin
          errorCodeIndex:=No_such_Relation;
        end;
      finally
        AdoQuery_2.Free;
      end;
    end
    else
    begin
      errorCodeIndex:=No_Such_Object;
    end;

  finally
    AdoQuery.Free;
  end;

end;

function TDataBaseUtil.QueryAllPropertyInfo(ID: Integer): TStringList;
var
  AdoQuery,AdoQuery_2 : TADOQuery;
  StringList:TStringList;
  I,j:Integer;
  bitIniArr:array[0..63] of Int64;
  FirstFlag:Int64;
  PropertyList:TStringlist;
  propertyIndex,staticIndex:Integer;
  tempInt64:Int64;
begin
  Result:=nil;
  AdoQuery:=TADOQuery.Create(nil);
  try
    AdoQuery.Connection:=self.Connection;
    AdoQuery.SQL.Add('select * from itemContainer where ID='+intToStr(ID));
    AdoQuery.Open;
    if AdoQuery.RecordCount=1 then
    begin
      //init the bitArr
      FirstFlag:=1;
      for I := 0 to 63 do
      begin
        bitIniArr[i]:=FirstFlag shl i;
      end;
      PropertyList:=TStringlist.Create;
      while not AdoQuery.eof do
      begin
        for I := 0 to ContainerCount-1 do
        begin
          if StrToInt64(AdoQuery.FieldByName('Col_'+intTostr(i)).AsString)=0 then
          begin
            Continue;
          end
          else
          begin
            tempInt64:=StrToInt64(AdoQuery.FieldByName('Col_'+intTostr(i)).AsString);
            staticIndex:=i*64;
            for j := 0 to 63 do
            begin
              if (tempInt64 and bitIniArr[j])<>0 then
              begin
                PropertyList.Add(intToStr(staticIndex+j));
              end
              else
                Continue;
            end;
          end;
        end;
        if PropertyList.Count>0 then
        begin
          StringList:=TStringlist.Create;
          for I := 0 to PropertyList.Count-1 do
          begin
            AdoQuery_2:=TADOQuery.Create(nil);
            try
              AdoQuery_2.Connection:=self.Connection;
              AdoQuery_2.SQL.Add('select * from propertyInfo where PIndex='+PropertyList[i]+'');
              AdoQuery_2.Open;
              if AdoQuery_2.RecordCount>0 then
              begin
                while not AdoQuery_2.eof do
                begin
                  StringList.Add(AdoQuery_2.FieldByName('ID').AsString+SplitChar+
                  AdoQuery_2.FieldByName('Name').AsString+SplitChar+
                  AdoQuery_2.FieldByName('Description').AsString);
                  AdoQuery_2.Next;
                end;
              end;
            finally
              AdoQuery_2.Free;
            end;
          end;
          Result:= StringList;
        end;
        AdoQuery.Next;
      end;
      PropertyList.Free;
    end
    else
    begin
      Result:=nil;
    end;
  finally
    AdoQuery.Free;
  end;
end;

function TDataBaseUtil.QueryClassInfoByID(ID: Integer): TStringList;
var
  AdoQuery,AdoQuery_2 : TADOQuery;
  StringList:TStringList;
  tableIndex:Integer;
  valueNameArr,valueArr:TArray<string>;
  tableName:string;
  sql:string;
begin
   AdoQuery:=TADOQuery.Create(nil);
  // AdoQuery_2:=TADOQuery.Create(nil);
   try
      AdoQuery.Connection:=self.Connection;
     // AdoQuery_2.Connection:=self.Connection;
      //query twice
      tableIndex:=self.CalculateTableIndex(self.CalculateIndex(ID));
      AdoQuery.SQL.Add('select * from mainSnak_'+intTostr(tableIndex)+
      ' where QID='+inttostr(ID)+' and propertyId=31');
      AdoQuery.Open;
      StringList:=TStringList.Create;
      if AdoQuery.RecordCount>0 then
      begin
        AdoQuery.First;
        while not  AdoQuery.eof do
        begin
          valueNameArr:=AdoQuery.FieldByName('nameArr').AsString.Split(['&&&&']);
          if (valueNameArr<>nil) and (Length(valueNameArr)=2) then
          begin
            valueArr:=AdoQuery.FieldByName('valueArr').AsString.Split(['&&&&']);
            if (valueArr<>nil) and (Length(valueArr)=2) then
            begin
              if valueArr[0]='item' then
              begin
                tableName:='itemInfo';
                sql:='select * from itemInfo where ID='+valueArr[1]+'';
              end
              else if valueArr[0]='property' then
              begin
                tableName:='propertyInfo';
                sql:='select * from propertyInfo where ID="P'+valueArr[1]+'"';
              end;
              AdoQuery_2:=TADOQuery.Create(nil);
              AdoQuery_2.Connection:=self.Connection;
              AdoQuery_2.SQL.Add(sql);
              AdoQuery_2.Open;
              if AdoQuery_2.RecordCount>0 then
              begin
                while not  AdoQuery_2.eof do
                begin
                  StringList.Add(AdoQuery_2.FieldByName('ID').AsString+SplitChar+
                  AdoQuery_2.FieldByName('Name').AsString+SplitChar+
                  AdoQuery_2.FieldByName('Description').AsString);
                  AdoQuery_2.Next;
                end;
              end;
              AdoQuery_2.Free;
            end;
          end;
          AdoQuery.Next;
        end;
      end;
      AdoQuery.Free;
     // AdoQuery_2.Free;
      AdoQuery:=TADOQuery.Create(nil);
      //AdoQuery_2:=TADOQuery.Create(nil);
      AdoQuery.Connection:=self.Connection;
      //AdoQuery_2.Connection:=self.Connection;
      //query twice
      tableIndex:=self.CalculateTableIndex(self.CalculateIndex(ID));
      AdoQuery.SQL.Add('select * from mainSnak_'+intTostr(tableIndex)+
      ' where QID='+inttostr(ID)+' and propertyId=279');
      AdoQuery.Open;
      //StringList:=TStringList.Create;
      if AdoQuery.RecordCount>0 then
      begin
        AdoQuery.First;
        while not  AdoQuery.eof do
        begin
          valueNameArr:=AdoQuery.FieldByName('nameArr').AsString.Split(['&&&&']);
          if (valueNameArr<>nil) and (Length(valueNameArr)=2) then
          begin
            valueArr:=AdoQuery.FieldByName('valueArr').AsString.Split(['&&&&']);
            if (valueArr<>nil) and (Length(valueArr)=2) then
            begin
              if valueArr[0]='item' then
              begin
                tableName:='itemInfo';
                sql:='select * from itemInfo where ID='+valueArr[1]+'';
              end
              else if valueArr[0]='property' then
              begin
                tableName:='propertyInfo';
                sql:='select * from propertyInfo where ID="P'+valueArr[1]+'"';
              end;
              AdoQuery_2:=TADOQuery.Create(nil);
              AdoQuery_2.Connection:=Self.Connection;
              AdoQuery_2.SQL.Add(sql);
              AdoQuery_2.Open;
              if AdoQuery_2.RecordCount>0 then
              begin
                while not  AdoQuery_2.eof do
                begin
                  StringList.Add(AdoQuery_2.FieldByName('ID').AsString+SplitChar+
                  AdoQuery_2.FieldByName('Name').AsString+SplitChar+
                  AdoQuery_2.FieldByName('Description').AsString);
                  AdoQuery_2.Next;
                end;
              end;
              AdoQuery_2.Free;
            end;
          end;
          AdoQuery.Next;
        end;
      end;
      result:=Stringlist;
   finally
      AdoQuery.Free;
     // AdoQuery_2.Free;
   end;
end;

function TDataBaseUtil.QueryEntityInfo(sql: string): TStringList;
var
  AdoQuery : TADOQuery;
  StringList:TStringList;
begin
  AdoQuery:=TADOQuery.Create(nil);
  try
    AdoQuery.Connection:=self.Connection;
    AdoQuery.SQL.Add(sql);
    AdoQuery.Open;
    StringList:=TStringList.Create;
    while not  AdoQuery.eof do
    begin
      StringList.Add(AdoQuery.FieldByName('ID').AsString+SplitChar+
      AdoQuery.FieldByName('Name').AsString+SplitChar+
      AdoQuery.FieldByName('Description').AsString);
      AdoQuery.Next;
    end;
    result:=StringList;
  finally
    AdoQuery.Free;
  end;
end;

function TDataBaseUtil.QueryEntityInfoByName(EntityName:String): TStringList;
begin
  result:=self.QueryEntityInfo('select ID,Name,Description from itemInfo where Name="'+EntityName+'"');
end;

function TDataBaseUtil.QueryRelatedEntityInfo(ID: Integer): TStringList;
var
  StringList:TStringList;
  AdoQuery,AdoQuery_2: TADOQuery;
  EntityList:TStringList;
  valueArr:TArray<string>;
  I:Integer;
begin
  AdoQuery:=TADOQuery.Create(nil);
  try
    AdoQuery.Connection:=self.Connection;
    AdoQuery.SQL.Add('with t as (select * from mainSnak_'+intToStr(self.CalculateTableIndex(self.CalculateIndex(ID)))+
    ' where QID='+intToStr(ID)+') select * from t where dataType=5 and type=3');
    AdoQuery.Open;
    EntityList:=TStringList.Create;
    if AdoQuery.RecordCount>0 then
    begin
      StringList:=TStringList.Create;
      while not AdoQuery.eof do
      begin
        valueArr:=AdoQuery.FieldByName('valueArr').AsString.Split(['&&&&']);
        EntityList.Add(valueArr[1]);
        AdoQuery.Next
      end;
      for I := 0 to EntityList.Count-1 do
      begin
        AdoQuery_2:=TADOQuery.Create(nil);
        try
          AdoQuery_2.Connection:=self.Connection;
          AdoQuery_2.SQL.Add('select * from itemInfo where ID='+EntityList[i]);
          AdoQuery_2.Open;
          if AdoQuery_2.RecordCount>0 then
          begin
            while not AdoQuery_2.eof do
            begin
              StringList.Add(AdoQuery_2.FieldByName('ID').AsString+SplitChar+
              AdoQuery_2.FieldByName('Name').AsString+SplitChar+
              AdoQuery_2.FieldByName('Description').AsString);
              AdoQuery_2.Next;
            end;
          end;
        finally
          AdoQuery_2.Free;
        end;
      end;
      Result:=StringList;
      exit;
    end
    else
    begin
      result:=nil;
      Exit;
    end;
  finally
    AdoQuery.Free;
  end;
end;

end.
