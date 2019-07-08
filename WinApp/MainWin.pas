unit MainWin;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Data.DB, Datasnap.DBClient, SimpleDS,
  Data.SqlExpr, Data.DBXMySQL, Data.DBXOracle, Data.Win.ADODB, Vcl.StdCtrls,
  Vcl.ExtCtrls, Vcl.Imaging.pngimage, RdImageButton, RdLabel, PPageNote,
  Vcl.ComCtrls, RdListView,DataBaseUtil, Datasnap.DSMetadata,
  Datasnap.DSServerMetadata, Vcl.Imaging.jpeg,ErrorCode;

type
  TWikibaseApp = class(TForm)
    ImageBtn_close: TRdImageButton;
    img_bg: TImage;
    ImageBtn_min: TRdImageButton;
    PageNote1: TPageNote;
    PageTab_1: TPageTab;
    img_tab_1_bg: TImage;
    ImageBtn_search: TRdImageButton;
    rdlbl_pageTitle: TRdLabel;
    rdlbl_entiyName: TRdLabel;
    img_move_top: TImage;
    edt_entityName: TEdit;
    rdlbl_Q1_result: TRdLabel;
    lv_Q1: TListView;
    img_point_1: TImage;
    img_point_2: TImage;
    img_point_3: TImage;
    img_point_4: TImage;
    img_point_5: TImage;
    img_point_press_store: TImage;
    img_point_nopress_store: TImage;
    PageTab_2: TPageTab;
    img_bg_tab2: TImage;
    rdlbl_title_2: TRdLabel;
    rdlbl_entityName_2: TRdLabel;
    edt_entityName_2: TEdit;
    rdlbl_result_2: TRdLabel;
    ImageBtn_searchClass: TRdImageButton;
    lv_result_2: TListView;
    PageTab_3: TPageTab;
    img_bg_tab3: TImage;
    rdlbl_title_3: TRdLabel;
    rdlbl_entityName_3: TRdLabel;
    edt_entityName_3: TEdit;
    rdlbl_result_3: TRdLabel;
    lv_result_3: TListView;
    ImageBtn_relate: TRdImageButton;
    PageTab_4: TPageTab;
    img_bg_4: TImage;
    rdlbl_title_4: TRdLabel;
    rdlbl_entityName_4: TRdLabel;
    edt_entityName_4: TEdit;
    rdlbl_result_4: TRdLabel;
    lv_result_4: TListView;
    ImageBtn_getProperty: TRdImageButton;
    img_pikaqiu: TImage;
    img_pikaqiu_1: TImage;
    img_pikaqiu_2: TImage;
    rdlbl_total_tile: TRdLabel;
    PageTab_5: TPageTab;
    img_bg_5: TImage;
    rdlbl_title_5: TRdLabel;
    rdlbl_entityName_5: TRdLabel;
    edt_entityName_5: TEdit;
    rdlbl_relation: TRdLabel;
    edt_relation: TEdit;
    rdlbl_result_5: TRdLabel;
    ImageBtn_QA: TRdImageButton;
    lv_result_5: TListView;
    procedure ImageBtn_closeClick(Sender: TObject);
    procedure ImageBtn_minClick(Sender: TObject);
    procedure img_move_topMouseMove(Sender: TObject; Shift: TShiftState; X,
      Y: Integer);
    procedure FormCreate(Sender: TObject);
    procedure ImageBtn_searchClick(Sender: TObject);
    procedure img_point_1MouseEnter(Sender: TObject);
    procedure img_point_2MouseEnter(Sender: TObject);
    procedure img_point_3MouseEnter(Sender: TObject);
    procedure img_point_4MouseEnter(Sender: TObject);
    procedure img_point_5MouseEnter(Sender: TObject);
    procedure ImageBtn_searchClassClick(Sender: TObject);
    procedure img_point_1Click(Sender: TObject);
    procedure img_point_2Click(Sender: TObject);
    procedure ImageBtn_relateClick(Sender: TObject);
    procedure img_point_3Click(Sender: TObject);
    procedure ImageBtn_getPropertyClick(Sender: TObject);
    procedure img_point_4Click(Sender: TObject);
    procedure ImageBtn_getPropertyMouseEnter(Sender: TObject);
    procedure ImageBtn_searchMouseEnter(Sender: TObject);
    procedure ImageBtn_searchClassMouseEnter(Sender: TObject);
    procedure ImageBtn_relateMouseEnter(Sender: TObject);
    procedure rdlbl_total_tileMouseMove(Sender: TObject; Shift: TShiftState; X,
      Y: Integer);
    procedure img_point_5Click(Sender: TObject);
    procedure ImageBtn_QAClick(Sender: TObject);
  private

  public

  end;

var
  WikibaseApp: TWikibaseApp;

implementation

{$R *.dfm}

procedure TWikibaseApp.FormCreate(Sender: TObject);
begin
  self.PageNote1.ActivePage:=self.PageTab_1;
  self.DoubleBuffered:=True;
  self.img_point_1.Picture.Assign(self.img_point_press_store.Picture);
end;

procedure TWikibaseApp.ImageBtn_closeClick(Sender: TObject);
begin
  Self.Close;
end;

procedure TWikibaseApp.ImageBtn_getPropertyClick(Sender: TObject);
var
  DBUtil:TDatabaseUtil;
  resultList:TStringList;
  i,j:Integer;
  strArr:TArray<String>;
begin
  if self.edt_entityName_4.Text='' then
  begin
    ShowMessage('please fill the entity');
    Exit;
  end;
  self.lv_result_4.Clear;
  DBUtil:=TDataBaseUtil.Create;
  resultList:=DBUtil.QueryAllPropertyInfo(StrToInt(self.edt_entityName_4.Text));
  if (resultList=nil) or (resultList.Count=0) then
    Exit;
  try
    if (resultList.Count>0) then
    begin
      for i := 0 to resultList.Count-1 do
      begin
        with self.lv_result_4.Items.Add do
        begin
          strArr:=resultList[i].Split([SplitChar]);
          if strArr[0].Contains('P') then
          begin
            Caption:=strArr[0];
          end
          else
          begin
            Caption:='Q'+strArr[0];
          end;

         // Font.Color:=clWhite;
          for j := 1 to Length(strArr) do
          begin
            SubItems.Add(strArr[j]);
          end;
        end;
      end;
    end;
  finally
    DBUtil.Free;
    if resultList<>nil then
      resultList.Free;
  end;
end;

procedure TWikibaseApp.ImageBtn_getPropertyMouseEnter(Sender: TObject);
begin
  self.ImageBtn_getProperty.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.ImageBtn_minClick(Sender: TObject);
begin
  SendMessage(handle, wm_SysCommand, sc_Minimize, 0);
end;

procedure TWikibaseApp.ImageBtn_QAClick(Sender: TObject);
var
  DBUtil:TDatabaseUtil;
  resultList:TStringList;
  I:Integer;
begin
  if (self.edt_entityName_5.Text='') or (self.edt_relation.Text='') then
  begin
    ShowMessage('please fill the blank');
    Exit;
  end;
  DBUtil:=TDataBaseUtil.Create;
  try
    resultList:=DBUtil.GetQAAnswer(self.edt_entityName_5.Text,self.edt_relation.Text);
    lv_result_5.Clear;
    if resultList<>nil then
    begin
      try
        for I := 0 to resultList.Count-1 do
        begin
          with self.lv_result_5.Items.Add do
          begin
            Caption:=resultList[i];
          end;
        end;
      finally
        resultList.Free;
      end;
    end
    else
    begin
      ShowMessage('error code is: '+inttostr(ErrorCode.getLastError())+' please contract with developer');
    end;
  finally
    DBUtil.Free;
  end;
end;

procedure TWikibaseApp.ImageBtn_relateClick(Sender: TObject);
var
  DBUtil:TDatabaseUtil;
  resultList:TStringList;
  i,j:Integer;
  strArr:TArray<String>;
begin
  if self.edt_entityName_3.Text='' then
  begin
    ShowMessage('please fill the entity');
    Exit;
  end;
  self.lv_result_3.Clear;
  DBUtil:=TDataBaseUtil.Create;
  resultList:=DBUtil.QueryRelatedEntityInfo(StrToInt(self.edt_entityName_3.Text));
  if (resultList=nil) or (resultList.Count=0) then
    Exit;
  try
    if (resultList.Count>0) then
    begin
      for i := 0 to resultList.Count-1 do
      begin
        with self.lv_result_3.Items.Add do
        begin
          strArr:=resultList[i].Split([SplitChar]);
          if strArr[0].Contains('P') then
          begin
            Caption:=strArr[0];
          end
          else
          begin
            Caption:='Q'+strArr[0];
          end;

         // Font.Color:=clWhite;
          for j := 1 to Length(strArr) do
          begin
            SubItems.Add(strArr[j]);
          end;
        end;
      end;
    end;
  finally
    DBUtil.Free;
    if resultList<>nil then
      resultList.Free;
  end;
end;

procedure TWikibaseApp.ImageBtn_relateMouseEnter(Sender: TObject);
begin
  ImageBtn_relate.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.ImageBtn_searchClassClick(Sender: TObject);
var
  DBUtil:TDatabaseUtil;
  resultList:TStringList;
  i,j:Integer;
  strArr:TArray<String>;
begin
  if self.edt_entityName_2.Text='' then
  begin
    ShowMessage('please fill the entity');
    Exit;
  end;
  self.lv_result_2.Clear;
  DBUtil:=TDataBaseUtil.Create;
  resultList:=DBUtil.QueryClassInfoByID(StrToInt(self.edt_entityName_2.Text));
  try
    if (resultList.Count>0) then
    begin
      for i := 0 to resultList.Count-1 do
      begin
        with self.lv_result_2.Items.Add do
        begin
          strArr:=resultList[i].Split([SplitChar]);
          if strArr[0].Contains('P') then
          begin
            Caption:=strArr[0];
          end
          else
          begin
            Caption:='Q'+strArr[0];
          end;

         // Font.Color:=clWhite;
          for j := 1 to Length(strArr) do
          begin
            SubItems.Add(strArr[j]);
          end;
        end;
      end;
    end;
  finally
    DBUtil.Free;
    resultList.Free;
  end;
end;

procedure TWikibaseApp.ImageBtn_searchClassMouseEnter(Sender: TObject);
begin
  ImageBtn_searchClass.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.ImageBtn_searchClick(Sender: TObject);
var
  DBUtil:TDatabaseUtil;
  resultList:TStringList;
  i,j:Integer;
  strArr:TArray<String>;
begin
  if (self.edt_entityName.Text='') then
  begin
    ShowMessage('please enter the entity name');
    Exit;
  end;
  self.lv_Q1.Clear;
  DBUtil:=TDataBaseUtil.Create;
  resultList:=DBUtil.QueryEntityInfoByName(self.edt_entityName.Text);
  if (resultList.Count>0) then
  begin
    for i := 0 to resultList.Count-1 do
    begin
      with self.lv_Q1.Items.Add do
      begin
        strArr:=resultList[i].Split([SplitChar]);
       // Font.Color:=clWhite;
        Caption:='Q'+strArr[0];
        for j := 1 to Length(strArr) do
        begin
          SubItems.Add(strArr[j]);
        end;
      end;
    end;
  end;
  self.edt_entityName.Text:=self.edt_entityName.Text;
  DBUtil.Free;
  resultList.Free;
end;

procedure TWikibaseApp.ImageBtn_searchMouseEnter(Sender: TObject);
begin
  ImageBtn_search.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.img_move_topMouseMove(Sender: TObject;
  Shift: TShiftState; X, Y: Integer);
begin
  ReleaseCapture;
  PostMessage(handle, wm_SysCommand, $F012, 0);
end;

procedure TWikibaseApp.img_point_1Click(Sender: TObject);
begin
  self.img_point_1.Picture.Assign(self.img_point_press_store.Picture);
  self.img_point_2.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_3.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_4.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_5.Picture.Assign(self.img_point_nopress_store.Picture);
  self.PageNote1.ActivePage:=self.PageTab_1;
end;

procedure TWikibaseApp.img_point_1MouseEnter(Sender: TObject);
begin
  self.img_point_1.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.img_point_2Click(Sender: TObject);
begin
  self.img_point_2.Picture.Assign(self.img_point_press_store.Picture);
  self.img_point_1.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_3.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_4.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_5.Picture.Assign(self.img_point_nopress_store.Picture);
  self.PageNote1.ActivePage:=self.PageTab_2;
end;

procedure TWikibaseApp.img_point_2MouseEnter(Sender: TObject);
begin
  self.img_point_2.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.img_point_3Click(Sender: TObject);
begin
  self.img_point_3.Picture.Assign(self.img_point_press_store.Picture);
  self.img_point_2.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_1.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_4.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_5.Picture.Assign(self.img_point_nopress_store.Picture);
  self.PageNote1.ActivePage:=self.PageTab_3;
end;

procedure TWikibaseApp.img_point_3MouseEnter(Sender: TObject);
begin
  self.img_point_3.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.img_point_4Click(Sender: TObject);
begin
  self.img_point_4.Picture.Assign(self.img_point_press_store.Picture);
  self.img_point_2.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_1.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_3.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_5.Picture.Assign(self.img_point_nopress_store.Picture);
  self.PageNote1.ActivePage:=self.PageTab_4;
end;

procedure TWikibaseApp.img_point_4MouseEnter(Sender: TObject);
begin
  self.img_point_4.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.img_point_5Click(Sender: TObject);
begin
  self.img_point_5.Picture.Assign(self.img_point_press_store.Picture);
  self.img_point_2.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_1.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_3.Picture.Assign(self.img_point_nopress_store.Picture);
  self.img_point_4.Picture.Assign(self.img_point_nopress_store.Picture);
  self.PageNote1.ActivePage:=self.PageTab_5;
end;

procedure TWikibaseApp.img_point_5MouseEnter(Sender: TObject);
begin
  self.img_point_5.Cursor:=crHandPoint;
end;

procedure TWikibaseApp.rdlbl_total_tileMouseMove(Sender: TObject;
  Shift: TShiftState; X, Y: Integer);
begin
  ReleaseCapture;
  PostMessage(handle, wm_SysCommand, $F012, 0);
end;

end.
