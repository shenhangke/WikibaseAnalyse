unit MainWin;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Data.DB, Datasnap.DBClient, SimpleDS,
  Data.SqlExpr, Data.DBXMySQL, Data.DBXOracle, Data.Win.ADODB, Vcl.StdCtrls,
  Vcl.ExtCtrls, Vcl.Imaging.pngimage, RdImageButton, RdLabel, PPageNote,
  Vcl.ComCtrls, RdListView;

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
    rdlv_Q1_result: TRdListView;
    procedure ImageBtn_closeClick(Sender: TObject);
    procedure ImageBtn_minClick(Sender: TObject);
    procedure img_move_topMouseMove(Sender: TObject; Shift: TShiftState; X,
      Y: Integer);
    procedure FormCreate(Sender: TObject);
  private

  public

  end;

var
  WikibaseApp: TWikibaseApp;

implementation

{$R *.dfm}

procedure TWikibaseApp.FormCreate(Sender: TObject);
begin
  self.PageNote1.ActivePage=self.PageTab_1;
end;

procedure TWikibaseApp.ImageBtn_closeClick(Sender: TObject);
begin
  Self.Close;
end;



procedure TWikibaseApp.ImageBtn_minClick(Sender: TObject);
begin
  SendMessage(handle, wm_SysCommand, sc_Minimize, 0);
end;

procedure TWikibaseApp.img_move_topMouseMove(Sender: TObject;
  Shift: TShiftState; X, Y: Integer);
begin
  ReleaseCapture;
  PostMessage(handle, wm_SysCommand, $F012, 0);
end;

end.
