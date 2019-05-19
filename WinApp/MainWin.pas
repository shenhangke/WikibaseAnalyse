unit MainWin;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Data.DB, Datasnap.DBClient, SimpleDS,
  Data.SqlExpr, Data.DBXMySQL, Data.DBXOracle, Data.Win.ADODB, Vcl.StdCtrls;

type
  TForm1 = class(TForm)
    ConnectionMySql: TSQLConnection;
    SimDataSetMySql: TSimpleDataSet;
    DataSource: TDataSource;
    con1: TADOConnection;
    btn1: TButton;
    qry1: TADOQuery;
    procedure btn1Click(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.dfm}

procedure TForm1.btn1Click(Sender: TObject);
begin
  try
    self.con1.ConnectionString:='Driver={MySQL ODBC 8.0 Unicode Driver};Server=127.0.0.1;Database=wikibase;User=root; Password=5166266skf;Option=3;';
    self.con1.LoginPrompt:=false;
    self.con1.Connected:=True;
    self.qry1.Connection:=self.con1;
    self.qry1.SQL.Add('select * from ItemInfo where ID=1');
    self.qry1.Open;
    while not self.qry1.Eof do
    begin
      ShowMessage(self.qry1.FieldByName('QIndex').AsString+' '+
      self.qry1.FieldByName('ID').AsString+' '+
      self.qry1.FieldByName('Name').AsString+' '+
      self.qry1.FieldByName('Description').AsString);
      self.qry1.Next;
    end;
  except on e:Exception do
    begin
      ShowMessage('connection failed');
    end;
  end;
end;

end.