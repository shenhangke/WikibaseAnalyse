unit DataBaseUtil;

interface
uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes,Data.DB, Datasnap.DBClient, SimpleDS,
  Data.SqlExpr, Data.DBXMySQL, Data.DBXOracle, Data.Win.ADODB, Vcl.StdCtrls;

  const DBConnectionStr='Driver={MySQL ODBC 8.0 Unicode Driver};Server=127.0.0.1;Database=wikibase;User=root; Password=5166266skf;Option=3;';

type
  TDataBaseUtil=class
  public
    constructor Create();
  private
    Connection:TADOConnection;
  end;

implementation

{ TDataBaseUtil }

constructor TDataBaseUtil.Create;
begin
  self.Connection:=TADOConnection.Create(nil);
  self.Connection.LoginPrompt:=false;
  self.Connection.Connected:=True;
end;

end.
