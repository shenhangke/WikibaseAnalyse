program WikibaseQuery;

uses
  Vcl.Forms,
  MainWin in 'MainWin.pas' {WikibaseApp},
  DataBaseUtil in 'publicUnit\DataBaseUtil.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TWikibaseApp, WikibaseApp);
  Application.Run;
end.
