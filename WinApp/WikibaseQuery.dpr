program WikibaseQuery;

uses
  Vcl.Forms,
  MainWin in 'MainWin.pas' {WikibaseApp};

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TWikibaseApp, WikibaseApp);
  Application.Run;
end.
