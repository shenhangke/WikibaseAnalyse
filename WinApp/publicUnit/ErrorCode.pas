unit ErrorCode;

interface
uses
 System.SysUtils,System.Classes;

 const No_Such_Object=0;
 const No_such_Relation=1;
 const No_Such_Subject=2;

 var
  errorCodeIndex:Integer;

  function getLastError():Integer;

implementation

function getLastError():Integer;
begin
  Result:=errorCodeIndex;
end;

end.
