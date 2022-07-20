unit uMain;

interface

uses
  Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms, FileCtrl,
  Dialogs, StdCtrls, Buttons, Mask, JvExMask, JvToolEdit,
  udatatypes_apps, ucore, ComCtrls, CheckLst, ZAbstractDataset, ZDataset,
  ExtCtrls, ClassParametrosDeEntrada, ClassExpressaoRegular;

type

  TfrmMain = class(TForm)
    btnSobre: TBitBtn;
    btnSair: TBitBtn;
    pgcMain: TPageControl;
    tbsEntrada: TTabSheet;
    tbsSaida: TTabSheet;
    tbsExecutar: TTabSheet;
    btnExecutar: TBitBtn;
    lblCaminhoArquivosEntrada: TLabel;
    btnSelecionarTodos: TBitBtn;
    btnLimparSelecao: TBitBtn;
    cltArquivos: TCheckListBox;
    lblInfos: TLabel;
    lblCaminhoArquivosSaida: TLabel;
    lblIdProcessamento: TLabel;
    lblIdProcessamentoValor: TLabel;
    lblNumeroDoLotePedido: TLabel;
    lblNumeroDoLotePedidoValor: TLabel;
    tsRelatorios: TTabSheet;
    lblLote: TLabel;
    edtLote: TEdit;
    btnPesquisar: TButton;
    mmoRelatorio: TMemo;
    rgTipoLote: TRadioGroup;
    edtSalvarRelatorio: TJvDirectoryEdit;
    lblSalvarRelatorio: TLabel;
    btnSalvarRelatorio: TButton;
    edtPathEntrada: TJvDirectoryEdit;
    edtPathSaida: TJvDirectoryEdit;
    btnConfereQuantidades: TButton;
    pnl_Numero_de_lotes: TPanel;
    pnl_Numero_de_objetos: TPanel;
    pnl_Peso_total: TPanel;
    lblInformePeso: TLabel;
    lblInformeNovoPeso: TLabel;
    edtPesoOld: TEdit;
    edtPesoNew: TEdit;
    lstPesosOld: TListBox;
    lstPesosNew: TListBox;
    btnAddPesos: TButton;
    btnClearPesos: TButton;
    btnPesosDel: TButton;
    procedure btnSairClick(Sender: TObject);
    procedure btnSobreClick(Sender: TObject);
    procedure btnExecutarClick(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure btnSelecionarTodosClick(Sender: TObject);
    procedure btnLimparSelecaoClick(Sender: TObject);
    procedure cltArquivosClick(Sender: TObject);
    procedure DesmarcaAnteriores();
    procedure FormCreate(Sender: TObject);
    procedure btnPesquisarClick(Sender: TObject);
    procedure btnSalvarRelatorioClick(Sender: TObject);
    procedure edtPathEntradaChange(Sender: TObject);
    procedure btnConfereQuantidadesClick(Sender: TObject);
    procedure ConfereQuantidades(Var iNumeroDeLotes, iNumeroDeObjetos, iPeso: Integer);
    procedure pgcMainChange(Sender: TObject);
    procedure btnAddPesosClick(Sender: TObject);
    procedure btnPesosDelClick(Sender: TObject);

  private
    { Private declarations }

    {
      Variável privada que contêm todos os parâmetros de entrada do Form para
      uCore.

      Todas as entradas de conponentes gráficos devem ser declarados no record
      RParametrosEntrada que se econtra no ucore.pas.

      E passadas diretamente para a Função Executar, onde a mesma fará o
      relacionamento do parâmetro gráfico com o parâmetro do record.

    }

    procedure AboutApplication(autores: String);
    procedure AtualizarArquivosEntrada(Path: String; focoAutomatico: boolean=false);
    function  ValidarParametrosInformados(ParametrosDeEntrada: TParametrosDeEntrada): Boolean;
    function Executar(): Boolean;
    procedure AtualizarListagemDeArquivos(path: String);
    procedure AtualizarQtdeArquivosMarcados();

    procedure LimparSelecao();
    procedure SelecionarTodos();
    procedure LogarParametrosDeEntrada(ParametrosDeEntrada: TParametrosDeEntrada);

    {Verifica se o programa já está aberto}
    function AplicacaoEstaAberta(NomeAPP: PChar): Boolean;
   {Converte String em Pchar}
    function StrToPChar(const Str: string): PChar;

    function GetUsuarioLogado(): String;

    procedure AddPesos();
    procedure ClearPesos();
    procedure DelPesos();


  public
    { Public declarations }

    sPathEntrada             : string;
    objCore                  : TCore;

  end;

var
  frmMain       : TfrmMain;
  bAppEstaAberto : Boolean;

implementation

{$R *.dfm}

procedure TfrmMain.btnSairClick(Sender: TObject);
begin
  Application.Terminate;
end;

procedure TfrmMain.btnSobreClick(Sender: TObject);
begin

  AboutApplication('Eduardo C. M. Monteiro');

end;

procedure TfrmMain.LogarParametrosDeEntrada(ParametrosDeEntrada: TParametrosDeEntrada);
var
  iContArquivos : Integer;
begin
  objCore.objLogar.Logar('[DEBUG] ID..............................: ' + ParametrosDeEntrada.ID_PROCESSAMENTO);
  objCore.objLogar.Logar('[DEBUG] ARQUIVOS SELECIONADOS...........: ');

  for iContArquivos := 0 TO ParametrosDeEntrada.LISTADEARQUIVOSDEENTRADA.Count -1 DO
    objCore.objLogar.Logar('[DEBUG] -> ' + ParametrosDeEntrada.LISTADEARQUIVOSDEENTRADA.Strings[iContArquivos]);

  objCore.objLogar.Logar('[DEBUG] INFORMAÇÕES.....................: ' + ParametrosDeEntrada.INFORMACAO_DOS_ARQUIVOS_SELECIONADOS);
  objCore.objLogar.Logar('[DEBUG] PATH ENTRADA....................: ' + ParametrosDeEntrada.PATHENTRADA);
  objCore.objLogar.Logar('[DEBUG] PATH SAIDA......................: ' + ParametrosDeEntrada.PATHSAIDA);
  objCore.objLogar.Logar('[DEBUG] PATH ARQUIVOS TEMPORARIOS.......: ' + ParametrosDeEntrada.PATHARQUIVO_TMP);
  objCore.objLogar.Logar('[DEBUG] TABELA DE PROCESSAMENTO.........: ' + ParametrosDeEntrada.TABELA_PROCESSAMENTO);
  objCore.objLogar.Logar('[DEBUG] TABELA DE PLANO DE TRIAGEM......: ' + ParametrosDeEntrada.TABELA_PLANO_DE_TRIAGEM);
  objCore.objLogar.Logar('[DEBUG] NUMERO DE REGISTROS POR SELECT..: ' + ParametrosDeEntrada.LIMITE_DE_SELECT_POR_INTERACOES_NA_MEMORIA);
end;

function TfrmMain.Executar(): Boolean;
var
  sListaMensagens: string;
  iNumeroDeErros: integer;

begin

  LogarParametrosDeEntrada(objCore.objParametrosDeEntrada);

  if not ValidarParametrosInformados(objCore.objParametrosDeEntrada) then
  begin
    showmessage('[ERRO] Erros ocorreram. Confira o arquivo de Log.');
    exit;
  end
  else
  begin

    btnExecutar.Enabled := False;

    btnExecutar.Enabled := false;
    screen.Cursor       := crSQLWait;

    objCore.MainLoop();

    btnExecutar.Enabled := true;
    screen.Cursor := crDefault;

  end;

end;

function TfrmMain.ValidarParametrosInformados(ParametrosDeEntrada: TParametrosDeEntrada): Boolean;
var
  bValido        : boolean;
  sMSG           : string;

begin

  bValido := true;
   //TODA SUA VALIDADÇÃO AQUI

  if ParametrosDeEntrada.LISTADEARQUIVOSDEENTRADA.Count <= 0  then
  begin
    bValido := False;
    sMSG    := '[ERRO] Nenhum arquivo selecionado. O programa será encerrado agora.'+#13#10#13#10;
    showmessage(sMSG);
    objCore.objLogar.Logar(sMSG);
  end;

  // flag que define se todos os parâmetros estão válidos.
  Result:= bValido;

end;

procedure TfrmMain.btnExecutarClick(Sender: TObject);
var
  ListaDeArquivosSelecionados: TStringList;
  iContArquivosSelecionados: Integer;
  sMSG : string;
begin
  try
    try

      ListaDeArquivosSelecionados:= TStringList.Create();
      for iContArquivosSelecionados:= 0 to cltArquivos.Count - 1 do
        if cltArquivos.Checked[iContArquivosSelecionados] then
          ListaDeArquivosSelecionados.Add(cltArquivos.Items[iContArquivosSelecionados]);

      objCore.objParametrosDeEntrada.ID_Processamento                              := lblIdProcessamentoValor.Caption;
      objCore.objParametrosDeEntrada.PathEntrada                                   := edtPathEntrada.Text;
      objCore.objParametrosDeEntrada.PathSaida                                     := edtPathSaida.Text;
      objCore.objParametrosDeEntrada.ListaDeArquivosDeEntrada                      := ListaDeArquivosSelecionados;

      objCore.objParametrosDeEntrada.INFORMACAO_DOS_ARQUIVOS_SELECIONADOS          := lblInfos.Caption;

      objCore.objParametrosDeEntrada.HORA_INICIO_PROCESSO                          := Now;

      objCore.objParametrosDeEntrada.stlListaPesosOld                              := lstPesosOld.Items;
      objCore.objParametrosDeEntrada.stlListaPesosNew                              := lstPesosNew.Items;



      Executar(); //rrParametrosRetorno                                                       := Executar();

  	  lblNumeroDoLotePedidoValor.Caption                                           := objCore.objParametrosDeEntrada.PEDIDO_LOTE;

      objCore.objParametrosDeEntrada.HORA_FIM_PROCESSO                             := Now;

      LimparSelecao;
      AtualizarArquivosEntrada(edtPathEntrada.Text, true);
      pgcMain.TabIndex := 0;

    finally
      ListaDeArquivosSelecionados.Clear;
      FreeAndNil(ListaDeArquivosSelecionados);

      objCore.objLogar.Logar('[DEBUG] INICIO PROCESSO...: ' + FormatDateTime('DD/MM/YYYY - hh:mm:ss', objCore.objParametrosDeEntrada.HORA_INICIO_PROCESSO));
      objCore.objLogar.Logar('[DEBUG] FIM PROCESSO......: ' + FormatDateTime('DD/MM/YYYY - hh:mm:ss', objCore.objParametrosDeEntrada.HORA_FIM_PROCESSO));
      objCore.objLogar.Logar('[DEBUG] DURACAO PROCESSO..: ' + FormatDateTime('hh:mm:ss', objCore.objParametrosDeEntrada.HORA_FIM_PROCESSO -
                                                                                         objCore.objParametrosDeEntrada.HORA_INICIO_PROCESSO));

      // 0 ----------------------------------------------------0
      // | COPIA O ARQUIVOS DE LOG PARA O DESTINO DOS ARQUIVOS |
      // 0 ----------------------------------------------------0
      CopyFile(objCore.objString.StrToPChar(objCore.objLogar.getArquivoDeLog()),
               objCore.objString.StrToPChar(objCore.objParametrosDeEntrada.PATHSAIDA + ExtractFileName(objCore.objLogar.getArquivoDeLog())), True);

      // 0 ---------------0
      // | ENVIA O E-MAIL |
      // 0 ---------------0
     // objCore.EnviarEmail('FIM DE PROCESSAMENTO !!!', sMSG + #13 + #13 + 'SEGUE LOG EM ANEXO.');

    end;

    case MessageBox (Application.Handle, Pchar ('FIM DE PROCESSAMENTO !'+#13#10#13#10+'Deseja abrir a pasta de saída? '),
     'Abrir pasta de saída', MB_YESNO) of
      idYes:
        objCore.objFuncoesWin.ExecutarArquivoComProgramaDefault(objCore.objParametrosDeEntrada.PATHSAIDA);
    end;

  except

    on E:Exception do
    begin
      sMSG := '[ERRO] Erro ao execultar a Função Executar(). '+#13#10#13#10
             +'EXCEÇÃO: '+ E.Message + #13#10#13#10
             +'O programa será encerrado agora.';

      objCore.EnviarEmail('ERRO DE PROCESSAMENTO !!!', sMSG + #13 + #13 + 'SEGUE LOG EM ANEXO.');

      showmessage(sMSG);
      objCore.objLogar.Logar(sMSG);

      Application.Terminate;
    end;
  end;

end;

procedure TfrmMain.FormShow(Sender: TObject);
var
  flLook : TextFile;
  flUser : TextFile;

  stlUser : TStringList;
begin

  TRY

    stlUser := TStringList.Create();

    if FileExists('USER.TXT') then
      stlUser.LoadFromFile('USER.TXT');

    AssignFile(flLook, 'LOOK.TXT');
    Rewrite(flLook);

    AssignFile(flUser, 'USER.TXT');
    Rewrite(flUser);

    Writeln(flUser, GetUsuarioLogado());

    CloseFile(flUser);



          objCore := TCore.Create();

          edtPathEntrada.Text                            := objCore.objParametrosDeEntrada.PATHENTRADA;
          edtPathSaida.Text                              := objCore.objParametrosDeEntrada.PATHSAIDA;
          lblIdProcessamentoValor.Caption                := objCore.objParametrosDeEntrada.ID_PROCESSAMENTO;
          lblNumeroDoLotePedidoValor.Caption             := objCore.objParametrosDeEntrada.PEDIDO_LOTE;
          frmMain.Caption                                := StringReplace(ExtractFileName(Application.ExeName), '.exe', '', [rfReplaceAll, rfIgnoreCase])
                                                            + ' - VERSAO: ' + objCore.objFuncoesWin.GetVersaoDaAplicacao()
                                                            + ' - CONECTADO EM: ' + objCore.objConexao.getHostName();

          edtLote.Clear;
          mmoRelatorio.Clear;
          edtSalvarRelatorio.Clear;

          tbsSaida.TabVisible     := False;
          tsRelatorios.TabVisible := False;

          Application.Title := StringReplace(ExtractFileName(Application.ExeName), '.exe', '', [rfReplaceAll, rfIgnoreCase]);

          pgcMain.TabIndex  := 0;
          AtualizarQtdeArquivosMarcados();

          pnl_Numero_de_lotes.Caption:= '';
          pnl_Numero_de_objetos.Caption:= '';
          pnl_Peso_total.Caption:= '';

          edtPesoOld.Text := '000000';
          edtPesoNew.Text := '000000';

          lstPesosNew.Enabled := false;

  except
      on E:Exception do
      begin

        showmessage('ATENÇÃO, PROGRAMA JÁ SE ENCONTRA ABERTO.'
                   + #13 + #13 + #13
                   + 'USUÁRIO: ' + stlUser.Text
                   +#13#10#13#10
                   //+'EXCEÇÃO: '+E.Message+#13#10#13#10
                   +'O programa será encerrado agora.');
        Application.Terminate;
      end;
  end;


end;


procedure TfrmMain.AtualizarArquivosEntrada(path: String; focoAutomatico: boolean=false);
var
  sltListaDeArquivos: TStringList;
begin
  try
    try
      sltListaDeArquivos:= TStringList.Create();
      if Path <> '' then
      begin
        sPathEntrada:=  objCore.objString.AjustaPath(Path);
        //objCore.objFuncoesWin.ObterListaDeArquivosDeUmDiretorio(Path, sltListaDeArquivos);
        objCore.objFuncoesWin.ObterListaDeArquivosDeUmDiretorioV2(Path, sltListaDeArquivos, objCore.objParametrosDeEntrada.EXTENCAO_ARQUIVO);
      end;
      cltArquivos.Items:= sltListaDeArquivos;
    except
      on E:Exception do
      begin

        showmessage('Não foi possível ler os arquivos no diretório ' + Path + '. '+#13#10#13#10
                   +'EXCEÇÃO: '+E.Message+#13#10#13#10
                   +'O programa será encerrado agora.');
        Application.Terminate;
      end;
    end;
  finally
    FreeAndNil(sltListaDeArquivos);
  end;
end;

procedure TfrmMain.AtualizarListagemDeArquivos(path: String);
var
  rListaDeObjetosDoDiretorio: RInfoArquivo;
  sNomeArquivo: string;
  sTipoItemLista: string;
  i: integer;
begin

  sPathEntrada := path;

  if copy(sPathEntrada, length(sPathEntrada), 1)<>'\' then
    sPathEntrada := sPathEntrada + '\';

  //Limpa a lista de arquivos:
  cltArquivos.Items.Clear;

  rListaDeObjetosDoDiretorio := objCore.objFuncoesWin.GetArquivos('*.*', sPathEntrada);

  for i:=0 to length(rListaDeObjetosDoDiretorio.Nome) - 1 do
  begin

    sNomeArquivo := rListaDeObjetosDoDiretorio.Nome[i];

    sTipoItemLista := objCore.objFuncoesWin.GetItemArquivoOuDiretorio(sPathEntrada+sNomeArquivo);

    if sTipoItemLista = 'arquivo' then
      cltArquivos.Items.Add(sNomeArquivo);
  end;

end;

procedure TfrmMain.btnSelecionarTodosClick(Sender: TObject);
begin
  SelecionarTodos();
end;

procedure TfrmMain.btnLimparSelecaoClick(Sender: TObject);
begin
  LimparSelecao();
end;

procedure TfrmMain.LimparSelecao();
var
  i: integer;
begin
  {Itera pela CheckListBox e marca cada item (checked = true)}

  for i:=0 to cltArquivos.Items.Count-1 do
  begin
    if cltArquivos.Checked[i] then
      cltArquivos.Checked[i] := false;
  end;

  AtualizarQtdeArquivosMarcados();
end;

procedure TfrmMain.SelecionarTodos();
var
  i: integer;
begin
 {Itera pela CheckListBox e marca cada item (checked = true)}

  for i:=0 to cltArquivos.Items.Count-1 do
    cltArquivos.Checked[i] := true;

  AtualizarQtdeArquivosMarcados();

end;

procedure TfrmMain.DesmarcaAnteriores();
var
  j : Integer;
  iMarcado : Integer;
begin

  iMarcado := cltArquivos.ItemIndex;

  {Itera na checklistbox}
  for j:=0 to cltArquivos.Items.Count-1 do
  begin
    if cltArquivos.Checked[j] then
    begin

      if j <> iMarcado then
        cltArquivos.Checked[j] := False;

    end;
  end;


end;

procedure TfrmMain.AtualizarQtdeArquivosMarcados();
var
  j, iTotalMarcados: integer;
  sNomeArquivoAtual: string;
  rrTamanhoArquivos: RFile;
  iTamArquivos:int64;
begin
  iTotalMarcados := 0;
  iTamArquivos := 0;

  {Itera na checklistbox}
  for j:=0 to cltArquivos.Items.Count-1 do
  begin
    if cltArquivos.Checked[j] then
    begin
      iTotalMarcados    := iTotalMarcados + 1;
      sNomeArquivoAtual := sPathEntrada+cltArquivos.Items[j];

      if trim(sNomeArquivoAtual) <> '' then
        iTamArquivos := iTamArquivos + objCore.objFuncoesWin.GetTamanhoArquivo_WinAPI(sNomeArquivoAtual)
      else
        iTamArquivos := iTamArquivos + 0;
    end;
  end;

//  rrTamanhoArquivos := objCore.objFuncoesWin.GetTamanhoMaiorUnidade(iTamArquivos);

//  lblInfos.Caption := inttostr(iTotalMarcados) + ' arquivo(s) marcado(s)  - '
//   +floattostr(rrTamanhoArquivos.Tamanho) + ' ' + rrTamanhoArquivos.Unidade;

  lblInfos.Caption := inttostr(iTotalMarcados) + ' arquivo(s) marcado(s)  - '
   + objCore.objFuncoesWin.GetTamanhoMaiorUnidade(iTamArquivos);

  lblInfos.Refresh;
  Application.ProcessMessages;

end;

procedure TfrmMain.cltArquivosClick(Sender: TObject);
begin

//  if chkFacRegistrado.Checked then
//    DesmarcaAnteriores();

  AtualizarQtdeArquivosMarcados();
  
end;

procedure TfrmMain.AboutApplication(autores: String);
var
  sMensagem: string;
  wDia : Word;
  wMes : Word;
  wAno : Word;
begin
  (*

   CRIADA POR: Eduardo Cordeiro M. Monteiro

  *)

  DecodeDate(Now(), wAno, wMes, wDia);

  sMensagem := Application.Title + #13#10
             + ' Versão '+ objCore.objFuncoesWin.GetVersaoDaAplicacao() + #13#10
             + ' @2010-' + IntToStr(wAno) + ' Fingerprint - ' + autores;

  showmessage(sMensagem);

end;

function TfrmMain.AplicacaoEstaAberta(NomeAPP: PChar): Boolean;
var
//não esqueça de declarar Windows esta uses
Hwnd : THandle;
begin

  Hwnd := FindWindow('TApplication', NomeAPP); //lembrando que Teste é o titulo da sua aplicação

  // se o Handle e' 0 significa que nao encontrou
  if Hwnd = 0 then
  begin
    // Não
    Result := False;
  end
  else
  Begin
    // Sim
    Result := True;
    SetForegroundWindow(Hwnd);
  end;

end;



procedure TfrmMain.FormCreate(Sender: TObject);
begin

  if AplicacaoEstaAberta(StrToPChar(StringReplace(ExtractFileName(Application.ExeName), '.exe', '', [rfReplaceAll, rfIgnoreCase])) ) then
  BEGIN
    bAppEstaAberto := True
  end
  else
  begin
    bAppEstaAberto := False;
  end;


end;

function TfrmMain.StrToPChar(const Str: string): PChar;
{Converte String em Pchar}
type
  TRingIndex = 0..7;
var
  Ring: array[TRingIndex] of PChar;
  RingIndex: TRingIndex;
  Ptr: PChar;
begin
  Ptr := @Str[length(Str)];
  Inc(Ptr);
  if Ptr^ = #0 then
  begin
  Result := @Str[1];
  end
  else
  begin
  Result := StrAlloc(length(Str)+1);
  RingIndex := (RingIndex + 1) mod (High(TRingIndex) + 1);
  StrPCopy(Result,Str);
  StrDispose(Ring[RingIndex]);
  Ring[RingIndex]:= Result;
  end;
end;

procedure TfrmMain.btnPesquisarClick(Sender: TObject);
var
  bResultado : Boolean;
begin
  bResultado := objCore.PesquisarLote(edtLote.Text, rgTipoLote.ItemIndex);
  mmoRelatorio.Clear;

  if bResultado then
    mmoRelatorio.Text := objCore.objParametrosDeEntrada.stlRelatorioQTDE.Text
  else
    ShowMessage('NÃO ENCONTRATO LOTES PARA A PESQUISA.');

  edtLote.SetFocus;
end;

procedure TfrmMain.btnSalvarRelatorioClick(Sender: TObject);
var
  sArquivo : string;
begin

  if not DirectoryExists(objCore.objString.AjustaPath(edtSalvarRelatorio.Text)) then
    ForceDirectories(objCore.objString.AjustaPath(edtSalvarRelatorio.Text));

  sArquivo := 'RELATORIO_OPERADORAS_LOTE_' + FormatFloat(objCore.objParametrosDeEntrada.FORMATACAO_LOTE_PEDIDO, strtoint(objCore.objParametrosDeEntrada.PEDIDO_LOTE_TMP)) + '.TXT';
  sArquivo := objCore.objString.AjustaPath(edtSalvarRelatorio.Text) + sArquivo;

  mmoRelatorio.Lines.SaveToFile(sArquivo);

  ShowMessage('RELATÓRIO SALVO EM :' + sArquivo);
  objCore.objFuncoesWin.ExecutarArquivoComProgramaDefault(sArquivo);

  edtLote.Clear;
  mmoRelatorio.Clear;
  edtSalvarRelatorio.Clear;
end;

procedure TfrmMain.edtPathEntradaChange(Sender: TObject);
begin
  LimparSelecao;
  AtualizarArquivosEntrada(edtPathEntrada.Text, true);
end;

function TfrmMain.getUsuarioLogado(): String;
Var
  User : DWord;
begin
  User := 50;
  SetLength(Result, User);
  GetUserName(PChar(Result), User);
  SetLength(Result, StrLen(PChar(Result)));
end;

procedure TfrmMain.btnConfereQuantidadesClick(Sender: TObject);
var
  iContLotes                  : Integer;
  iContObjetos                : Integer;
  iPesoTotal                  : Integer;
  iContArquivosSelecionados   : Integer;
  ListaDeArquivosSelecionados : TStringList;
begin
  btnExecutar.Enabled           := false;
  btnConfereQuantidades.Enabled := false;
  screen.Cursor                 := crSQLWait;

  objCore.objParametrosDeEntrada.PathEntrada                                   := edtPathEntrada.Text;
  objCore.objParametrosDeEntrada.PathSaida                                     := edtPathSaida.Text;

  if objCore.objParametrosDeEntrada.PathEntrada = '' then
    objCore.objParametrosDeEntrada.PathEntrada := '.\';

  objCore.objParametrosDeEntrada.PathEntrada := objCore.objString.AjustaPath(objCore.objParametrosDeEntrada.PathEntrada);

  ListaDeArquivosSelecionados:= TStringList.Create();
  ListaDeArquivosSelecionados.Clear;
  for iContArquivosSelecionados:= 0 to cltArquivos.Count - 1 do
    if cltArquivos.Checked[iContArquivosSelecionados] then
      ListaDeArquivosSelecionados.Add(cltArquivos.Items[iContArquivosSelecionados]);

  objCore.objParametrosDeEntrada.ListaDeArquivosDeEntrada := ListaDeArquivosSelecionados;

  iContLotes   :=0;
  iContObjetos :=0;
  iPesoTotal   :=0;

  ConfereQuantidades(iContLotes, iContObjetos, iPesoTotal);

  pnl_Numero_de_lotes.Caption   := 'Total Lotes....: ' + FormatFloat('000000000000', iContLotes);
  pnl_Numero_de_objetos.Caption := 'Total Objetos..: ' + FormatFloat('000000000000', iContObjetos);
  pnl_Peso_total.Caption        := 'Peso Total.....: ' + FormatFloat('000000000000', iPesoTotal);

  btnExecutar.Enabled           := true;
  btnConfereQuantidades.Enabled := True;
  screen.Cursor                 := crDefault;
end;

procedure TfrmMain.ConfereQuantidades(Var iNumeroDeLotes, iNumeroDeObjetos, iPeso: Integer);
var
  txtEntrada                     : TextFile;
  i                              : Integer;
  iContArquivos                  : Integer;
  sNome_do_zip                   : string;
  sDestino_dos_arquivos_temp_zip : string;
  sArquivo                       : string;
  sLote                          : string;
  sLinha                         : string;
  sTipo                          : string;

begin

//  iNumeroDeLotes   := 0;
//  iNumeroDeObjetos := 0;
//  iPeso            := 0;


  For iContArquivos := 0 to objCore.objParametrosDeEntrada.ListaDeArquivosDeEntrada.Count - 1 do
  Begin

       sArquivo := objCore.objParametrosDeEntrada.ListaDeArquivosDeEntrada.Strings[iContArquivos];

       AssignFile(txtEntrada, objCore.objParametrosDeEntrada.PathEntrada + sArquivo);
       Reset(txtEntrada);

       While Not Eof(txtEntrada) do
       Begin
         readln(txtEntrada, sLinha);
         sTipo := copy(sLinha, 1, 1);
         if sTipo = '2' then
         Begin
           iNumeroDeObjetos := iNumeroDeObjetos + 1;
           iPeso            := iPeso + StrToInt(copy(sLinha, 13, 6));
         End;
       end;
       closefile(txtEntrada);

       iNumeroDeLotes := iNumeroDeLotes + 1;

  end;

end;

procedure TfrmMain.pgcMainChange(Sender: TObject);
var
  ListaDeArquivosSelecionados : TStrings;
  iContArquivosSelecionados   : Integer;
  sMSG                        : STRING;

begin

  ListaDeArquivosSelecionados := TStrings.Create();

  ListaDeArquivosSelecionados:= TStringList.Create();
  for iContArquivosSelecionados:= 0 to cltArquivos.Count - 1 do
    if cltArquivos.Checked[iContArquivosSelecionados] then
      ListaDeArquivosSelecionados.Add(cltArquivos.Items[iContArquivosSelecionados]);

  if pgcMain.TabIndex <> 0 then
  Begin

    if (pgcMain.TabIndex = 1) then
    begin

      if ListaDeArquivosSelecionados.Count = 0 then
      begin
        sMSG := #13 + 'Você está tentando ir em EXECUTAR com ' + IntToStr(ListaDeArquivosSelecionados.Count) + ' arquivos selecionados.';
        objCore.objLogar.Logar(sMSG);
        ShowMessage(sMSG);
        pgcMain.TabIndex  := 0;
      end
      else
      if pgcMain.TabIndex = 2 then
      begin

        sMSG := #13 + 'CONFIRMAÇÃO !'+#13#10#13#10+ IntToStr(ListaDeArquivosSelecionados.Count) + ' arquivo selecionados.' + #13 + 'Deseja continuar?';
        objCore.objLogar.Logar(sMSG);

        case MessageBox (Application.Handle, Pchar (sMSG),
         'Deseja continuar ?', MB_YESNO) of
          IDYES: Begin
                   objCore.objLogar.Logar('SIM.');
                 end;

          IDNO : Begin
                   objCore.objLogar.Logar('NÃO.');
                   pgcMain.TabIndex  := 0;
                 end;
        end;

      end;
    end;

  end;
end;

procedure TfrmMain.AddPesos();
var
  sPesoOld : String;
  sPesoNew : string;
begin

  sPesoOld := trim(edtPesoOld.Text);
  sPesoNew := trim(edtPesoNew.Text);

  try

    if lstPesosOld.Items.IndexOf(FormatFloat('000000', StrToInt(sPesoOld))) < 0 then
    begin

      lstPesosNew.Items.Add(FormatFloat('000000', StrToInt(sPesoNew)));

      lstPesosOld.Items.Add(FormatFloat('000000', StrToInt(sPesoOld)));

    end;


  except

  end;


end;

procedure TfrmMain.ClearPesos();
begin

  try

      lstPesosNew.Items.Clear;
      lstPesosOld.Items.Clear;

  except

  end;


end;

procedure TfrmMain.DelPesos();
begin

  try

    lstPesosNew.Items.Delete(lstPesosOld.ItemIndex);
    lstPesosOld.Items.Delete(lstPesosOld.ItemIndex);

  except

  end;


end;

procedure TfrmMain.btnAddPesosClick(Sender: TObject);
begin
  AddPesos();
  edtPesoOld.SetFocus;

end;

procedure TfrmMain.btnPesosDelClick(Sender: TObject);
begin
  DelPesos
end;

end.


