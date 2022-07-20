unit ucore;

interface

uses
  Windows, Messages, Variants, Graphics, Controls, FileCtrl,
  Dialogs, StdCtrls,  Classes, SysUtils, Forms, ExtCtrls,
  DB, ZConnection, ZAbstractRODataset, ZAbstractDataset, ZDataset, ZSqlProcessor,
  ADODb, DBTables, Printers, QRPrntr,
  udatatypes_apps,
  // Classes
  ClassParametrosDeEntrada,
  ClassArquivoIni, ClassStrings, ClassConexoes, ClassConf, ClassMySqlBases,
  ClassTextFile, ClassDirectory, ClassLog, ClassFuncoesWin, ClassLayoutArquivo,
  ClassBlocaInteligente, ClassFuncoesBancarias, ClassPlanoDeTriagem, ClassExpressaoRegular,
  ClassStatusProcessamento, ClassDateTime, ClassSMTPDelphi;

type

  TCore = class(TObject)
  private

    __queryMySQL_processamento__    : TZQuery;
    __queryMySQL_processamento2__   : TZQuery;
    __queryMySQL_plano_de_triagem__ : TZQuery;

    // FUNÇÃO DE PROCESSAMENTO
      Procedure PROCESSAMENTO();
      function getNumeroLotePedido(): String;
      function GravaNumeroLotePedido(NumeroLotePedido: String): Boolean;

      procedure StoredProcedure_Dropar(Nome: string; logBD:boolean=false; idprograma:integer=0);

      function StoredProcedure_Criar(Nome : string; scriptSQL: TStringList): boolean;

      procedure StoredProcedure_Executar(Nome: string; ComParametro:boolean=false; logBD:boolean=false; idprograma:integer=0);

  public

    __ListaPlanoDeTriagem__       : TRecordPlanoTriagemCorreios;

    objParametrosDeEntrada   : TParametrosDeEntrada;
    objConexao               : TMysqlDatabase;
    objPlanoDeTriagem        : TPlanoDeTriagem;
    objString                : TFormataString;
    objLogar                 : TArquivoDelog;
    objDateTime              : TFormataDateTime;
    objArquivoIni            : TArquivoIni;
    objArquivoDeConexoes     : TArquivoDeConexoes;
    objArquivoDeConfiguracao : TArquivoConf;
    objDiretorio             : TDiretorio;
    objFuncoesWin            : TFuncoesWin;
    objLayoutArquivoCliente  : TLayoutCliente;
    objBlocagemInteligente   : TBlocaInteligente;
    objFuncoesBancarias      : TFuncoesBancarias;
    objExpressaoRegular      : TExpressaoRegular;
    objStatusProcessamento   : TStausProcessamento;
    objEmail                 : TSMTPDelphi;

    function Extrair_Arquivo_7z(Arquivo, destino : String): integer;
    PROCEDURE EXTRAIR_ARQUIVO(ARQUIVO_ORIGEM, PATH_DESTINO: String);

    function Compactar_Arquivo_7z(Arquivo, destino : String; mover_arquivo: Boolean=false; ZIP: Boolean=false): integer;
    PROCEDURE COMPACTAR_ARQUIVO(ARQUIVO_ORIGEM, PATH_DESTINO: String; MOVER_ARQUIVO: Boolean=FALSE; ZIP: Boolean=false);

    function PesquisarLote(LOTE_PEDIDO : STRING; status : Integer): Boolean;

    procedure ExcluirBase(NomeTabela: String);
    procedure ExcluirTabela(NomeTabela: String);
    function EnviarEmail(Assunto: string=''; Corpo: string=''): Boolean;
    procedure MainLoop();
    constructor create();

  end;

implementation

uses uMain;

constructor TCore.create();
var
  sMSG                       : string;
  sArquivosScriptSQL         : string;
  stlScripSQL                : TStringList;
begin

  try

    stlScripSQL                          := TStringList.Create();

    objStatusProcessamento               := TStausProcessamento.create();
    objParametrosDeEntrada               := TParametrosDeEntrada.Create();

    objLogar                             := TArquivoDelog.Create();
    if FileExists(objLogar.getArquivoDeLog()) then
      objFuncoesWin.DelFile(objLogar.getArquivoDeLog());

    objFuncoesWin                        := TFuncoesWin.create(objLogar);
    objString                            := TFormataString.Create(objLogar);
    objDateTime                          := TFormataDateTime.Create(objLogar);
    objLayoutArquivoCliente              := TLayoutCliente.Create();
    objFuncoesBancarias                  := TFuncoesBancarias.Create();
    objExpressaoRegular                  := TExpressaoRegular.Create();

    objArquivoIni                        := TArquivoIni.create(objLogar,
                                                               objString,
                                                               ExtractFilePath(Application.ExeName),
                                                               ExtractFileName(Application.ExeName));

    objArquivoDeConexoes                 := TArquivoDeConexoes.create(objLogar,
                                                                      objString,
                                                                      objArquivoIni.getPathConexoes());

    objArquivoDeConfiguracao             := TArquivoConf.create(objArquivoIni.getPathConfiguracoes(),
                                                                ExtractFileName(Application.ExeName));

    objParametrosDeEntrada.ID_PROCESSAMENTO := objArquivoDeConfiguracao.getIDProcessamento;

    objConexao                           := TMysqlDatabase.Create();

    if objArquivoIni.getPathConfiguracoes() <> '' then
    begin

      objParametrosDeEntrada.PATHENTRADA                                := objArquivoDeConfiguracao.getConfiguracao('path_default_arquivos_entrada');
      objParametrosDeEntrada.PATHSAIDA                                  := objArquivoDeConfiguracao.getConfiguracao('path_default_arquivos_saida');
      objParametrosDeEntrada.TABELA_PROCESSAMENTO                       := objArquivoDeConfiguracao.getConfiguracao('tabela_processamento');
      objParametrosDeEntrada.TABELA_LOTES_PEDIDOS                       := objArquivoDeConfiguracao.getConfiguracao('TABELA_LOTES_PEDIDOS');
      objParametrosDeEntrada.TABELA_PLANO_DE_TRIAGEM                    := objArquivoDeConfiguracao.getConfiguracao('tabela_plano_de_triagem');
      objParametrosDeEntrada.CARREGAR_PLANO_DE_TRIAGEM_MEMORIA          := objArquivoDeConfiguracao.getConfiguracao('CARREGAR_PLANO_DE_TRIAGEM_MEMORIA');
      objParametrosDeEntrada.TABELA_BLOCAGEM_INTELIGENTE                := objArquivoDeConfiguracao.getConfiguracao('TABELA_BLOCAGEM_INTELIGENTE');
      objParametrosDeEntrada.TABELA_BLOCAGEM_INTELIGENTE_RELATORIO      := objArquivoDeConfiguracao.getConfiguracao('TABELA_BLOCAGEM_INTELIGENTE_RELATORIO');
      objParametrosDeEntrada.TABELA_ENTRADA_SP                          := objArquivoDeConfiguracao.getConfiguracao('TABELA_ENTRADA_SP');
      objParametrosDeEntrada.TABELA_AUX_SP                              := objArquivoDeConfiguracao.getConfiguracao('TABELA_AUX_SP');
      objParametrosDeEntrada.LIMITE_DE_SELECT_POR_INTERACOES_NA_MEMORIA := objArquivoDeConfiguracao.getConfiguracao('numero_de_select_por_interacoes_na_memoria');
      objParametrosDeEntrada.NUMERO_DE_IMAGENS_PARA_BLOCAGENS           := objArquivoDeConfiguracao.getConfiguracao('NUMERO_DE_IMAGENS_PARA_BLOCAGENS');
      objParametrosDeEntrada.BLOCAR_ARQUIVO                             := objArquivoDeConfiguracao.getConfiguracao('BLOCAR_ARQUIVO');
      objParametrosDeEntrada.BLOCAGEM                                   := objArquivoDeConfiguracao.getConfiguracao('BLOCAGEM');
      objParametrosDeEntrada.MANTER_ARQUIVO_ORIGINAL                    := objArquivoDeConfiguracao.getConfiguracao('MANTER_ARQUIVO_ORIGINAL');
      objParametrosDeEntrada.FORMATACAO_LOTE_PEDIDO                     := objArquivoDeConfiguracao.getConfiguracao('FORMATACAO_LOTE_PEDIDO');
      objParametrosDeEntrada.lista_de_caracteres_invalidos              := objArquivoDeConfiguracao.getConfiguracao('lista_de_caracteres_invalidos');
      objParametrosDeEntrada.eHost                                      := objArquivoDeConfiguracao.getConfiguracao('eHost');
      objParametrosDeEntrada.eUser                                      := objArquivoDeConfiguracao.getConfiguracao('eUser');
      objParametrosDeEntrada.eFrom                                      := objArquivoDeConfiguracao.getConfiguracao('eFrom');
      objParametrosDeEntrada.eTo                                        := objArquivoDeConfiguracao.getConfiguracao('eTo');

      objParametrosDeEntrada.IMAGEM_UNICA                               := objArquivoDeConfiguracao.getConfiguracao('IMAGEM_UNICA');
      objParametrosDeEntrada.IMAGEM_PG1                                 := objArquivoDeConfiguracao.getConfiguracao('IMAGEM_PG1');
      objParametrosDeEntrada.IMAGEM_PGN                                 := objArquivoDeConfiguracao.getConfiguracao('IMAGEM_PGN');

      objParametrosDeEntrada.IMAGEM_UNICA_FAC_REGISTRADO                := objArquivoDeConfiguracao.getConfiguracao('IMAGEM_UNICA_FAC_REGISTRADO');
      objParametrosDeEntrada.IMAGEM_PG1_FAC_REGISTRADO                  := objArquivoDeConfiguracao.getConfiguracao('IMAGEM_PG1_FAC_REGISTRADO');
      objParametrosDeEntrada.IMAGEM_PGN_FAC_REGISTRADO                  := objArquivoDeConfiguracao.getConfiguracao('IMAGEM_PGN_FAC_REGISTRADO');

      objParametrosDeEntrada.IMAGEM                                     := objArquivoDeConfiguracao.getConfiguracao('IMAGEM');
      objParametrosDeEntrada.IMAGEM_PG2                                 := objArquivoDeConfiguracao.getConfiguracao('IMAGEM_PG2');
      objParametrosDeEntrada.EXTENCAO_ARQUIVO                           := objArquivoDeConfiguracao.getConfiguracao('EXTENCAO_ARQUIVO');
      objParametrosDeEntrada.NUMERO_DE_IMPRESSOES                       := objArquivoDeConfiguracao.getConfiguracao('NUMERO_DE_IMPRESSOES');

      objParametrosDeEntrada.app_7z_32bits                              := objArquivoDeConfiguracao.getConfiguracao('app_7z_32bits');
      objParametrosDeEntrada.app_7z_64bits                              := objArquivoDeConfiguracao.getConfiguracao('app_7z_64bits');
      objParametrosDeEntrada.ARQUITETURA_WINDOWS                        := objArquivoDeConfiguracao.getConfiguracao('ARQUITETURA_WINDOWS');

      objParametrosDeEntrada.CODIGO_ADM_CONTRATO                        := objArquivoDeConfiguracao.getConfiguracao('CODIGO_ADM_CONTRATO');
      objParametrosDeEntrada.DR_POSTAGEM                                := objArquivoDeConfiguracao.getConfiguracao('DR_POSTAGEM');

      objParametrosDeEntrada.ENVIAR_EMAIL                               := objArquivoDeConfiguracao.getConfiguracao('ENVIAR_EMAIL');



      objLogar.Logar('[DEBUG] TfrmMain.FormCreate() - Versão do programa: ' + objFuncoesWin.GetVersaoDaAplicacao());

      objParametrosDeEntrada.PathArquivo_TMP := objArquivoIni.getPathArquivosTemporarios();

      // Criando a Conexao
      objConexao.ConectarAoBanco(objArquivoDeConexoes.getHostName,
                                 'mysql',
                                 objArquivoDeConexoes.getUser,
                                 objArquivoDeConexoes.getPassword,
                                 objArquivoDeConexoes.getProtocolo
                                 );

      sArquivosScriptSQL := ExtractFileName(Application.ExeName);
      sArquivosScriptSQL := StringReplace(sArquivosScriptSQL, '.exe', '.sql', [rfReplaceAll, rfIgnoreCase]);

      stlScripSQL.LoadFromFile(objArquivoIni.getPathScripSQL() + sArquivosScriptSQL);
      objConexao.ExecutaScript(stlScripSQL);

      objBlocagemInteligente   := TBlocaInteligente.create(objParametrosDeEntrada,
                                                           objConexao,
                                                           objFuncoesWin,
                                                           objString,
                                                           objLogar);

      // Criando Objeto de Plano de Triagem
      if StrToBool(objParametrosDeEntrada.CARREGAR_PLANO_DE_TRIAGEM_MEMORIA) then
        objPlanoDeTriagem := TPlanoDeTriagem.create(objConexao,
                                                    objLogar,
                                                    objString,
                                                    objParametrosDeEntrada.TABELA_PLANO_DE_TRIAGEM, fac);

      objParametrosDeEntrada.PEDIDO_LOTE      := getNumeroLotePedido();

      objParametrosDeEntrada.stlRelatorioQTDE := TStringList.Create();
      objParametrosDeEntrada.stlListaPesosNew := TStringList.Create();
      objParametrosDeEntrada.stlListaPesosOld := TStringList.Create();

    end;

  except
    on E:Exception do
    begin

      sMSG := '[ERRO] Não foi possível inicializar as configurações aq do programa. '+#13#10#13#10
            + ' EXCEÇÃO: '+E.Message+#13#10#13#10
            + ' O programa será encerrado agora.';

      showmessage(sMSG);

      objLogar.Logar(sMSG);

      Application.Terminate;
    end;
  end;

end;

function TCore.getNumeroLotePedido(): String;
var
  sComando : string;
  iPedido  : Integer;
begin
  sComando := ' SELECT max(LOTE_PEDIDO) as LOTE_PEDIDO FROM  ' + objParametrosDeEntrada.TABELA_LOTES_PEDIDOS;
  objStatusProcessamento := objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 2);

  iPedido := StrToIntDef(__queryMySQL_processamento__.FieldByName('LOTE_PEDIDO').AsString, 0) + 1;
  Result := FormatFloat(objParametrosDeEntrada.FORMATACAO_LOTE_PEDIDO, iPedido);
end;

function TCore.GravaNumeroLotePedido(NumeroLotePedido: String): Boolean;
var
  sComando : string;
  sData    : string;
begin

  sData := FormatDateTime('YYYY-MM-DD hh:mm:ss', Now());

  sComando := ' insert into ' + objParametrosDeEntrada.TABELA_LOTES_PEDIDOS + '(LOTE_PEDIDO, VALIDO, DATA_CRIACAO, RELATORIO_QTD)'
            + ' Value(' + NumeroLotePedido + ',"S", "' + sData + '","' + objParametrosDeEntrada.stlRelatorioQTDE.Text + '")';
  Result := objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 1).status;
end;


procedure TCore.MainLoop();
var
  sMSG : string;
begin

  objLogar.Logar('[DEBUG] TCore.MainLoop() - begin...');
  try
    try

      if objParametrosDeEntrada.PathEntrada = '' then
        objParametrosDeEntrada.PathEntrada := '.\';

      if objParametrosDeEntrada.PathSaida = '' then
        objParametrosDeEntrada.PathSaida := '.\';

      objDiretorio := TDiretorio.create(objParametrosDeEntrada.PathEntrada);
      objParametrosDeEntrada.PathEntrada := objDiretorio.getDiretorio();

      //objDiretorio.setDiretorio(objParametrosDeEntrada.PathSaida);
      //objParametrosDeEntrada.PathSaida   := objDiretorio.getDiretorio();

      //if not DirectoryExists(objParametrosDeEntrada.PathSaida) then
      //  ForceDirectories(objParametrosDeEntrada.PathSaida);

      PROCESSAMENTO();

    finally

      if Assigned(objDiretorio) then
      begin
        objDiretorio.destroy;
        Pointer(objDiretorio) := nil;
      end;

      if not objStatusProcessamento.status then
        ShowMessage('ERROS OCORRERAM !!!' + #13 + objStatusProcessamento.msg)
      else
      begin

        GravaNumeroLotePedido(objParametrosDeEntrada.PEDIDO_LOTE);
        objParametrosDeEntrada.PEDIDO_LOTE := getNumeroLotePedido();
      end;

    end;

  except

    // 0------------------------------------------0
    // |  Excessões desntro do objCore caem aqui  |
    // 0------------------------------------------0
    on E:Exception do
    begin

      sMSG :='Erro ao execultar a Função MainLoop(). ' + #13#10#13#10
                 +'EXCEÇÃO: '+E.Message+#13#10#13#10
                 +'O programa será encerrado agora.';

      IF StrToBool(objParametrosDeEntrada.ENVIAR_EMAIL) THEN
        EnviarEmail('ERRO DE PROCESSAMENTO !!!', sMSG + #13 + #13 + 'SEGUE LOG EM ANEXO.');

      showmessage(sMSG);
      objLogar.Logar(sMSG);

    end;
  end;

  objLogar.Logar('[DEBUG] TCore.MainLoop() - ...end');

end;

Procedure TCore.PROCESSAMENTO();
Var
//
// Variáveis básicas
flArquivoEntrada      : TextFile;
Arq_Arquivo_Saida_CIF : TextFile;
objString           : TFormataString;
sPathEntrada        : string;
sPathEntradaTMP     : string;
sPathSaida          : string;
sArquivoSaida       : string;
sLinha              : string;
sValues             : string;
sArquivoEntrada     : string;
sArquivoEntradaZIP  : string;
sArquivoEntradaCIF  : string;
sComando            : string;
sNumeroDeCartao     : string;
sCampos             : string;

sCodigoCategoria    : string;

ListaDeArquivosCIF  : TStringList;

iContArquivos       : Integer;
iTotalDeArquivos    : Integer;

iContArquivosCIF       : Integer;
iTotalDeArquivosCIF    : Integer;

iContImpressoes     : Integer;


// Variáveis de controle do select
iTotalDeRegistrosDaTabela   : Integer;
iLimit                      : Integer;
iTotalDeInteracoesDeSelects : Integer;
iResto                      : Integer;
iRegInicial                 : Integer;
iQtdeRegistros              : Integer;
iContInteracoesDeSelects    : Integer;

// Demias Variáveis
sTipoDeRegistro : string;
sListaDeLotes   : string;

sQuantidade                  : string;
sPeso                        : String;

i82015QuantidadeTotal        : Integer;
i82023QuantidadeTotal        : Integer;
i82031QuantidadeTotal        : Integer;

d82015PesoTotal              : Double;
d82023PesoTotal              : Double;
d82031PesoTotal              : Double;


iContLinhas                  : Integer;
iContLinhasNaPagina          : Integer;

iContLinhaPorPagina          : Integer;
iContPagina                  : Integer;
iTotalDePaginas              : Integer;

stlLOCAL_PesoUnitario        : TStringList;
stlLOCAL_Quantidades         : TStringList;
stlLOCAL_Totais              : TStringList;
iContLinhasLOCAL             : Integer;

stlESTADUAL_PesoUnitario        : TStringList;
stlESTADUAL_Quantidades         : TStringList;
stlESTADUAL_Totais              : TStringList;
iContLinhasESTADUAL             : Integer;

stlNACIONAL_PesoUnitario        : TStringList;
stlNACIONAL_Quantidades         : TStringList;
stlNACIONAL_Totais              : TStringList;
iContLinhasNACIONAL             : Integer;

iTotalDeLinhasLista          : Integer;


stlListaDeLotes              : TStringList;
stlCategorias                : TStringList;
stlListaDePesos              : TStringList;
stlListaDeNCartao            : TStringList;


rrLayoutArquivo              : RLayoutModelo;

//Imagem : TBitMap;
Lista             : TQRprinter;

iContCategoria         : Integer;
iContPesoUnitario      : Integer;
iContLotes             : Integer;
iContNumeroContratos   : Integer;
iNumeroDeLotesNaPagina : Integer;
iLimiteDeLotesNaPagina : Integer;
iAjusteLinha           : Integer;
iLimiteLotes           : Integer;
iLimiteColunas         : Integer;
xDesloc                : Integer;
xDesloc2               : Integer;
yDesloc                : Integer;
yDeslocPg2             : Integer;
iContLinhasExtrasOG2   : Integer;
iDeslocYLinhaExtra     : Integer;
iNumeroCampos          : Integer;

iLimiteDeLinhasPorPaginas : Integer;

img                    :TImage;
img_PG2                :TImage;

bFacRegistrado                             : Boolean;
TIPO_FAC                                   : string;
CODIGO_SERVICO_LOCAL                       : string;
CODIGO_SERVICO_ESTADUAL                    : string;
CODIGO_SERVICO_NACIONAL                    : string;
sPRIMEIRO_OBJ                              : string;
sULTIMO_OBJ                                : string;

IMG_FOLHA_UNICA                            : string;
IMG_FOLHA_01                               : string;
IMG_FOLHA_0N                               : string;

sPesoOld                                   : string;
sPesoNew                                   : string;

LOCAL                                      : string;
sFlagAR                                    : string;

iContPesosUpdate                           : Integer;

//
objArquivoSaida : TArquivoTexto;

Arq_Arquivo_Entada : TextFile;

sOperadora : string;
sContrato : string;
sCep : string;

sCartao : string;
sLote : string;
sArquivoSaidaCIF : string;
sPathMovimentoCIF : string;

Image : TBitmap;

begin

  //criarPlanoDeTriagem(ParametrosDeEntrada.TABELA_PLANO_DE_TRIAGEM);

  stlListaDeLotes      := TStringList.Create();
  stlCategorias        := TStringList.Create();
  stlListaDePesos      := TStringList.Create();
  stlListaDeNCartao    := TStringList.Create();

  //==============================================================================================
  //                         Alimentando nome dos campos da tabela de Cliente
  //==============================================================================================
  sComando := 'describe ' + objParametrosDeEntrada.tabela_processamento;
  objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 2);

  while not __queryMySQL_processamento__.Eof do
  Begin
    sCampos := sCampos + __queryMySQL_processamento__.FieldByName('Field').AsString;
    __queryMySQL_processamento__.Next;
    if not __queryMySQL_processamento__.Eof then
      sCampos := sCampos + ',';
  end;

  sComando := 'delete from ' + objParametrosDeEntrada.tabela_processamento;
  objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 1);

  //==============================================================================================
  //                               CARREGA CIF NA TABELA
  //==============================================================================================
  iTotalDeArquivos := objParametrosDeEntrada.ListaDeArquivosDeEntrada.Count;

  ListaDeArquivosCIF  := TStringList.Create();

  sPathEntrada    := objString.AjustaPath(objParametrosDeEntrada.PATHENTRADA);
  sPathEntradaTMP := sPathEntrada + 'CIF_ATUALIZADO' + PathDelim;

  for iContArquivos := 0 to iTotalDeArquivos - 1 do
  begin

    ForceDirectories(sPathEntradaTMP);

    sArquivoEntradaZIP := objParametrosDeEntrada.ListaDeArquivosDeEntrada.Strings[iContArquivos];

    EXTRAIR_ARQUIVO(sPathEntrada + sArquivoEntradaZIP, sPathEntrada);

//    EXTRAIR_ARQUIVO(sPathEntrada + sArquivoEntradaZIP, sPathEntradaTMP);

//    ListaDeArquivosCIF.Clear;
//    objFuncoesWin.ObterListaDeArquivosDeUmDiretorioV2(sPathEntradaTMP, ListaDeArquivosCIF, '*.*');

//    iTotalDeArquivosCIF := ListaDeArquivosCIF.Count;

//    for iContArquivosCIF := 0 to iTotalDeArquivosCIF -1 do
//    begin

//      sArquivoEntradaCIF := ListaDeArquivosCIF.Strings[iContArquivosCIF];

      sArquivoEntradaCIF := StringReplace(sArquivoEntradaZIP, '.ZIP', '.TXT', [rfReplaceAll, rfIgnoreCase]);

      AssignFile(flArquivoEntrada, sPathEntrada + sArquivoEntradaCIF);
      Reset(flArquivoEntrada);


      while not Eof(flArquivoEntrada) do
      Begin

        Readln(flArquivoEntrada, sLinha);

        iNumeroCampos := objString.GetNumeroOcorrenciasCaracter(sLinha, '|');

        if iNumeroCampos > 0 then
        begin

          if iNumeroCampos = 8 then
          begin

            objParametrosDeEntrada.N_CONTRATO             := objString.getTermo(1, '|', sLinha);
            sCartao                                       := objString.getTermo(2, '|', sLinha);
            sLote                                         := objString.getTermo(3, '|', sLinha);
            objParametrosDeEntrada.COD_UN_POST            := objString.getTermo(4, '|', sLinha);
            objParametrosDeEntrada.CEP_UNI_POST           := objString.getTermo(5, '|', sLinha);

            objParametrosDeEntrada.SEQUENCIA_OBJ          := '';
            objParametrosDeEntrada.COD_CATEGORIA          := '';
            sPeso                                         := '';

          end;

          if iNumeroCampos = 13 then
          begin

            objParametrosDeEntrada.SEQUENCIA_OBJ       := objString.getTermo(1, '|', sLinha);
            sPeso                                      := objString.getTermo(2, '|', sLinha);
            objParametrosDeEntrada.COD_CATEGORIA       := objString.getTermo(3, '|', sLinha);

            sPeso                                      := objString.getTermo(2, '|', sLinha);

            for iContPesoUnitario := 0 to objParametrosDeEntrada.stlListaPesosOld.Count -1 do
            begin

              sPesoOld := objParametrosDeEntrada.stlListaPesosOld.Strings[iContPesoUnitario];
              sPesoNew := objParametrosDeEntrada.stlListaPesosNew.Strings[iContPesoUnitario];

              if StrToInt(sPeso) = StrToInt(sPesoOld) then
              begin
                sPeso  := FormatFloat('000000', StrToInt(sPesoNew));
                sLinha := StringReplace(sLinha, '|' + sPesoOld + '|', '|' + sPesoNew + '|', [rfReplaceAll, rfIgnoreCase]);
                Break;
              end;
            end;



          end;

          if iNumeroCampos = 1 then
          begin
            objParametrosDeEntrada.SEQUENCIA_OBJ          := '';
            objParametrosDeEntrada.COD_CATEGORIA          := '';
            sPeso                                         := '';
          end;

            sValues := '"' + objParametrosDeEntrada.COD_CATEGORIA + '",'
                     + '"' + sPeso                                      + '",'
                     + '"' + objParametrosDeEntrada.DR_POSTAGEM         + '",'
                     + '"' + objParametrosDeEntrada.CODIGO_ADM_CONTRATO + '",'
                     + '"' + sCartao                                    + '",'
                     + '"' + sLote                                      + '",'
                     + '"' + objParametrosDeEntrada.COD_UN_POST         + '",'
                     + '"' + objParametrosDeEntrada.CEP_UNI_POST        + '",'
                     + '"' + objParametrosDeEntrada.N_CONTRATO          + '",'
                     + '"' + sLinha                                     + '",'
                     + '"' + objParametrosDeEntrada.SEQUENCIA_OBJ       + '"';

            sComando := 'Insert into ' + objParametrosDeEntrada.tabela_processamento + ' (' + sCampos + ') values(' + sValues + ')';
            objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 1);


        end
        else
        Begin

          sTipoDeRegistro := copy(sLinha, 1, 1);

          if sTipoDeRegistro = '1' then
          begin
            objParametrosDeEntrada.COD_DR                 := copy(sLinha, 2,2);
            objParametrosDeEntrada.CODIGO_ADM_CONTRATO    := copy(sLinha, 4,8);
            sCartao                                       := copy(sLinha, 12,12);
            sLote                                         := copy(sLinha, 24,5);
            objParametrosDeEntrada.COD_UN_POST            := copy(sLinha, 29,8);
            objParametrosDeEntrada.CEP_UNI_POST           := copy(sLinha, 37,8);
            objParametrosDeEntrada.N_CONTRATO             := copy(sLinha, 45,10);

            objParametrosDeEntrada.SEQUENCIA_OBJ          := '';
            objParametrosDeEntrada.COD_CATEGORIA          := '';
            sPeso                                         := '';

          end;

          if sTipoDeRegistro = '2' then
          begin
            objParametrosDeEntrada.SEQUENCIA_OBJ       := copy(sLinha, 2,11);
            sPeso                                      := copy(sLinha, 13,6);
            objParametrosDeEntrada.COD_CATEGORIA       := copy(sLinha, 27,5);

            for iContPesoUnitario := 0 to objParametrosDeEntrada.stlListaPesosOld.Count -1 do
            begin

              sPesoOld := objParametrosDeEntrada.stlListaPesosOld.Strings[iContPesoUnitario];
              sPesoNew := objParametrosDeEntrada.stlListaPesosNew.Strings[iContPesoUnitario];

              if StrToInt(sPeso) = StrToInt(sPesoOld) then
                sPeso := FormatFloat('000000', StrToInt(sPesoNew));

              sLinha := copy(sLinha, 1, 12) + sPeso + copy(sLinha, 19, 13);
            end;

          end;

          if sTipoDeRegistro = '4' then
          begin
            objParametrosDeEntrada.SEQUENCIA_OBJ          := '';
            objParametrosDeEntrada.COD_CATEGORIA          := '';
            sPeso                                         := '';
          end;

            sValues := '"' + objParametrosDeEntrada.COD_CATEGORIA + '",'
                     + '"' + sPeso                                      + '",'
                     + '"' + objParametrosDeEntrada.DR_POSTAGEM         + '",'
                     + '"' + objParametrosDeEntrada.CODIGO_ADM_CONTRATO + '",'
                     + '"' + sCartao                                    + '",'
                     + '"' + sLote                                      + '",'
                     + '"' + objParametrosDeEntrada.COD_UN_POST         + '",'
                     + '"' + objParametrosDeEntrada.CEP_UNI_POST        + '",'
                     + '"' + objParametrosDeEntrada.N_CONTRATO          + '",'
                     + '"' + sLinha                                     + '",'
                     + '"' + objParametrosDeEntrada.SEQUENCIA_OBJ       + '"';

            sComando := 'Insert into ' + objParametrosDeEntrada.tabela_processamento + ' (' + sCampos + ') values(' + sValues + ')';
            objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 1);

        end;

      end;
      CloseFile(flArquivoEntrada);

      DeleteFile(sPathEntrada + sArquivoEntradaCIF);

//    end;

  end;

  //====================================================================================
  //  CRIANDO ARQUIVO CIF
  //====================================================================================
    sComando := 'SELECT  * FROM ' + objParametrosDeEntrada.TABELA_PROCESSAMENTO
              + ' WHERE COD_CATEGORIA = 0 '
              + ' group by CARTAO, LOTE';
    objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 2);

   sPathMovimentoCIF := sPathEntradaTMP;

   while not __queryMySQL_processamento__.Eof do
   Begin

     sCartao := __queryMySQL_processamento__.FieldByName('CARTAO').AsString;
     sLote   := __queryMySQL_processamento__.FieldByName('LOTE').AsString;

     sArquivoSaidaCIF := 'FAC_' + sCartao + '_' + sLote + '_UNICA';

     IF objParametrosDeEntrada.TESTE THEN
       sArquivoSaidaCIF := sArquivoSaidaCIF + '_TESTE';

     sArquivoSaidaCIF := sArquivoSaidaCIF + '.txt';

     AssignFile(Arq_Arquivo_Saida_CIF, sPathMovimentoCIF + sArquivoSaidaCIF);
     Rewrite(Arq_Arquivo_Saida_CIF);

     //========================================================================================
     //  CABECALHO DO ARQUIVO
     //========================================================================================
     sLinha := __queryMySQL_processamento__.FieldByName('LINHA').AsString;

     writeln(Arq_Arquivo_Saida_CIF, sLinha);
     //========================================================================================

     //========================================================================================
     //  DETALHES DO ARQUIVO
     //========================================================================================
     sComando := 'SELECT  * FROM ' + objParametrosDeEntrada.TABELA_PROCESSAMENTO
               + ' WHERE CARTAO            = "' + sCartao + '"'
               + '   AND LOTE              = "' + sLote + '"'
               + '   AND SEQUENCIA_OBJ    <> "" '
               + ' ORDER BY SEQUENCIA_OBJ ';
     objConexao.Executar_SQL(__queryMySQL_processamento2__, sComando, 2);

     while not __queryMySQL_processamento2__.Eof do
     Begin

       sLinha := __queryMySQL_processamento2__.FieldByName('LINHA').AsString;

       writeln(Arq_Arquivo_Saida_CIF, sLinha);

         __queryMySQL_processamento2__.Next;

     END;
     //========================================================================================


     //==================================================================================================================================
     //  RODAPÉ DO ARQUIVO
     //==================================================================================================================================
     sComando := 'SELECT  COUNT(LOTE) AS "QTD", SUM(PESO_UNITARIO) AS "PESO" FROM ' + objParametrosDeEntrada.TABELA_PROCESSAMENTO
               + ' WHERE CARTAO            = "' + sCartao + '"'
               + '   AND LOTE              = "' + sLote + '"'
               + '   AND SEQUENCIA_OBJ    <> "" '
               + ' group by LOTE ';
     objConexao.Executar_SQL(__queryMySQL_processamento2__, sComando, 2);

     sLinha := FormatFloat('0000000', __queryMySQL_processamento2__.FieldByName('QTD').AsInteger)
       + '|' + FormatFloat('000000000000', __queryMySQL_processamento2__.FieldByName('PESO').AsInteger);
     writeln(Arq_Arquivo_Saida_CIF, sLinha);
     //==================================================================================================================================

     closefile(Arq_Arquivo_Saida_CIF);
     COMPACTAR_ARQUIVO(sPathMovimentoCIF + sArquivoSaidaCIF, ExtractFilePath(sPathMovimentoCIF + sArquivoSaidaCIF), True, True);

     __queryMySQL_processamento__.Next;

   end;

end;

procedure TCore.ExcluirBase(NomeTabela: String);
var
  sComando : String;
  sBase    : string;
begin

  sBase := objString.getTermo(1, '.', NomeTabela);

  sComando := 'drop database ' + sBase;
  objStatusProcessamento := objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 1);
end;

procedure TCore.ExcluirTabela(NomeTabela: String);
var
  sComando : String;
  sTabela  : String;
begin

  sTabela := objString.getTermo(2, '.', NomeTabela);

  sComando := 'drop table ' + sTabela;
  objStatusProcessamento := objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 1);
end;



procedure TCore.StoredProcedure_Dropar(Nome: string; logBD:boolean=false; idprograma:integer=0);
var
  sSQL: string;
  sMensagem: string;
begin
  try
    sSQL := 'DROP PROCEDURE if exists ' + Nome;
    objConexao.Executar_SQL(__queryMySQL_processamento__, sSQL, 1);
  except
    on E:Exception do
    begin
      sMensagem := '  StoredProcedure_Dropar(' + Nome + ') - Excecao:' + E.Message + ' . SQL: ' + sSQL;
      objLogar.Logar(sMensagem);
    end;
  end;

end;

function TCore.StoredProcedure_Criar(Nome : string; scriptSQL: TStringList): boolean;
var
  bExecutou    : boolean;
  sMensagem    : string;
begin


  bExecutou := objConexao.Executar_SQL(__queryMySQL_processamento__, scriptSQL.Text, 1).status;

  if not bExecutou then
  begin
    sMensagem := '  StoredProcedure_Criar(' + Nome + ') - Não foi possível carregar a stored procedure para execução.';
    objLogar.Logar(sMensagem);
  end;

  result := bExecutou;
end;

procedure TCore.StoredProcedure_Executar(Nome: string; ComParametro:boolean=false; logBD:boolean=false; idprograma:integer=0);
var

  sSQL        : string;
  sMensagem   : string;
begin

  try
    (*
    if not Assigned(con) then
    begin
      con := TZConnection.Create(Application);
      con.HostName  := objConexao.getHostName;
      con.Database  := sNomeBase;
      con.User      := objConexao.getUser;
      con.Protocol  := objConexao.getProtocolo;
      con.Password  := objConexao.getPassword;
      con.Properties.Add('CLIENT_MULTI_STATEMENTS=1');
      con.Connected := True;
    end;

    if not Assigned(QP) then
      QP := TZQuery.Create(Application);

    QP.Connection := con;
    QP.SQL.Clear;
    *)

    sSQL := 'CALL '+ Nome;
    if not ComParametro then
      sSQL := sSQL + '()';

    objConexao.Executar_SQL(__queryMySQL_processamento__, sSQL, 1);

  except
    on E:Exception do
    begin
      sMensagem := '[ERRO] StoredProcedure_Executar('+Nome+') - Excecao:'+E.Message+' . SQL: '+sSQL;
      objLogar.Logar(sMensagem);
      ShowMessage(sMensagem);
    end;
  end;

//  objConexao.Executar_SQL(__queryMySQL_processamento__, sSQL, 1)

end;

function TCore.EnviarEmail(Assunto: string=''; Corpo: string=''): Boolean;
var
  sHost    : string;
  suser    : string;
  sFrom    : string;
  sTo      : string;
  sAssunto : string;
  sCorpo   : string;
  sAnexo   : string;
  sAplicacao: string;

begin

  sAplicacao := ExtractFileName(Application.ExeName);
  sAplicacao := StringReplace(sAplicacao, '.exe', '', [rfReplaceAll, rfIgnoreCase]);

  sHost    := objParametrosDeEntrada.eHost;
  suser    := objParametrosDeEntrada.eUser;
  sFrom    := objParametrosDeEntrada.eFrom;
  sTo      := objParametrosDeEntrada.eTo;
  sAssunto := 'Processamento - ' + sAplicacao + ' - ' + objFuncoesWin.GetVersaoDaAplicacao() + ' [PROCESSAMENTO: ' + objParametrosDeEntrada.PEDIDO_LOTE + ']';
  sAssunto := sAssunto + ' ' + Assunto;
  sCorpo   := Corpo;

  sAnexo := objLogar.getArquivoDeLog();

  //sAnexo := StringReplace(anexo, '"', '', [rfReplaceAll, rfIgnoreCase]);
  //sAnexo := StringReplace(anexo, '''', '', [rfReplaceAll, rfIgnoreCase]);

  try

    objEmail := TSMTPDelphi.create(sHost, suser);

    if objEmail.ConectarAoServidorSMTP() then
    begin
      if objEmail.AnexarArquivo(sAnexo) then
      begin

          if not (objEmail.EnviarEmail(sFrom, sTo, sAssunto, sCorpo)) then
            ShowMessage('ERRO AO ENVIAR O E-MAIL')
          else
          if not objEmail.DesconectarDoServidorSMTP() then
            ShowMessage('ERRO AO DESCONECTAR DO SERVIDOR');
      end
      else
        ShowMessage('ERRO AO ANEXAR O ARQUIVO');
    end
    else
      ShowMessage('ERRO AO CONECTAR AO SERVIDOR');

  except
    ShowMessage('NÃO FOI POSSIVEL ENVIAR O E-MAIL.');
  end;
end;

function Tcore.PesquisarLote(LOTE_PEDIDO : STRING; status : Integer): Boolean;
var
  sComando : string;
  iPedido  : Integer;
  sStauts  : string;
begin

  case status of
    0: sStauts := 'S';
    1: sStauts := 'N';
  end;

  objParametrosDeEntrada.PEDIDO_LOTE_TMP := LOTE_PEDIDO;

  sComando := ' SELECT RELATORIO_QTD FROM  ' + objParametrosDeEntrada.TABELA_LOTES_PEDIDOS
            + ' WHERE LOTE_PEDIDO = ' + LOTE_PEDIDO + ' AND VALIDO = "' + sStauts + '"';
  objStatusProcessamento := objConexao.Executar_SQL(__queryMySQL_processamento__, sComando, 2);

  objParametrosDeEntrada.stlRelatorioQTDE.Text := __queryMySQL_processamento__.FieldByName('RELATORIO_QTD').AsString;

  if __queryMySQL_processamento__.RecordCount > 0 then
    Result := True
  else
    Result := False;

end;

function TCORE.Extrair_Arquivo_7z(Arquivo, destino : String): integer;
Var
  sComando                  : String;
  sParametros               : String;
  __AplicativoCompactacao__ : String;

  iRetorno                  : Integer;
Begin

    destino := objString.AjustaPath(destino);

    sParametros := ' e ';

    IF StrToInt(objParametrosDeEntrada.ARQUITETURA_WINDOWS) = 32 THEN
      __AplicativoCompactacao__ := objParametrosDeEntrada.app_7z_32bits;

    IF StrToInt(objParametrosDeEntrada.ARQUITETURA_WINDOWS) = 64 THEN
      __AplicativoCompactacao__ := objParametrosDeEntrada.app_7z_64bits;

    sComando := __AplicativoCompactacao__ + sParametros + ' ' + Arquivo +  ' -y -o"' + destino + '"';

    iRetorno := objFuncoesWin.WinExecAndWait32(sComando);

    Result   := iRetorno;

End;

PROCEDURE TCORE.EXTRAIR_ARQUIVO(ARQUIVO_ORIGEM, PATH_DESTINO: String);
begin

  Extrair_Arquivo_7z(ARQUIVO_ORIGEM, PATH_DESTINO);

end;

PROCEDURE TCORE.COMPACTAR_ARQUIVO(ARQUIVO_ORIGEM, PATH_DESTINO: String; MOVER_ARQUIVO: Boolean = FALSE; ZIP: Boolean=false);
begin

  Compactar_Arquivo_7z(ARQUIVO_ORIGEM, PATH_DESTINO, MOVER_ARQUIVO, ZIP);

end;

function TCORE.Compactar_Arquivo_7z(Arquivo, destino : String; mover_arquivo: Boolean=false; ZIP: Boolean=false): integer;
Var
  sComando                  : String;
  sArquivoDestino           : String;
  sParametros               : String;
  __AplicativoCompactacao__ : String;

  iRetorno                  : Integer;
Begin

  destino     := objString.AjustaPath(destino);
  sParametros := ' a ';

  if ZIP then
  begin

    IF Pos('.csv', Arquivo) > 0 THEN
      sArquivoDestino := StringReplace(ExtractFileName(Arquivo), '.csv', '', [rfReplaceAll, rfIgnoreCase]) + '.zip'
    else
    IF Pos('.txt', Arquivo) > 0 THEN
      sArquivoDestino := StringReplace(ExtractFileName(Arquivo), '.txt', '', [rfReplaceAll, rfIgnoreCase]) + '.zip'
    else
    IF Pos('.CSV', Arquivo) > 0 THEN
      sArquivoDestino := StringReplace(ExtractFileName(Arquivo), '.CSV', '', [rfReplaceAll, rfIgnoreCase]) + '.ZIP'
    else
    IF Pos('.TXT', Arquivo) > 0 THEN
      sArquivoDestino := StringReplace(ExtractFileName(Arquivo), '.TXT', '', [rfReplaceAll, rfIgnoreCase]) + '.ZIP'
    else
      sArquivoDestino := ExtractFileName(Arquivo) + '.ZIP';

    sParametros     := sParametros + ' -tzip ';

  end
  else
  BEGIN

    IF Pos('.TXT', Arquivo) > 0 THEN
      sArquivoDestino := StringReplace(ExtractFileName(Arquivo), '.TXT', '', [rfReplaceAll, rfIgnoreCase]) + '.7Z'
    ELSE
      sArquivoDestino := ExtractFileName(Arquivo) + '.7Z';

  end;

    IF StrToInt(objParametrosDeEntrada.ARQUITETURA_WINDOWS) = 32 THEN
      __AplicativoCompactacao__ := objParametrosDeEntrada.app_7z_32bits;

    IF StrToInt(objParametrosDeEntrada.ARQUITETURA_WINDOWS) = 64 THEN
      __AplicativoCompactacao__ := objParametrosDeEntrada.app_7z_64bits;

    sComando := __AplicativoCompactacao__ + sParametros + ' "' + destino + sArquivoDestino + '" "' + Arquivo + '"';

    if mover_arquivo then
      sComando := sComando + ' -sdel';

    iRetorno := objFuncoesWin.WinExecAndWait32(sComando);

    Result   := iRetorno;

End;


end.
