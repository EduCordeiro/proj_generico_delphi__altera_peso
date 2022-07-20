Unit ClassParametrosDeEntrada;

interface

  uses Classes, Dialogs, SysUtils, Forms, Controls, Graphics,
  StdCtrls, ComCtrls;

  type
    TRetorno = record
      bStatus : Boolean;
      sMSG    : string;
      iValor  : Integer;
      sValor  : String;
    end;

  type
    TParametrosDeEntrada= Class
      // Propriedades da Classe ClassParametrosDeEntrada
      HORA_INICIO_PROCESSO                       : TDateTime;
      HORA_FIM_PROCESSO                          : TDateTime;
      INFORMACAO_DOS_ARQUIVOS_SELECIONADOS       : string;

      ID_PROCESSAMENTO                           : STRING;
      LISTADEARQUIVOSDEENTRADA                   : TSTRINGS;
      PATHENTRADA                                : STRING;
      PATHSAIDA                                  : STRING;
      PATHARQUIVO_TMP                            : STRING;

      TABELA_PROCESSAMENTO                       : STRING;
      TABELA_LOTES_PEDIDOS                       : STRING;
      TABELA_PLANO_DE_TRIAGEM                    : STRING;
      CARREGAR_PLANO_DE_TRIAGEM_MEMORIA          : STRING;
      TABELA_BLOCAGEM_INTELIGENTE                : STRING;
      TABELA_BLOCAGEM_INTELIGENTE_RELATORIO      : STRING;

      TABELA_ENTRADA_SP                          : STRING;
      TABELA_AUX_SP                              : STRING;

      NUMERO_DE_IMAGENS_PARA_BLOCAGENS           : STRING;
      BLOCAGEM                                   : STRING;
      BLOCAR_ARQUIVO                             : STRING;
      MANTER_ARQUIVO_ORIGINAL                    : STRING;

      LIMITE_DE_SELECT_POR_INTERACOES_NA_MEMORIA : string;

      PEDIDO_LOTE                                : string;

      FORMATACAO_LOTE_PEDIDO                     : string;
      lista_de_caracteres_invalidos              : string;

      ENVIAR_EMAIL                               : string;

      IMAGEM_UNICA                               : string;
      IMAGEM_PG1                                 : string;
      IMAGEM_PGN                                 : string;

      IMAGEM                                     : string;
      IMAGEM_PG2                                 : string;

      IMAGEM_UNICA_FAC_REGISTRADO                : string;
      IMAGEM_PG1_FAC_REGISTRADO                  : string;
      IMAGEM_PGN_FAC_REGISTRADO                  : string;

      app_7z_32bits                              : string;
      app_7z_64bits                              : string;
      ARQUITETURA_WINDOWS                        : string;

      STIPO_LISTA_POSTAGEM                       : string;
      FAC_REGISTRADO                             : Boolean;

      EXTENCAO_ARQUIVO                           : string;

      FAC_MONITORADO                                : string;
      FAC_MONITORADO_QUANTIDADE                     : Integer;

      stlRelatorioQTDE                           : TStringList;

      stlListaPesosNew                           : TStrings;
      stlListaPesosOld                           : TStrings;

      PEDIDO_LOTE_TMP                            : string; // USADO PARA SALVAR RELATORIO

      NUMERO_DE_IMPRESSOES                       : string;
      LOCAL_ARQUIVO_LOOK                         : string;

      TESTE                                      : Boolean;

      IMPRIMIR                                   : Boolean;

      COD_CATEGORIA       : string;
      PESO_UNITARIO       : string;
      COD_DR              : string;
      DR_POSTAGEM         : string;
      CODIGO_ADM_CONTRATO : string;
      CARTAO              : string;
      LOTE                : string;
      COD_UN_POST         : string;
      CEP_UNI_POST        : string;
      N_CONTRATO          : string;
      SEQUENCIA_OBJ       : string;

      rStatus                                    : TRetorno;

      // Parâmetros para o envio de e-mail
      eHost                                    : string;
      eUser                                    : string;
      eFrom                                    : string;
      eTo                                      : string;      

    end;

  RLayoutModelo = record
    COD_CATEGORIA       : string;
    PESO_UNITARIO       : string;
    COD_DR              : string;
    CODIGO_ADM_CONTRATO : string;
    CARTAO              : string;
    LOTE                : string;
    COD_UN_POST         : string;
    CEP_UNI_POST        : string;
    N_CONTRATO          : string;
    SEQUENCIA_OBJ       : string;
  end;

implementation


End.
