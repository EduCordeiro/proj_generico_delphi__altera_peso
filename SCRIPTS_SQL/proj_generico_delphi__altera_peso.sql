CREATE DATABASE IF NOT EXISTS proj_generico_delphi__altera_peso;

DROP TABLE IF EXISTS proj_generico_delphi__altera_peso.debug_sql;
CREATE TABLE IF NOT EXISTS proj_generico_delphi__altera_peso.debug_sql (
  interacoes varchar(10) default NULL,
  inte varchar(10) default NULL,
  reg_lidos varchar(10) default NULL,
  timestamp varchar(20) default NULL
);

DROP TABLE IF EXISTS proj_generico_delphi__altera_peso.processamento;
CREATE TABLE IF NOT EXISTS proj_generico_delphi__altera_peso.processamento (
  COD_CATEGORIA       INT(1),
  PESO_UNITARIO       DOUBLE,
  COD_DR              VARCHAR(02),
  CODIGO_ADM_CONTRATO VARCHAR(08),
  CARTAO              VARCHAR(12),
  LOTE                VARCHAR(05),
  COD_UN_POST         VARCHAR(08),
  CEP_UNI_POST        VARCHAR(08),
  N_CONTRATO          VARCHAR(10),
  LINHA               TEXT,
  SEQUENCIA_OBJ       VARCHAR(11)
);

drop table if exists proj_generico_delphi__altera_peso.tbl_entrada;
create table proj_generico_delphi__altera_peso.tbl_entrada(
  seq int auto_increment,
  tipo_reg varchar(2),
  OPERADORA varchar(3),
  CONTRATO varchar(9),
  arquivo varchar(100),
  textolinha VARCHAR(959),
  PRIMARY KEY(seq)
);
/*CREATE INDEX idx_tbl_entrada ON proj_generico_delphi__altera_peso.tbl_entrada (seq, tipo_reg, OPERADORA, CONTRATO, arquivo);*/

DROP TABLE IF EXISTS proj_generico_delphi__altera_peso._AUX_;
create table proj_generico_delphi__altera_peso._aux_(
    seq int auto_increment,
    tipo_reg varchar(2),
    OPERADORA varchar(3),
    CONTRATO varchar(9),
    CEP VARCHAR(8),
    FLAG_EMAIL VARCHAR(1),
    FLAG_STATUS VARCHAR(1),
    arquivo varchar(100),
    textolinha VARCHAR(959),
    PRIMARY KEY(seq)
);
/*CREATE INDEX idx___aux__ ON proj_generico_delphi__altera_peso._AUX_ (seq, tipo_reg, OPERADORA, CONTRATO, CEP, FLAG_EMAIL, FLAG_STATUS);*/

CREATE TABLE IF NOT EXISTS proj_generico_delphi__altera_peso.LOTES_PEDIDOS (
  LOTE_PEDIDO int  NOT NULL default '-1',
  VALIDO CHAR(1) NOT NULL default 'S',
  DATA_CRIACAO date,
  RELATORIO_QTD MEDIUMBLOB,
  PRIMARY KEY  (LOTE_PEDIDO)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS proj_generico_delphi__altera_peso.tbl_blocagem;

CREATE TABLE IF NOT EXISTS proj_generico_delphi__altera_peso.tbl_blocagem(
  linha VARCHAR(5000),
  diconix integer,
  numeroDaImagem integer,
  lote integer,
  sequencia integer
) CHARACTER SET latin1 COLLATE latin1_swedish_ci;

CREATE INDEX idx_blocagem ON proj_generico_delphi__altera_peso.tbl_blocagem (Diconix, numeroDaImagem, lote, sequencia);

CREATE TABLE IF NOT EXISTS proj_generico_delphi__altera_peso.tbl_blocagemRelatorio(
  id BIGINT AUTO_INCREMENT,
  data VARCHAR(10),
  duracao VARCHAR(50),
  arquivo VARCHAR(600),
  tamanhoArquivo VARCHAR(50),
  qtdeImagensNoArquivo BIGINT,
  parQtdeImagensBlocagem BIGINT, 
  parBlocagem BIGINT, 
  saidaQtdeLotesComBlocagemPadrao BIGINT,
  saidaSobra BIGINT, 
  saidaBlocagemParaSobra BIGINT, 
  saidaQtdeImagensDesperdicadas BIGINT,
  PRIMARY KEY(id)
) CHARACTER SET latin1 COLLATE latin1_swedish_ci;
