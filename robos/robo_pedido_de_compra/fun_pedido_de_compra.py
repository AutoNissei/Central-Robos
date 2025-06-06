import pyodbc
import os
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime
import tempfile
import shutil







sql_script = '''

-----------------------------------------------------------------------------------
-- Desenvolvido por: Eduardo Nakanishi, Michael Faria e Luan Ribeiro  21/08/2024 --
-----------------------------------------------------------------------------------


BEGIN
	TRY


	BEGIN TRANSACTION

DECLARE @ENTIDADE			 NVARCHAR(MAX) = ( SELECT TOP 1 ENTIDADE
												   FROM NF_COMPRA
												  WHERE EMPRESA    = @EMPRESA
												     AND CHAVE_NFE = @CHAVE_NFE
												  ORDER BY EMISSAO DESC )
DECLARE @PEDIDO_COMPRA		   NUMERIC(15)
DECLARE @PEDIDO_COMPRA_PARCELA NUMERIC(15)
DECLARE @PEDIDO_COMPRA_PRODUTO NUMERIC(15)
DECLARE @PEDIDO_COMPRA_TOTAL   NUMERIC(15)
DECLARE @PRODUTOS_INSERT	   TABLE ( ID		  INT IDENTITY (1,1)
							         , PRODUTO	  NUMERIC(10)
							         , QUANTIDADE INT)


	SET @PEDIDO_COMPRA		   = IDENT_CURRENT('PEDIDOS_COMPRAS')		   + 1
	SET @PEDIDO_COMPRA_PRODUTO = IDENT_CURRENT('PEDIDOS_COMPRAS_PRODUTOS') + 1
	SET @PEDIDO_COMPRA_PARCELA = IDENT_CURRENT('PEDIDOS_COMPRAS_PARCELAS') + 1
	SET @PEDIDO_COMPRA_TOTAL   = IDENT_CURRENT('PEDIDOS_COMPRAS_TOTAIS')   + 1


IF EXISTS (SELECT * FROM NF_COMPRA WHERE CHAVE_NFE = @CHAVE_NFE AND PEDIDO_COMPRA IS NULL)
BEGIN


INSERT INTO @PRODUTOS_INSERT


SELECT  B.PRODUTO
	  , B.QUANTIDADE
  FROM NF_COMPRA AS A
  JOIN NF_COMPRA_PRODUTOS AS B ON A.NF_COMPRA = B.NF_COMPRA
 WHERE CHAVE_NFE = @CHAVE_NFE
   AND EMPRESA   = @EMPRESA


END
ELSE


INSERT INTO @PRODUTOS_INSERT


VALUES (579, 13)
	 , (578,15)
	 , (3123, 15)


----------------------------------
-- INSERE PEDIDO_COMPRA NA TEMPOR RIA --
----------------------------------------


IF OBJECT_ID('TEMPDB..#TEMP__PEDIDOS_COMPRAS_INSERT') IS NOT NULL
	DROP TABLE #TEMP__PEDIDOS_COMPRAS_INSERT


CREATE TABLE #TEMP__PEDIDOS_COMPRAS_INSERT ( PEDIDO_COMPRA	      NUMERIC(15)
										   , FORMULARIO_ORIGEM    NUMERIC(15)
										   , TAB_MASTER_ORIGEM    NUMERIC(15)
										   , REG_MASTER_ORIGEM    NUMERIC(15)
										   , EMPRESA		      NUMERIC(3)
										   , DATA_HORA		      DATETIME
										   , USUARIO_LOGADO	      NUMERIC(15)
										   , ENTIDADE		      NUMERIC(20)
										   , DATA_ENTREGA	      DATE
										   , PHARMALINK		      CHAR(1)
										   , PEDIDO_OL		      CHAR(1)
										   , PEDIDO_ELETRONICO    CHAR(1)
										   , SUBST_INCLUSA_PRECO  CHAR(1)
										   , PHARMALINK_CONDICAO  VARCHAR(3)
										   , COMPRADOR			  NUMERIC(9)
										   , INTEGRACAO_EXTERNA   CHAR(1)
										   , CROSSDOCKING	      CHAR(1)
										   , PROCESSAR_INTEGRACAO CHAR(1)
										   , SYNC_CD			  CHAR(1))


INSERT INTO #TEMP__PEDIDOS_COMPRAS_INSERT ( PEDIDO_COMPRA
							, FORMULARIO_ORIGEM
							, TAB_MASTER_ORIGEM
							, REG_MASTER_ORIGEM
							, EMPRESA
							, DATA_HORA
							, USUARIO_LOGADO
							, ENTIDADE
							, DATA_ENTREGA
							, PHARMALINK
							, PEDIDO_OL
							, PEDIDO_ELETRONICO
							, SUBST_INCLUSA_PRECO
							, PHARMALINK_CONDICAO
							, COMPRADOR
							, INTEGRACAO_EXTERNA
							, CROSSDOCKING
							, PROCESSAR_INTEGRACAO
							, SYNC_CD)


SELECT @PEDIDO_COMPRA			   AS PEDIDO_COMPRA
     , 104						   AS FORMULARIO_ORIGEM
     , 188						   AS TAB_MASTER_ORIGEM
     , @PEDIDO_COMPRA			   AS REG_MASTER_ORIGEM
     , @EMPRESA					   AS EMPRESA
     , CAST(GETDATE() AS DATETIME) AS DATA_HORA
     , 1						   AS USUARIO_LOGADO
     , @ENTIDADE				   AS ENTIDADE
     , CAST(GETDATE() AS DATE)	   AS DATA_ENTREGA
     , 'N'						   AS PHARMALINK
     , 'N'						   AS PEDIDO_OL
     , 'N'						   AS PEDIDO_ELETRONICO
     , 'N'						   AS SUBST_INCLUSA_PRECO
     , 2						   AS PHARMALINK_CONDICAO
     , 0						   AS COMPRADOR
     , 'N'						   AS INTEGRACAO_EXTERNA
     , 'N'						   AS CROSSDOCKING
     , 'S'						   AS PROCESSAR_INTEGRACAO
     , 'S'						   AS SYNC_CD


------------------------------------------------
-- INSERE PEDIDO_COMPRA_PRODUTO NA TEMPOR RIA --
------------------------------------------------


IF OBJECT_ID('TEMPDB..#TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT') IS NOT NULL
	DROP TABLE #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT


CREATE TABLE #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT ( PEDIDO_COMPRA_PRODUTO		  NUMERIC(15)
													, FORMULARIO_ORIGEM			  NUMERIC(15)
													, TAB_MASTER_ORIGEM			  NUMERIC(15)
													, REG_MASTER_ORIGEM			  NUMERIC(15)
													, PEDIDO_COMPRA				  NUMERIC(15)
													, PRODUTO					  NUMERIC(10)
													, QUANTIDADE				  INT
													, UNIDADE_MEDIDA			  VARCHAR(10)
													, OPERACAO_FISCAL			  NUMERIC(5)
													, VALOR_UNITARIO			  FLOAT
													, TIPO_DESCONTO				  NUMERIC(5)
													, DESCONTO					  NUMERIC(5)
													, TOTAL_DESCONTO			  NUMERIC(9)
													, TOTAL_PRODUTO				  FLOAT
													, QUANTIDADE_EMBALAGEM		  NUMERIC(5)
													, IPI_ALIQUOTA				  NUMERIC(5)
													, IPI_VALOR					  FLOAT
													, IPI_CREDITO				  VARCHAR(1)
													, CLASSIF_FISCAL_CODIGO		  VARCHAR(13)
													, IPI_ICMS					  VARCHAR(1)
													, SITUACAO_TRIBUTARIA		  VARCHAR(3)
													, ICMS_REDUCAO_BASE			  NUMERIC(9)
													, ICMS_ALIQUOTA				  NUMERIC(9)
													, ICMS_VALOR				  NUMERIC(9)
													, ICMS_CREDITO				  VARCHAR(1)
													, OBJETO_CONTROLE			  NUMERIC(9)
													, PRECO_FABRICA				  FLOAT
													, PRECO_MAXIMO				  FLOAT
													, PRECO_VENDA				  FLOAT
													, QUANTIDADE_ESTOQUE		  NUMERIC(9)
													, ICMS_SUBST_VALOR			  NUMERIC(9)
													, ICMS_ALIQUOTA_DECRETO_35346 NUMERIC(9)
													, ICMS_VALOR_DECRETO_35346	  NUMERIC(9)
													, ICMS_ST_VALOR				  NUMERIC(9)
													, PERCENTUAL_REPASSE		  NUMERIC(9)
													, VALOR_REPASSE				  NUMERIC(9)
													, REGIME_FISCAL				  VARCHAR(1)
													, SYNC_CD					  VARCHAR(1))


INSERT INTO #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT ( PEDIDO_COMPRA_PRODUTO
												   , FORMULARIO_ORIGEM
												   , TAB_MASTER_ORIGEM
												   , REG_MASTER_ORIGEM
												   , PEDIDO_COMPRA
												   , PRODUTO
												   , QUANTIDADE
												   , UNIDADE_MEDIDA
												   , OPERACAO_FISCAL
												   , VALOR_UNITARIO
												   , TIPO_DESCONTO
												   , DESCONTO
												   , TOTAL_DESCONTO
												   , TOTAL_PRODUTO
												   , QUANTIDADE_EMBALAGEM
												   , IPI_ALIQUOTA
												   , IPI_VALOR
												   , IPI_CREDITO
												   , CLASSIF_FISCAL_CODIGO
												   , IPI_ICMS
												   , SITUACAO_TRIBUTARIA
												   , ICMS_REDUCAO_BASE
												   , ICMS_ALIQUOTA
												   , ICMS_VALOR
												   , ICMS_CREDITO
												   , OBJETO_CONTROLE
												   , PRECO_FABRICA
												   , PRECO_MAXIMO
												   , PRECO_VENDA
												   , QUANTIDADE_ESTOQUE
												   , ICMS_SUBST_VALOR
												   , ICMS_ALIQUOTA_DECRETO_35346
												   , ICMS_VALOR_DECRETO_35346
												   , ICMS_ST_VALOR
												   , PERCENTUAL_REPASSE
												   , VALOR_REPASSE
												   , REGIME_FISCAL
												   , SYNC_CD)


SELECT @PEDIDO_COMPRA_PRODUTO												  AS PEDIDO_COMPRA_PRODUTO
	 , 104																	  AS FORMULARIO_ORIGEM
	 , 188																	  AS TAB_MASTER_ORIGEM
	 , @PEDIDO_COMPRA														  AS REG_MASTER_ORIGEM
	 , @PEDIDO_COMPRA														  AS PEDIDO_COMPRA
	 , E.PRODUTO
     , E.QUANTIDADE
     , ''																	  AS UNIDADE_MEDIDA
     , B.OPERACAO_FISCAL
     , DBO.MAIOR_ZERO(ISNULL(A.VALOR_UNITARIO,0), ISNULL(C.PRECO_FABRICA,0))  AS VALOR_UNITARIO
     , 3																	  AS TIPO_DESCONTO
     , ISNULL(A.PERCENTUAL_DESCONTO,0)										  AS DESCONTO
     , 0																      AS TOTAL_DESCONTO
     , 0																      AS TOTAL_PRODUTO
     , CASE WHEN ( @ENTIDADE = D.FABRICANTE) OR ( A.ENTIDADE IS NOT NULL AND A.TIPO_FORNECEDOR IN(2) )
            THEN ISNULL(A.QUANTIDADE_EMBALAGEM, D.EMBALAGEM_INDUSTRIA)
            ELSE D.FATOR_EMBALAGEM
        END QUANTIDADE_EMBALAGEM
     , A.ALIQUOTA_IPI
     , 0																	  AS ICMS_VALOR
     , 'N'																	  AS ICMS_CREDITO
     , ISNULL( D.NCM, C.CLASSIF_FISCAL_CODIGO)								  AS CLASSIF_FISCAL_CODIGO
     , 'N'																	  AS IPI_ICMS
     , ISNULL(A.SITUACAO_TRIBUTARIA,'060')									  AS SITUACAO_TRIBUTARIA
     , 0																      AS ICMS_REDUCAO_BASE
     , CASE WHEN A.IVA > 0
	         THEN ISNULL(A.ICMS_SUBSTITUTO, ISNULL(A.ALIQUOTA_ICMS,0))
	         ELSE CASE WHEN C.IVA > 0
	                   THEN ISNULL(C.ICMS_SUBSTITUTO,0)
	                   ELSE ISNULL(C.ALIQUOTA_ICMS,0)
	              END
	    END																	  AS ICMS_ALIQUOTA
     , 0																      AS ICMS_VALOR
     , CASE WHEN ( C.IVA > 0  OR C.ALIQUOTA_ICMS = 0 )
            THEN 'N'
            ELSE 'S'
        END	ICMS_CREDITO
     , B.OBJETO_CONTROLE_COMPRAS											  AS OBJETO_CONTROLE
     , C.PRECO_FABRICA														  AS PRECO_FABRICA
     , 0																      AS PRECO_MAXIMO
     , 0																      AS PRECO_VENDA
     , 0																	  AS QUANTIDADE_ESTOQUE
     , 0																	  AS ICMS_SUBST_VALOR
     , 0																	  AS ICMS_ALIQUOTA_DECRETO_35346
     , 0																	  AS ICMS_VALOR_DECRETO_35346
     , 0																	  AS ICMS_ST_VALOR
     , 0																	  AS PERCENTUAL_REPASSE
     , 0																	  AS VALOR_REPASSE
     , 'N'																	  AS REGIME_FISCAL
     , 'S'																	  AS SYNC_CD

  FROM @PRODUTOS_INSERT				AS E
  LEFT
  JOIN PRODUTOS_FORNECEDORES		AS A WITH (NOLOCK) ON A.ENTIDADE = @ENTIDADE
								                      AND A.PRODUTO  = E.PRODUTO
  LEFT
  JOIN PARAMETROS_COMPRAS           AS B WITH (NOLOCK) ON B.EMPRESA_USUARIA = @EMPRESA

  LEFT
  JOIN PRODUTOS_PARAMETROS_EMPRESAS AS C WITH (NOLOCK) ON C.PRODUTO = E.PRODUTO
													  AND C.EMPRESA = B.EMPRESA_USUARIA
  JOIN PRODUTOS						AS D WITH (NOLOCK) ON D.PRODUTO = E.PRODUTO

 WHERE B.EMPRESA_USUARIA = @EMPRESA


----------------------------------------------------------------
-- REALIZA OS C LCULOS NAS COLUNAS TOTAL_DESCONTO E IPI_VALOR --
----------------------------------------------------------------


 UPDATE A SET TOTAL_DESCONTO = (B.QUANTIDADE * VALOR_UNITARIO) * (DESCONTO / 100)
	  , TOTAL_PRODUTO  = (VALOR_UNITARIO * B.QUANTIDADE)
  FROM #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT AS A
  JOIN @PRODUTOS_INSERT						  AS B ON A.PRODUTO = B.PRODUTO
											      AND A.QUANTIDADE = B.QUANTIDADE
 WHERE A.PEDIDO_COMPRA		   = @PEDIDO_COMPRA
   AND A.PEDIDO_COMPRA_PRODUTO = @PEDIDO_COMPRA_PRODUTO


UPDATE A SET IPI_VALOR = TOTAL_PRODUTO * (IPI_ALIQUOTA / 100)
  FROM #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT AS A
  JOIN @PRODUTOS_INSERT						  AS B ON A.PRODUTO = B.PRODUTO
												  AND A.QUANTIDADE = B.QUANTIDADE
 WHERE A.PEDIDO_COMPRA	       = @PEDIDO_COMPRA
   AND A.PEDIDO_COMPRA_PRODUTO = @PEDIDO_COMPRA_PRODUTO


UPDATE A SET QUANTIDADE_ESTOQUE = (A.QUANTIDADE * A.QUANTIDADE_EMBALAGEM)
  FROM #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT AS A
  JOIN @PRODUTOS_INSERT						  AS B ON A.PRODUTO = B.PRODUTO
												  AND A.QUANTIDADE = B.QUANTIDADE


UPDATE A SET UNIDADE_MEDIDA = CASE WHEN A.QUANTIDADE_EMBALAGEM > 1
								    THEN 'CX'
								    ELSE 'UN'
							   END
  FROM #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT AS A
   JOIN @PRODUTOS_INSERT						  AS B ON A.PRODUTO = B.PRODUTO
												      AND A.QUANTIDADE = B.QUANTIDADE


-------------------------------------------------
-- INSERE PEDIDO_COMPRA_PARCELAS NA TEMPOR RIA --
-------------------------------------------------


IF OBJECT_ID('TEMPDB..#TEMP__PEDIDOS_COMPRAS_PARCELAS_INSERT') IS NOT NULL
	DROP TABLE #TEMP__PEDIDOS_COMPRAS_PARCELAS_INSERT


CREATE TABLE #TEMP__PEDIDOS_COMPRAS_PARCELAS_INSERT ( PEDIDO_COMPRA_PARCELA NUMERIC(9)
													, FORMULARIO_ORIGEM		NUMERIC(9)
													, TAB_MASTER_ORIGEM		NUMERIC(9)
													, REG_MASTER_ORIGEM		NUMERIC(9)
													, PEDIDO_COMPRA			NUMERIC(9)
													, DIAS					NUMERIC(5)
													, PERCENTUAL			NUMERIC(5)
													, VENCIMENTO			DATETIME
													, GERAR_ANTECIPACAO		VARCHAR(1)
													, VALOR					FLOAT
													, PARCELA				NUMERIC(5)
													, VALOR_MOEDA			FLOAT )


INSERT INTO #TEMP__PEDIDOS_COMPRAS_PARCELAS_INSERT ( PEDIDO_COMPRA_PARCELA
												   , FORMULARIO_ORIGEM
												   , TAB_MASTER_ORIGEM
												   , REG_MASTER_ORIGEM
												   , PEDIDO_COMPRA
												   , DIAS
												   , PERCENTUAL
												   , VENCIMENTO
												   , GERAR_ANTECIPACAO
												   , VALOR
												   , PARCELA
												   , VALOR_MOEDA)


SELECT @PEDIDO_COMPRA_PARCELA			AS PEDIDO_COMPRA_PARCELA
	 , 104								AS FORMULARIO_ORIGEM
	 , 188								AS TAB_MASTER_ORIGEM
	 , @PEDIDO_COMPRA					AS REG_MASTER_ORIGEM
	 , @PEDIDO_COMPRA					AS PEDIDO_COMPRA
	 , 60								AS DIAS
	 , 100								AS PERCENTUAL
	 , DATEADD(DAY, 60, A.DATA_ENTREGA) AS VENCIMENTO
	 , 'N'								AS GERAR_ANTECIPACAO
	 , 0							    AS VALOR
	 , 1							    AS PARCELA
	 , 0							    AS VALOR_MOEDA
  FROM #TEMP__PEDIDOS_COMPRAS_INSERT	AS A
 WHERE PEDIDO_COMPRA = @PEDIDO_COMPRA


---------------------------------------------
-- INSERE PEDIDO_COMPRA_TOTAIS NA TEMPOR RIA
---------------------------------------------


IF OBJECT_ID('TEMPDB..#TEMP__PEDIDOS_COMPRAS_TOTAIS_INSERT') IS NOT NULL
	DROP TABLE #TEMP__PEDIDOS_COMPRAS_TOTAIS_INSERT


CREATE TABLE #TEMP__PEDIDOS_COMPRAS_TOTAIS_INSERT( PEDIDO_COMPRA_TOTAL   NUMERIC(9)
												 , FORMULARIO_ORIGEM     NUMERIC(9)
												 , TAB_MASTER_ORIGEM     NUMERIC(9)
												 , REG_MASTER_ORIGEM     NUMERIC(9)
												 , PEDIDO_COMPRA         NUMERIC(9)
												 , TOTAL_PRODUTOS	     FLOAT
												 , TOTAL_IPI		     FLOAT
												 , SUB_TOTAL		     FLOAT
												 , TOTAL_SERVICOS	     FLOAT
												 , TOTAL_GERAL		     FLOAT
												 , ICMS_BASE_CALCULO     NUMERIC(9)
												 , ICMS_VALOR		     NUMERIC(9)
												 , ICMS_BASE_SUBST	     NUMERIC(9)
												 , ICMS_VALOR_SUBST	     NUMERIC(9)
												 , TOTAL_DESC_FINANCEIRO NUMERIC(9)
												 , TOTAL_REPASSE		 FLOAT
												 , TOTAL_DESPESAS		 FLOAT
												 , TOTAL_FRETE			 FLOAT
												 , TOTAL_SEGURO			 FLOAT
												 , DESCONTO_FINANCEIRO   NUMERIC(5)
												 , REPASSE				 NUMERIC(5)
												 , TOTAL_SUBSTITUICAO    FLOAT
												 , DESCONTO_NEGOCIADO    NUMERIC(6) )


INSERT INTO #TEMP__PEDIDOS_COMPRAS_TOTAIS_INSERT ( PEDIDO_COMPRA_TOTAL
												 , FORMULARIO_ORIGEM
												 , TAB_MASTER_ORIGEM
												 , REG_MASTER_ORIGEM
												 , PEDIDO_COMPRA
												 , TOTAL_PRODUTOS
												 , TOTAL_IPI
												 , SUB_TOTAL
												 , TOTAL_SERVICOS
												 , TOTAL_GERAL
												 , ICMS_BASE_CALCULO
												 , ICMS_VALOR
												 , ICMS_BASE_SUBST
												 , ICMS_VALOR_SUBST
												 , TOTAL_DESC_FINANCEIRO
												 , TOTAL_REPASSE
												 , TOTAL_DESPESAS
												 , TOTAL_FRETE
												 , TOTAL_SEGURO
												 , DESCONTO_FINANCEIRO
												 , REPASSE
												 , TOTAL_SUBSTITUICAO
												 , DESCONTO_NEGOCIADO )


SELECT @PEDIDO_COMPRA_TOTAL									   AS PEDIDO_COMPRA_TOTAL
	 , 104													   AS FORMULARIO_ORIGEM
	 , 188													   AS TAB_MASTER_ORIGEM
	 , @PEDIDO_COMPRA										   AS REG_MASTER_ORIGEM
	 , @PEDIDO_COMPRA										   AS PEDIDO_COMPRA
	 , SUM(B.TOTAL_PRODUTO)									   AS TOTAL_PRODUTO
	 , SUM(B.IPI_VALOR)										   AS IPI_VALOR
	 , (SUM(B.TOTAL_PRODUTO) + SUM(B.IPI_VALOR))			   AS SUB_TOTAL
	 , 0													   AS TOTAL_SERVICOS
	 , ((SUM(B.TOTAL_PRODUTO) + SUM(B.IPI_VALOR)) + 0 - 0 - 0) AS TOTAL_GERAL
	 , 0													   AS ICMS_BASE_CALCULO
	 , SUM(B.ICMS_VALOR)									   AS ICMS_VALOR
	 , 0													   AS ICMS_BASE_SUBST
	 , 0													   AS ICMS_VALOR_SUBST
	 , 0													   AS TOTAL_DESC_FINANCEIRO
	 , 0													   AS TOTAL_REPASSE
	 , 0													   AS TOTAL_DESPESAS
	 , 0													   AS TOTAL_FRETE
	 , 0													   AS TOTAL_SEGURO
	 , 0													   AS DESCONTO_FINANCEIRO
	 , 0													   AS REPASSE
	 , 0													   AS TOTAL_SUBSTITUICAO
	 , 0													   AS DESCONTO_NEGOCIADO
  FROM #TEMP__PEDIDOS_COMPRAS_INSERT		  AS A
  JOIN #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT AS B ON A.PEDIDO_COMPRA = B.PEDIDO_COMPRA
 WHERE A.PEDIDO_COMPRA = @PEDIDO_COMPRA


-------------------------------------------------------------
-- ATUALIZA O C LCULO DO VALOR NA PEDIDOS_COMPRAS_PARCELAS --
-------------------------------------------------------------


UPDATE C SET C.VALOR = B.TOTAL_GERAL
  FROM #TEMP__PEDIDOS_COMPRAS_INSERT		  AS A
  JOIN #TEMP__PEDIDOS_COMPRAS_TOTAIS_INSERT   AS B ON A.PEDIDO_COMPRA = B.PEDIDO_COMPRA
  JOIN #TEMP__PEDIDOS_COMPRAS_PARCELAS_INSERT AS C ON A.PEDIDO_COMPRA = C.PEDIDO_COMPRA
 WHERE A.PEDIDO_COMPRA = @PEDIDO_COMPRA


-------------------------------------------------------------
-- IN CIO INSER  O NA TABELA DE PEDIDOS_COMPRAS E DETAIL's --
-------------------------------------------------------------


INSERT INTO PEDIDOS_COMPRAS ( FORMULARIO_ORIGEM
							, TAB_MASTER_ORIGEM
							, REG_MASTER_ORIGEM
							, EMPRESA
							, DATA_HORA
							, USUARIO_LOGADO
							, ENTIDADE
							, DATA_ENTREGA
							, PHARMALINK
							, PEDIDO_OL
							, PEDIDO_ELETRONICO
							, SUBST_INCLUSA_PRECO
							, PHARMALINK_CONDICAO
							, COMPRADOR
							, INTEGRACAO_EXTERNA
							, CROSSDOCKING
							, PROCESSAR_INTEGRACAO
							, SYNC_CD )


SELECT FORMULARIO_ORIGEM
     , TAB_MASTER_ORIGEM
     , REG_MASTER_ORIGEM
     , EMPRESA
     , DATA_HORA
     , USUARIO_LOGADO
     , ENTIDADE
     , DATA_ENTREGA
     , PHARMALINK
     , PEDIDO_OL
     , PEDIDO_ELETRONICO
     , SUBST_INCLUSA_PRECO
     , PHARMALINK_CONDICAO
     , COMPRADOR
     , INTEGRACAO_EXTERNA
     , CROSSDOCKING
     , PROCESSAR_INTEGRACAO
     , SYNC_CD
  FROM #TEMP__PEDIDOS_COMPRAS_INSERT


INSERT INTO PEDIDOS_COMPRAS_PRODUTOS ( FORMULARIO_ORIGEM
									 , TAB_MASTER_ORIGEM
									 , REG_MASTER_ORIGEM
									 , PEDIDO_COMPRA
									 , PRODUTO
									 , QUANTIDADE
									 , UNIDADE_MEDIDA
									 , OPERACAO_FISCAL
									 , VALOR_UNITARIO
									 , TIPO_DESCONTO
									 , DESCONTO
									 , TOTAL_DESCONTO
									 , TOTAL_PRODUTO
									 , QUANTIDADE_EMBALAGEM
									 , IPI_ALIQUOTA
									 , IPI_VALOR
									 , IPI_CREDITO
									 , CLASSIF_FISCAL_CODIGO
									 , IPI_ICMS
									 , SITUACAO_TRIBUTARIA
									 , ICMS_REDUCAO_BASE
									 , ICMS_ALIQUOTA
									 , ICMS_VALOR
									 , ICMS_CREDITO
									 , OBJETO_CONTROLE
									 , PRECO_FABRICA
									 , PRECO_MAXIMO
									 , PRECO_VENDA
									 , QUANTIDADE_ESTOQUE
									 , ICMS_SUBST_VALOR
									 , ICMS_ALIQUOTA_DECRETO_35346
									 , ICMS_VALOR_DECRETO_35346
									 , ICMS_ST_VALOR
									 , PERCENTUAL_REPASSE
									 , VALOR_REPASSE
									 , REGIME_FISCAL
									 , SYNC_CD )


SELECT FORMULARIO_ORIGEM
     , TAB_MASTER_ORIGEM
     , REG_MASTER_ORIGEM
     , PEDIDO_COMPRA
     , PRODUTO
     , QUANTIDADE
     , UNIDADE_MEDIDA
     , OPERACAO_FISCAL
     , VALOR_UNITARIO
     , TIPO_DESCONTO
     , DESCONTO
     , TOTAL_DESCONTO
     , TOTAL_PRODUTO
     , QUANTIDADE_EMBALAGEM
     , ISNULL(IPI_ALIQUOTA,0) AS IPI_ALIQUOTA
     , ISNULL(IPI_VALOR,0) AS IPI_VALOR
     , ISNULL(IPI_CREDITO,0) AS IPI_CREDITO
     , CLASSIF_FISCAL_CODIGO
     , IPI_ICMS
     , SITUACAO_TRIBUTARIA
     , ICMS_REDUCAO_BASE
     , ICMS_ALIQUOTA
     , ICMS_VALOR
     , ICMS_CREDITO
     , OBJETO_CONTROLE
     , PRECO_FABRICA
     , PRECO_MAXIMO
     , PRECO_VENDA
     , QUANTIDADE_ESTOQUE
     , ICMS_SUBST_VALOR
     , ICMS_ALIQUOTA_DECRETO_35346
     , ICMS_VALOR_DECRETO_35346
     , ICMS_ST_VALOR
     , PERCENTUAL_REPASSE
     , VALOR_REPASSE
     , REGIME_FISCAL
     , SYNC_CD
  FROM #TEMP__PEDIDOS_COMPRAS_PRODUTOS_INSERT


INSERT INTO PEDIDOS_COMPRAS_PARCELAS ( FORMULARIO_ORIGEM
									 , TAB_MASTER_ORIGEM
									 , REG_MASTER_ORIGEM
									 , PEDIDO_COMPRA
									 , DIAS
									 , PERCENTUAL
									 , VENCIMENTO
									 , GERAR_ANTECIPACAO
									 , VALOR
									 , PARCELA
									 , VALOR_MOEDA )


SELECT FORMULARIO_ORIGEM
     , TAB_MASTER_ORIGEM
     , REG_MASTER_ORIGEM
     , PEDIDO_COMPRA
     , DIAS
     , PERCENTUAL
     , VENCIMENTO
     , GERAR_ANTECIPACAO
     , VALOR
     , PARCELA
     , VALOR_MOEDA
FROM #TEMP__PEDIDOS_COMPRAS_PARCELAS_INSERT
WHERE PEDIDO_COMPRA = @PEDIDO_COMPRA


INSERT INTO PEDIDOS_COMPRAS_TOTAIS ( FORMULARIO_ORIGEM
								   , TAB_MASTER_ORIGEM
								   , REG_MASTER_ORIGEM
								   , PEDIDO_COMPRA
								   , TOTAL_PRODUTOS
								   , TOTAL_IPI
								   , SUB_TOTAL
								   , TOTAL_SERVICOS
								   , TOTAL_GERAL
								   , ICMS_BASE_CALCULO
								   , ICMS_VALOR
								   , ICMS_BASE_SUBST
								   , ICMS_VALOR_SUBST
								   , TOTAL_DESC_FINANCEIRO
								   , TOTAL_REPASSE
								   , TOTAL_DESPESAS
								   , TOTAL_FRETE
								   , TOTAL_SEGURO
								   , DESCONTO_FINANCEIRO
								   , REPASSE
								   , TOTAL_SUBSTITUICAO
								   , DESCONTO_NEGOCIADO )


SELECT FORMULARIO_ORIGEM
     , TAB_MASTER_ORIGEM
     , REG_MASTER_ORIGEM
     , PEDIDO_COMPRA
     , TOTAL_PRODUTOS
     , ISNULL(TOTAL_IPI,0) AS TOTAL_IPI
     , ISNULL(SUB_TOTAL,0) AS SUB_TOTAL
     , TOTAL_SERVICOS
     , ISNULL(TOTAL_GERAL,0) AS TOTAL_GERAL
     , ICMS_BASE_CALCULO
     , ICMS_VALOR
     , ICMS_BASE_SUBST
     , ICMS_VALOR_SUBST
     , TOTAL_DESC_FINANCEIRO
     , TOTAL_REPASSE
     , TOTAL_DESPESAS
     , TOTAL_FRETE
     , TOTAL_SEGURO
     , DESCONTO_FINANCEIRO
     , REPASSE
     , TOTAL_SUBSTITUICAO
     , DESCONTO_NEGOCIADO
  FROM #TEMP__PEDIDOS_COMPRAS_TOTAIS_INSERT
 WHERE PEDIDO_COMPRA = @PEDIDO_COMPRA


----------------------------------------------------------
-- FIM INSER  O NA TABELA DE PEDIDOS_COMPRAS E DETAIL's --
----------------------------------------------------------


---------------------------------
-- REPROCESSA A GRAVA  O FINAL --
---------------------------------


EXEC USP_PROCFIT_TABELA_EXEC_TRANSACOES 'PEDIDOS_COMPRAS', @PEDIDO_COMPRA


-------------------------------------------
-- ATUALIZA O PEDIDO_COMPRA NA NF_COMPRA --
-------------------------------------------


UPDATE A SET PEDIDO_COMPRA = @PEDIDO_COMPRA
  FROM NF_COMPRA AS A
 WHERE CHAVE_NFE = @CHAVE_NFE
   AND EMPRESA   = @EMPRESA
   AND PEDIDO_COMPRA IS NULL


   COMMIT TRANSACTION


END
	TRY


BEGIN
	CATCH


IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION


			RAISERROR('A gera  o do Pedido de Compra n o foi conclu da, por favor verique os par metros',15, -1)


	RETURN


END
	CATCH

------------------
-- SELECT FINAL --
------------------


SELECT A.PEDIDO_COMPRA
  FROM PEDIDOS_COMPRAS			AS A
 WHERE A.PEDIDO_COMPRA = @PEDIDO_COMPRA'''















# === CONFIGURAÇÃO DO LOG ===
def get_logger(nome_robo: str):
    log_dir = os.path.join("logs", f"logs_{nome_robo}")
    os.makedirs(log_dir, exist_ok=True)

    log_filename = datetime.now().strftime("log_%Y-%m-%d.txt")
    log_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger(f"logger_{nome_robo}")  # Usa nome único
    logger.setLevel(logging.INFO)

    # Remove handlers antigos se existirem
    if logger.hasHandlers():
        logger.handlers.clear()

    # Cria novo FileHandler e StreamHandler
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s - %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger.info



log = get_logger("pedidos_de_compra")
load_dotenv()


central_db_config = {
    "server": os.getenv("CENTRAL_DB_SERVER"),
    "database": os.getenv("CENTRAL_DB_DATABASE"),
    "username": os.getenv("CENTRAL_DB_USER"),
    "password": os.getenv("CENTRAL_DB_PASS")
}

awayson_db_config = {
    "server": os.getenv("AWAYSON_DB_SERVER"),
    "database": os.getenv("AWAYSON_DB_DATABASE"),
    "username": os.getenv("AWAYSON_DB_USER"),
    "password": os.getenv("AWAYSON_DB_PASS")
}


def obter_ip_filial(filial):
    if 1 <= filial <= 200 or filial == 241:
        ip = f"10.16.{filial}.24"
    elif 201 <= filial <= 299:
        ip = f"10.17.{filial % 100}.24"
    elif 300 <= filial <= 399:
        ip = f"10.17.1{filial % 100}.24"
    elif 400 <= filial <= 499:
        ip = f"10.18.{filial % 100}.24"
    elif filial == 247:
        ip = f"192.168.201.1"
    else:
        raise ValueError("Número de filial inválido.")

    filial_db_config = {
        "server": ip,
        "database": os.getenv("FILIAL_DB_DATABASE"),
        "username": os.getenv("FILIAL_DB_USER"),
        "password": os.getenv("FILIAL_DB_PASS")
    }

    return filial_db_config


def conectar_filial(num_filial):
    """Estabelece conexão com o banco de dados central e retorna a conexão."""

    config_bd_filial = obter_ip_filial(num_filial)
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={config_bd_filial['server']};"
            f"DATABASE={config_bd_filial['database']};"
            f"UID={config_bd_filial['username']};"
            f"PWD={config_bd_filial['password']}"
        )
        return conn
    except Exception as e:
        log(f"Erro ao conectar ao banco da filial: {e}")
        return None



def conectar_awayson():
    """Estabelece conexão com o banco de dados awayson e retorna a conexão."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={awayson_db_config['server']};"
            f"DATABASE={awayson_db_config['database']};"
            f"UID={awayson_db_config['username']};"
            f"PWD={awayson_db_config['password']}"
        )
        return conn

    except Exception as e:
        log(f"Erro ao conectar ao banco awayson: {e}")
        return None

def conectar_central():
    """Estabelece conexão com o banco de dados central e retorna a conexão."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={central_db_config['server']};"
            f"DATABASE={central_db_config['database']};"
            f"UID={central_db_config['username']};"
            f"PWD={central_db_config['password']}"
        )
        return conn

    except Exception as e:
        log(f"Erro ao conectar ao banco central: {e}")
        return None




def ler_arquivo(nome_arquivo):
    """Lê um arquivo linha por linha e retorna uma lista de strings sem quebras de linha."""
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, "r") as arquivo:
            return [linha.strip() for linha in arquivo.readlines() if linha.strip()]
    return []


def is_pepsico(entidade):
    entidades_pepsico = {
        '10948', '6496', '10946', '10944', '13316702', '12869', '10964',
        '6563', '10947', '13316560', '10945', '8683', '14851366',
        '12246', '15016527', '13346787'
    }
    return str(entidade) in entidades_pepsico



def gerar_pedido_pepsico(chave_nfe, empresa):
    try:


        # Prefixo com as declarações das variáveis
        prefixo = f"""
        DECLARE @EMPRESA NUMERIC(10) = {empresa}
        DECLARE @CHAVE_NFE VARCHAR(50) = '{chave_nfe}'
        """

        # Junta as variáveis com o script real
        sql_completo = prefixo + "\n" + sql_script

        conn_central = conectar_central()
        cursor = conn_central.cursor()

        # Executa o script de geração de pedido
        cursor.execute(sql_completo)
        conn_central.commit()

        log(f"Pedido gerado com sucesso para a nota {chave_nfe}.")

        # Consulta o pedido gerado
        consulta = """
        SELECT PEDIDO_COMPRA 
        FROM NF_COMPRA 
        WHERE CHAVE_NFE = ?
          AND EMPRESA = ?
        """
        cursor.execute(consulta, (chave_nfe, empresa))
        resultado = cursor.fetchone()

        if resultado and resultado[0]:
            log(f"Pedido de compra gerado: {resultado[0]}")
        else:
            log("Nenhum pedido de compra foi associado à nota.")

        cursor.close()

    except Exception as e:
        log(f"Erro ao gerar ou consultar pedido para a nota {chave_nfe}: {e}")







def consultar_pedidos_notas(num_filial, chaves, empresa):
    notas_com_pedido = []
    notas_nao_central = []
    notas_sem_pedido = []
    notas_nao_loja = []
    notas_gerado_pedido = []

    try:
        conn = conectar_awayson()
        if conn is None:
            return notas_com_pedido, notas_sem_pedido, notas_nao_central, notas_nao_loja

        cursor = conn.cursor()

        for chave in chaves:
            cursor.execute('''SELECT A.NF_COMPRA, A.PEDIDO_COMPRA, B.NOME, A.ENTIDADE, A.EMPRESA
                              FROM NF_COMPRA AS A
                              JOIN ENTIDADES AS B ON A.ENTIDADE = B.ENTIDADE
                              WHERE CHAVE_NFE = ?''', (chave,))
            resultado = cursor.fetchone()

            if resultado:
                nf_compra, pedido_compra, nome, entidade, empresa_nota = resultado
                nota_info = {"CHAVE": chave, "ENTIDADE": str(entidade), "NOME": nome, "EMPRESA": empresa_nota}

                if pedido_compra is None:
                    # Geração do pedido para notas PEPSICO sem pedido


                    gerar_pedido_pepsico(chave, empresa)
                    # Após tentar gerar, vamos checar de novo se gerou
                    cursor.execute("SELECT PEDIDO_COMPRA FROM NF_COMPRA WHERE CHAVE_NFE = ?", (chave,))
                    pedido_check = cursor.fetchone()
                    if pedido_check and pedido_check[0]:
                        notas_gerado_pedido.append(nota_info)
                    else:
                        notas_sem_pedido.append(nota_info)
                else:
                    notas_com_pedido.append(nota_info)
            else:
                notas_nao_central.append(chave)

        cursor.close()
        conn.close()

    except Exception as e:
        log(f"Erro ao consultar notas na central: {e}")
        return notas_com_pedido, notas_sem_pedido, notas_nao_central, notas_nao_loja

    # Verifica se essas notas com pedido também estão na loja
    try:
        conn_filial = conectar_filial(num_filial)
        if conn_filial is None:
            return notas_com_pedido, notas_sem_pedido, notas_nao_central, notas_nao_loja

        cursor = conn_filial.cursor()

        for nota in notas_com_pedido:
            cursor.execute("SELECT NF_COMPRA FROM NF_COMPRA WHERE CHAVE_NFE = ?", (nota["CHAVE"],))
            resultado = cursor.fetchone()

            if not resultado:
                notas_nao_loja.append(nota)

        cursor.close()
        conn_filial.close()

    except Exception as e:
        log(f"Erro ao consultar notas na filial: {e}")

    return notas_com_pedido, notas_sem_pedido, notas_nao_central, notas_nao_loja, notas_gerado_pedido






def interagir_chamado(cod_chamado, token, notas_com_pedido, notas_sem_pedido, notas_nao_encontradas, notas_nao_loja, notas_gerado_pedido):
    notas_com_pedido = notas_com_pedido or []
    notas_sem_pedido = notas_sem_pedido or []
    notas_nao_encontradas = notas_nao_encontradas or []
    notas_nao_loja = notas_nao_loja or []
    notas_gerado_pedido = notas_gerado_pedido or []
    # Criando a descrição formatada
    descricao = "Resumo da Validação das notas\n\n"

    # Conjunto de entidades da PEPSICO
    entidades_pepsico = {
        '5362', '2027', '5660', '2025', '2026', '3436', '5051', '5316', '5902',
        '6550', '7192', '7269', '7706', '7840', '7842', '8791', '11062', '12509',
        '13220', '18691', '4564598', '8497914', '8524792', '8535855', '8580695',
        '12293770', '12790279', '13364893', '13366195', '13367260', '14515766',
        '14515817', '2945'
    }

    # Verifica se existe nota da PEPSICO em qualquer das listas
    existe_nota_pepsico = any(
        nota.get("ENTIDADE") in entidades_pepsico
        for nota in notas_com_pedido + notas_sem_pedido
    )


    # Adiciona blocos de descrição por tipo
    if notas_com_pedido:
        descricao += "*Notas com Pedido de Compra:*\n\n"
        for nota in notas_com_pedido:
            descricao += f"{nota['CHAVE']} -- {nota['NOME']}\n"
        descricao += "\n"

    if notas_sem_pedido:
        descricao += "*Notas sem Pedido de Compra:*\n"
        for nota in notas_sem_pedido:
            descricao += f"{nota['CHAVE']} -- {nota['NOME']}\n"

    if notas_nao_encontradas:
        descricao += "*Notas não encontradas na Central:*\n"
        descricao += "\n".join(notas_nao_encontradas) + "\n\n"

    if notas_nao_loja:
        descricao +="*Notas não encontradas na loja:*\n"
        for nota in notas_nao_loja:
            descricao += f"{nota['CHAVE']} -- {nota['NOME']}\n"
    if notas_gerado_pedido:
        descricao +="*Notas com pedidos gerados, segue para recebimento:*\n"
        for nota in notas_gerado_pedido:
            descricao += f"{nota['CHAVE']} -- {nota['NOME']}\n"
    # Definição do status do chamado
    if notas_nao_encontradas or notas_sem_pedido or notas_com_pedido:
        descricao += "Chamado encaminhado para análise, favor aguardar.\n\n"
        cod_status = "0000006"  # Chamado permanece aberto
    else:
        cod_status = "0000002"  # Chamado pode ser encerrado

    if notas_com_pedido:
        cod_grupo = "000049"
    else:
        cod_grupo = ""

    # Data da interação
    data_interacao = datetime.now().strftime("%d-%m-%Y")

    # Payload da API
    payload = {
        "Chave": cod_chamado,
        "TChamado": {
            "CodFormaAtendimento": "1",
            "CodStatus": cod_status,
            "CodAprovador": [""],
            "TransferirOperador": "",
            "TransferirGrupo": cod_grupo,
            "CodTerceiros": "",
            "Protocolo": "",
            "Descricao": descricao,
            "CodAgendamento": "",
            "DataAgendamento": "",
            "HoraAgendamento": "",
            "CodCausa": "000467",
            "CodOperador": "249",
            "CodGrupo": "",
            "EnviarEmail": "S",
            "EnvBase": "N",
            "CodFPMsg": "",
            "DataInteracao": data_interacao,
            "HoraInicial": "",
            "HoraFinal": "",
            "SMS": "",
            "ObservacaoInterna": "",
            "PrimeiroAtendimento": "S",
            "SegundoAtendimento": "N"
        },
        "TIc": {
            "Chave": {
                "278": "on",
                "280": "on"
            }
        }
    }

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    try:
        response = requests.put("https://api.desk.ms/ChamadosSuporte/interagir", json=payload, headers=headers)
        if response.status_code == 200:
            if cod_status == "0000006":
                log(f"Chamado {cod_chamado} encaminhado para análise. \n")
            if cod_status == "0000002":
                log(f"Chamado {cod_chamado} encerrado com sucesso! \n")
        else:
            log(f"Erro ao interagir no chamado. Código: {response.status_code}")
            log("Resposta da API:")
            log(response.text)
            try:
                log("Detalhes do erro:", response.json())
            except ValueError:
                log("Não foi possível converter a resposta da API para JSON.")
    except requests.exceptions.RequestException as e:
        log(f"Erro ao conectar com a API: {e}")