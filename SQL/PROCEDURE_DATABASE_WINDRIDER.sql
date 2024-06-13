--creare procedure con dati censurati
CREATE OR ALTER PROCEDURE DWH_WINDRIDER_PER_ANALISI_PROCEDURE AS 
BEGIN
	SET NOCOUNT ON;  -- Sopprime i messaggi di conteggio delle righe interessate


	-- Verifica l'esistenza del database DWH_WONDER_FF_MP e, se non esiste, lo crea
	IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWH_WINDRIDER_ANALISI')
		CREATE DATABASE DWH_WINDRIDER_ANALISI;


	-- INIZIO  facts_telemetry

	-- Verifica l'esistenza della tabella facts_telemetry nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp];

	CREATE TABLE [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp] (
		__RECNO__ BIGINT IDENTITY(1,1) PRIMARY KEY
		,TRANSACTION_ID VARCHAR(100)
		,READING_TIMESTAMP DATETIME
		,LOTTOPR VARCHAR(10)
		,DESLAV CHAR(40)
		,DESART VARCHAR(200)
		,MACC VARCHAR(15)
		,PARAMETER_NAME varchar(50)
		,PARAMETER_VALUE varchar(50)
		,OPERAT CHAR(40)
	);

	INSERT INTO [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp] (
		TRANSACTION_ID,
		READING_TIMESTAMP,
		LOTTOPR,
		DESLAV,
		DESART,
		MACC,
		PARAMETER_NAME,
		PARAMETER_VALUE,
		OPERAT
	)
	SELECT 
		TEL.TRANSACTION_ID
		,TEL.READING_TIMESTAMP
		,TEL.LOTTOPR
		,C.DESLAV
		,ART.DESART
		,TEL.MACC
		,TEL.PARAMETER_NAME
		,TEL.PARAMETER_VALUE
		,COALESCE(
			TEL.OPERAT,
			LAG(TEL.OPERAT) OVER (
				PARTITION BY TEL.MACC
				ORDER BY TEL.READING_TIMESTAMP DESC
			),
			'N.A.'
		) AS DESOPE
	FROM [TS_WND].[dbo].[TELEMETRY_CHANGES] AS TEL
		LEFT JOIN [TS_WND].[dbo].[CICLI] AS C
			ON TEL.LOTTOPR = C.LOTTOPR
				AND TEL.CODART = C.CODART
				AND TEL.FASE = C.FASE
		LEFT JOIN [TS_WND].[dbo].[ARTICOLI] AS ART
			ON TEL.CODART = ART.CODART  
		LEFT JOIN [TS_WND].[dbo].[TABOPE] AS OPE
			ON TEL.OPERAT = OPE.OPERAT
	WHERE TEL.DATASTATE = 'Good' AND TEL.MACC <> ''
		AND PARAMETER_VALUE <> ''
		AND ART.LOTTOPR = '0000000000';
		

	-- INIZIO  DIM_LOTTIPR
	
	-- Verifica l'esistenza della tabella DIM_LOTTIPR nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR_temp]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR_temp];

	CREATE TABLE [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR_temp] (
		LOTTOPR CHAR(10) PRIMARY KEY
		,DESART VARCHAR(200)
		,TIPART CHAR(1)
		,SCADENZA DATE
		,DATINIPRO DATETIME
		,DATFINPRO DATETIME
	);

	INSERT INTO [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR_temp] (
		LOTTOPR,
		DESART,
		TIPART,
		SCADENZA,
		DATINIPRO,
		DATFINPRO
	)
	SELECT
		LPR.LOTTOPR
		,RTRIM(ART.DESART +' '+ ART.DESESTESA) AS DESART
		,ART.TIPART
		,CAST(LPR.SCADENZA AS DATE) AS SCADENZA
		,LPR.DATINIPRO
		,LPR.DATFINPRO
	FROM [TS_WND].[dbo].[LOTTIPR] AS LPR
	LEFT JOIN [TS_WND].[dbo].[ARTICOLI] AS ART
		ON LPR.CODART = ART.CODART
	WHERE ART.LOTTOPR = '0000000000'
		AND LPR.LOTTOPR IN(SELECT DISTINCT LOTTOPR FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp])
		AND LPR.DATINIPRO IS NOT NULL AND LPR.DATFINPRO IS NOT NULL
		AND LPR.DATINIPRO >= (
			SELECT MIN(READING_TIMESTAMP)
			FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp]
		)
	GROUP BY
		LPR.LOTTOPR
		,ART.DESART
		,ART.DESESTESA
		,ART.TIPART
		,LPR.SCADENZA
		,LPR.DATINIPRO
		,LPR.DATFINPRO; 

	-- FINE  DIM_LOTTIPR

	
	-- Filtra la tabella telemetry, eliminado i lotti che non servono
	DELETE FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp]
	WHERE LOTTOPR NOT IN(SELECT LOTTOPR FROM [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR_temp]);

	-- Verifica l'esistenza della tabella facts_telemetry nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry];

	select * 
	INTO [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry]
	FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp]
	WHERE MACC NOT IN('CNC08', 'HAL11', 'HAL16', 'IND01', 'IND02', 'TRF10')
		AND PARAMETER_NAME IN('Pressione', 'Temperatura', 'Velocità', 'TEMPERATURA_GRADI')
		OR PARAMETER_NAME LIKE 'DIFETTO%' OR PARAMETER_NAME LIKE 'READ%'
		OR PARAMETER_NAME LIKE 'SET%' OR PARAMETER_NAME LIKE 'PEZZO%'
	
	--elimina la tabella temporanea
	Drop table [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry_temp]

	--Verifica l'esistenza della tabella DIM_LOTTIPR nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR];
	
	select *
	into [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR]
	from [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR_temp]
	wHERE LOTTOPR IN (SELECT DISTINCT(LOTTOPR) FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry]);

	--elimina la tabella temporanea
	drop table [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR_temp]

	
	-- FINE  facts_telemetry
	-- INIZIO  Dim_macchine

	-- Verifica l'esistenza della tabella Dim_macchine nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[Dim_macchine]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[Dim_macchine];

	CREATE TABLE [DWH_WINDRIDER_ANALISI].[dbo].[Dim_macchine] (
		MACC CHAR(15) PRIMARY KEY
		,DESMACC CHAR(40)
		,DESREP CHAR(40)
	);

	INSERT INTO [DWH_WINDRIDER_ANALISI].[dbo].[Dim_macchine] (MACC, DESMACC, DESREP)
	SELECT
		M.MACC
		,M.DESMACC
		,CASE M.CODREP
			WHEN '' THEN 'N.A.'
			ELSE REP.DESREP
		END AS DESREP
	FROM [TS_WND].[dbo].[MACCHINE] AS M
	LEFT JOIN [TS_WND].[dbo].[TABREP] AS REP
		ON M.CODREP = REP.CODREP
	WHERE M.MACC IN(SELECT DISTINCT MACC FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry]);

	-- FINE  Dim_macchine
	-- INIZIO  Dim_matprime

	-- Verifica l'esistenza della tabella Dim_matprime nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[Dim_matprime]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[Dim_matprime];

	CREATE TABLE [DWH_WINDRIDER_ANALISI].[dbo].[Dim_matprime] (
		__RECNO__ BIGINT IDENTITY(1,1) PRIMARY KEY
		,LOTTOPR CHAR(10)
		,DESLAV CHAR(40)  -- DESLAV
		,DESART CHAR(200)
		,DESMATP CHAR(40)
	);

	INSERT INTO [DWH_WINDRIDER_ANALISI].[dbo].[Dim_matprime] (LOTTOPR, DESLAV, DESART, DESMATP)
	SELECT
		D.LOTTOPR
		,C.DESLAV
		,RTRIM(ART.DESART +' '+ ART.DESESTESA) AS DESART
		,CASE MP.DESMATP
			WHEN '' THEN 'N.A.'
			WHEN NULL THEN 'N.A.'
			ELSE MP.DESMATP
		END AS DESMATP
	FROM [TS_WND].[dbo].[DISTBASE] AS D
	LEFT JOIN [TS_WND].[dbo].[MATPRIME] AS MP
		ON D.CODCOMP = MP.CODMATP
	LEFT JOIN [TS_WND].[dbo].[CICLI] AS C
			ON D.LOTTOPR = C.LOTTOPR
				AND D.CODART = C.CODART
				AND D.FASE = C.FASE
	LEFT JOIN [TS_WND].[dbo].[ARTICOLI] AS ART
		ON D.CODART = ART.CODART
	WHERE ART.LOTTOPR = '0000000000'
		AND D.TIPOCOMP = 'M'
		AND D.LOTTOPR IN(SELECT LOTTOPR FROM [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR]);

	-- FINE  Dim_matprime
	-- INIZIO  Dim_prod

	-- Verifica l'esistenza della tabella Dim_prod nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[Dim_prod]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[Dim_prod];

	CREATE TABLE [DWH_WINDRIDER_ANALISI].[dbo].[Dim_prod] (
		__RECNO__ BIGINT IDENTITY(1,1) PRIMARY KEY
		,LOTTOPR CHAR(10)
		,DATA DATE
		,QUANT FLOAT
		,QUANTSC FLOAT
		,DESCDS CHAR(40)
	);

	INSERT INTO [DWH_WINDRIDER_ANALISI].[dbo].[Dim_prod] (LOTTOPR, DATA, QUANT, QUANTSC, DESCDS)
	SELECT
		RQ.LOTTOPR
		,CAST(RQ.DATA AS DATE) AS DATA
		,SUM(
			CASE
				WHEN RQ.CODCDS = '' THEN RQ.QUANT
				ELSE 0
			END
		) AS QUANT
		,SUM(
			CASE
				WHEN RQ.CODCDS <> '' THEN RQ.QUANT
				ELSE 0
			END
		) AS QUANTSC
		,CASE RQ.CODCDS
			WHEN '' THEN 'N.A.'
			ELSE TCS.DESCDS
		END AS DESCDS
	FROM [TS_WND].[dbo].[RESQUANT] AS RQ
	LEFT JOIN [TS_WND].[dbo].[TABCSCAR] AS TCS
		ON RQ.CODCDS = TCS.CODCDS
	WHERE RQ.LOTTOPR IN(SELECT LOTTOPR FROM [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR])
		AND RQ.DATA BETWEEN (
				SELECT MIN(READING_TIMESTAMP)
				FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry]
			)
			AND
			(
				SELECT MAX(READING_TIMESTAMP)
				FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry]
			)
	GROUP BY
		RQ.LOTTOPR
		,RQ.DATA
		,RQ.CODCDS
		,TCS.DESCDS
	HAVING SUM(CASE WHEN RQ.CODCDS = '' THEN RQ.QUANT ELSE 0 END) <> 0
		OR SUM(CASE WHEN RQ.CODCDS <> '' THEN RQ.QUANT ELSE 0 END) <> 0
	ORDER BY RQ.LOTTOPR, RQ.DATA;


	WITH recursive_CTE AS (
		SELECT
			__RECNO__
			,DATA
			,LOTTOPR
			,QUANT
			,ROW_NUMBER() OVER (PARTITION BY LOTTOPR ORDER BY DATA DESC) AS RN
		FROM [DWH_WINDRIDER_ANALISI].[dbo].[Dim_prod]
	)

	,recursive_result AS (
		SELECT
			__RECNO__
			,DATA
			,LOTTOPR
			,CASE
				WHEN QUANT < 0 THEN 0
				ELSE QUANT
			END AS QUANT
			,RN
			,QUANT AS running_sum
		FROM recursive_CTE
		WHERE RN = (SELECT MIN(RN) FROM recursive_CTE WHERE QUANT < 0)

		UNION ALL

		SELECT
			RC.__RECNO__
			,RC.DATA
			,RC.LOTTOPR
			,CASE
				WHEN RC.QUANT = 0 THEN 0
				WHEN RR.running_sum + RC.QUANT >= 0 THEN RR.running_sum + RC.QUANT
				ELSE 0
			END AS QUANT
			,RC.RN
			,CASE
				WHEN RR.running_sum + RC.QUANT < 0 THEN RR.running_sum + RC.QUANT
				ELSE 0
			END AS running_sum
		FROM recursive_CTE AS RC
		INNER JOIN recursive_result AS RR
			ON RC.LOTTOPR = RR.LOTTOPR
			AND RC.RN = RR.RN + 1
	)


	UPDATE DPROD
	SET DPROD.QUANT = RR.QUANT
	FROM [DWH_WINDRIDER_ANALISI].[dbo].[Dim_prod] AS DPROD
	RIGHT JOIN recursive_result AS RR
		ON DPROD.__RECNO__ = RR.__RECNO__
	OPTION(MAXRECURSION 0);

	DELETE FROM [DWH_WINDRIDER_ANALISI].[dbo].[Dim_prod]
	WHERE QUANT = 0 AND QUANTSC = 0;

	-- FINE  Dim_prod
	-- INIZIO  Dim_grumacc

	-- Verifica l'esistenza della tabella Dim_grumacc nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[Dim_grumacc]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[Dim_grumacc];

	CREATE TABLE [DWH_WINDRIDER_ANALISI].[dbo].[Dim_grumacc] (
		__RECNO__ BIGINT IDENTITY(1,1) PRIMARY KEY
		,MACC CHAR(15)
		,GRUMACC CHAR(15)
	);

	INSERT INTO [DWH_WINDRIDER_ANALISI].[dbo].[Dim_grumacc] (MACC, GRUMACC)
	SELECT
		MACC
		,GRUMACC
	FROM [TS_WND].[dbo].DETGRMAC
	WHERE MACC IN(SELECT MACC FROM [DWH_WINDRIDER_ANALISI].[dbo].[Dim_macchine]);

	-- FINE  Dim_grumacc
	-- INIZIO  DIM_FERM


	-- Verifica l'esistenza della tabella DIM_FERM nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[DIM_FERM]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[DIM_FERM];

	CREATE TABLE [DWH_WINDRIDER_ANALISI].[dbo].[DIM_FERM] (
		__RECNO__ BIGINT IDENTITY(1,1) PRIMARY KEY
		,MACC VARCHAR(15)
		,LOTTOPR VARCHAR(10)
		,DESLAV CHAR(40)
		,DESOPE CHAR(40)
		,TMACC FLOAT
		,TLOT FLOAT
		,POPER BIT
		,QPROD FLOAT
		,QSC FLOAT
		,INI DATETIME
		,FIN DATETIME
		,DESFERM CHAR(40)
	);

	WITH FERM_CTE AS (
		SELECT
			RT.MACC
			,RT.LOTTOPR
			,C.DESLAV
			,COALESCE(
				RT.OPERAT,
				LAG(RT.OPERAT) OVER (
					PARTITION BY RT.MACC
					ORDER BY RT.DATA DESC, RT.INI DESC
				),
				'N.A.'
			) AS OPERAT
			,RT.TMACC
			,RT.TLOT
			,CAST(RT.TOPER AS BIT) AS POPER  
			,RT.QPROD
			,RT.QSCPR + RT.QSCLA AS QSC
			,RT.DATA
			,RT.INI
			,RT.FIN
			,ISNULL(TCF.DESFERM, 'PRODUZIONE') AS DESFERM
		FROM [TS_WND].[dbo].[RESTURNI] as RT
		LEFT JOIN [TS_WND].[dbo].[TABCFER] AS TCF
			ON RT.CODFERM = TCF.CODFERM
		LEFT JOIN [TS_WND].[dbo].[LOTTIPR] AS LPR
			ON RT.LOTTOPR = LPR.LOTTOPR
		LEFT JOIN [TS_WND].[dbo].[CICLI] AS C
			ON RT.LOTTOPR = C.LOTTOPR
			AND LPR.CODART = C.CODART
			AND RT.FASE = C.FASE
		 WHERE RT.LOTTOPR IN(SELECT LOTTOPR FROM [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR])
		    AND RT.MACC IN(SELECT MACC FROM [DWH_WINDRIDER_ANALISI].[dbo].[Dim_macchine])
			AND QPROD >= 0
	)


	INSERT INTO [DWH_WINDRIDER_ANALISI].[dbo].[DIM_FERM] (
		MACC,
		LOTTOPR,
		DESLAV,
		DESOPE,
		TMACC,
		TLOT,
		POPER,
		QPROD,
		QSC,
		INI,
		FIN,
		DESFERM
	)
	SELECT
		MACC
		,LOTTOPR
		,DESLAV
		,OPERAT
		,TMACC
		,TLOT
		,POPER
		,QPROD
		,QSC
		,DATEADD(
			MINUTE,
			CAST(LEFT(INI, 2) AS INT) * 60 + CAST(RIGHT(INI, 2) AS INT),
			DATA
		) AS INI
		,DATEADD(
			MINUTE,
			CAST(LEFT(FIN, 2) AS INT) * 60 + CAST(RIGHT(FIN, 2) AS INT),
			DATA
		) AS FIN
		,DESFERM
	FROM FERM_CTE
	WHERE DATA BETWEEN (
			SELECT MIN(READING_TIMESTAMP)
			FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry]
		)
		AND
		(
			SELECT MAX(READING_TIMESTAMP)
			FROM [DWH_WINDRIDER_ANALISI].[dbo].[facts_telemetry]
		);

	-- FINE  DIM_FERM
	-- INIZIO DIM_RESA

	-- Verifica l'esistenza della tabella DIM_RESA nel database DWH_WONDER_FF_MP e, se presente, la elimina
	IF OBJECT_ID('[DWH_WINDRIDER_ANALISI].[dbo].[[DIM_RESA]]', 'U') IS NOT NULL
		DROP TABLE [DWH_WINDRIDER_ANALISI].[dbo].[DIM_RESA];

	WITH CICLOREA_CTE AS (
		SELECT
			LOTTOPR
			,MACC
			,FASE
			,CODART
			,SUM(TLOT) / CASE SUM(QPROD + QSCPR + QSCLA)
				WHEN 0 THEN 1
				ELSE SUM(QPROD + QSCPR + QSCLA)
			END AS CICLOREA
		FROM [TS_WND].[dbo].[RESTEMPI]
		GROUP BY
			LOTTOPR
			,MACC
			,FASE
			,CODART
			,CODFERM
		HAVING LOTTOPR IN(SELECT LOTTOPR FROM [DWH_WONDER_ANALISI].[dbo].[DIM_LOTTIPR])
			AND SUM(QPROD + QSCPR + QSCLA) <> 0
			AND CODFERM = '99'
	)


	SELECT
		CR.LOTTOPR
		,CR.MACC
		,C.DESLAV
		,ROUND(C.CICLOTEO / CASE CR.CICLOREA
			WHEN 0 THEN 1
			ELSE CR.CICLOREA
		END * 100.0, 2) AS 'RESA (%)'
	INTO [DWH_WINDRIDER_ANALISI].[dbo].[DIM_RESA]
	FROM CICLOREA_CTE AS CR
	INNER JOIN [TS_WND].[dbo].[CICLI] AS C
		ON CR.LOTTOPR = C.LOTTOPR
		AND CR.FASE = C.FASE
		AND CR.CODART = C.CODART;

	--FINE DIM_RESA
END;
GO

exec DWH_WINDRIDER_PER_ANALISI_PROCEDURE

------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE DWH_WINDRIDER_PER_CENSURA_PROCEDURE AS 
BEGIN
	SET NOCOUNT ON;

	--A
	--censurare colonna DESMATP tabella Dim_matprime
	DECLARE @json NVARCHAR(MAX) = N'
	[
		"Alluminio AA",
		"Acciaio AA",
		"Fibra di carbonio AA",
		"Titanio",
		"Acciaio BB",
		"Fibra di carbonio BB",
		"Alluminio BB",
		"Cerchi in alluminio",
		"Cerchi in fibra di carbonio",
		"Raggi in acciaio inossidabile",
		"Mozzi in alluminio o acciaio",
		"Gomme naturali",
		"Gomme sintetiche GT",
		"Gomma butilica TH",
		"Lattice",
		"Pelle",
		"Gel",
		"Carbonio",
		"Poliestere",
		"Silicone",
		"Cotone",
		"Pinze in alluminio",
		"Pastiglie dei freni in gomma sintetica",
		"Acciaio inossidabile",
		"Rivestimenti in plastica",
		"Cuscinetti in ceramica",
		"Acciaio inossidabile CCC",
		"Parafanghi",
		"Pedivelle",
		"Guarnitura",
		"Cavi dei freni e del cambio",
		"Portapacchi",
		"Cavalletto",
		"Gomma naturale FG",
		"Gomma sintetica DR",
		"Valvole",
		"Ottone",
		"LED",
		"Reggisella",
		"Catadiottri",
		"Portaborraccia AA",
		"Paracatena",
		"Tappi manubrio",
		"Attacchi manubrio",
		"Serie sterzo",
		"Acciaio inossidabile GP",
		"Rivestimenti in Teflon",
		"Blocchi pedale",
		"Cestini",
		"Sellini"
	]';

	WITH OLD_DESMATP_CTE AS (
		SELECT DESMATP, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM (SELECT DISTINCT DESMATP FROM [DWH_WONDER_FF_MP].[dbo].[Dim_matprime]) AS L
	)
	,NEW_DESMATP_JSON_CTE AS (
		SELECT VALUE AS VAL, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM OPENJSON(@JSON)
	)
	,NEW_DESMATP_CTE AS (
		SELECT OAC.DESMATP, NAVC.VAL AS NEW_DESMATP
		FROM OLD_DESMATP_CTE AS OAC
		INNER JOIN NEW_DESMATP_JSON_CTE AS NAVC
			ON OAC.Row_Num = NAVC.Row_Num
	)

	UPDATE L
	SET L.DESMATP = NAC.NEW_DESMATP
	FROM [DWH_WONDER_FF_MP].[dbo].[Dim_matprime] AS L
	INNER JOIN NEW_DESMATP_CTE AS NAC
		ON L.DESMATP = NAC.DESMATP;

	--B
	--censurare colonne MACC, DESMACC, REP tabella Dim_macchine
	DECLARE @macc NVARCHAR(MAX) = N'
	[
		"PPR01",
		"GM01",
		"AM28",
		"AM29",
		"AM30",
		"AM33",
		"GP02",
		"CSC01",
		"CSC02",
		"CSC03"
	]';

	WITH OLD_MACC_CTE AS (
		SELECT MACC, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM (SELECT DISTINCT MACC FROM [DWH_WONDER_FF_MP].[dbo].[Dim_macchine]) AS L
	)
	,NEW_MACC_JSON_CTE AS (
		SELECT VALUE AS VAL, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM OPENJSON(@macc)
	)
	,NEW_MACC_CTE AS (
		SELECT OAC.MACC, NAVC.VAL AS NEW_MACC
		FROM OLD_MACC_CTE AS OAC
		INNER JOIN NEW_MACC_JSON_CTE AS NAVC
			ON OAC.Row_Num = NAVC.Row_Num
	)

	UPDATE L
	SET L.MACC = NAC.NEW_MACC
	FROM [DWH_WONDER_FF_MP].[dbo].[Dim_macchine] AS L
	INNER JOIN NEW_MACC_CTE AS NAC
		ON L.MACC = NAC.MACC;

	--C
	DECLARE @desmacc NVARCHAR(MAX) = N'
	[
		"Velocityis pro",
		"Gommis pro",
		"Union",
		"Union",
		"Union",
		"Union",
		"Ripasso meccanico",
		"controllo qualità",
		"controllo qualità",
		"controllo qualità"
	]';

	WITH OLD_DESMACC_CTE AS (
		SELECT DESMACC, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM (SELECT DISTINCT DESMACC FROM [DWH_WONDER_FF_MP].[dbo].[Dim_macchine]) AS L
	)
	,NEW_MACC_JSON_CTE AS (
		SELECT VALUE AS VAL, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM OPENJSON(@desmacc)
	)
	,NEW_DESMACC_CTE AS (
		SELECT OAC.DESMACC, NAVC.VAL AS NEW_DESMACC
		FROM OLD_DESMACC_CTE AS OAC
		INNER JOIN NEW_MACC_JSON_CTE AS NAVC
			ON OAC.Row_Num = NAVC.Row_Num
	)

	UPDATE L
	SET L.DESMACC = NAC.NEW_DESMACC
	FROM [DWH_WONDER_FF_MP].[dbo].[Dim_macchine] AS L
	INNER JOIN NEW_DESMACC_CTE AS NAC
		ON L.DESMACC = NAC.DESMACC;

	--D
	DECLARE @rep NVARCHAR(MAX) = N'
	[
		"Reparto Creation qualità",
		"Reparto Fusion",
		"Reparto Fusion",
		"Reparto Fusion",
		"Reparto Fusion",
		"Reparto Fusion",
		"Reparto Mecanic",
		"Reparto Creation qualità",
		"Reparto Creation qualità",
		"Reparto Creation qualità"
	]';

	WITH OLD_DESMACC_CTE AS (
		SELECT DESREP, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM (SELECT DISTINCT DESREP FROM [DWH_WONDER_FF_MP].[dbo].[Dim_macchine]) AS L
	)
	,NEW_MACC_JSON_CTE AS (
		SELECT VALUE AS VAL, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM OPENJSON(@rep)
	)
	,NEW_DESMACC_CTE AS (
		SELECT OAC.DESREP, NAVC.VAL AS NEW_DESREP
		FROM OLD_DESMACC_CTE AS OAC
		INNER JOIN NEW_MACC_JSON_CTE AS NAVC
			ON OAC.Row_Num = NAVC.Row_Num
	)

	UPDATE L
	SET L.DESREP = NAC.NEW_DESREP
	FROM [DWH_WONDER_FF_MP].[dbo].[Dim_macchine] AS L
	INNER JOIN NEW_DESMACC_CTE AS NAC
		ON L.DESREP = NAC.DESREP;

	--E
	--censurare colonne GRUMACC tabella Dim_grumacc
	DECLARE @grumacc NVARCHAR(MAX) = N'   
	[
		"ALL",
		"TEAS",
		"PPR01",
		"MAB_1",
		"MAB_2",
		"MAB_03",
		"MANB",
		"BUN",
		"CRVA",
		"CSC01_nn",
		"CSC02_nn",
		"CSC03_nn"
	]';

	WITH OLD_GRUMACC_CTE AS (
		SELECT GRUMACC, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM (SELECT DISTINCT GRUMACC FROM [DWH_WONDER_FF_MP].[dbo].[Dim_grumacc]) AS L
	)
	,NEW_GRUMACC_JSON_CTE AS (
		SELECT VALUE AS VAL, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM OPENJSON(@grumacc)
	)
	,NEW_GRUMACC_CTE AS (
		SELECT OAC.GRUMACC, NAVC.VAL AS NEW_GRUMACC
		FROM OLD_GRUMACC_CTE AS OAC
		INNER JOIN NEW_GRUMACC_JSON_CTE AS NAVC
			ON OAC.Row_Num = NAVC.Row_Num
	)

	UPDATE L
	SET L.GRUMACC = NAC.NEW_GRUMACC
	FROM [DWH_WONDER_FF_MP].[dbo].[Dim_GRUMACC] AS L
	INNER JOIN NEW_GRUMACC_CTE AS NAC
		ON L.GRUMACC = NAC.GRUMACC;

	--F
	WITH OLD_OPERAT_CTE AS (
		SELECT OPERAT, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM (SELECT DISTINCT OPERAT FROM [DWH_WONDER_ANALISI].[dbo].[facts_telemetry]) AS L
		WHERE OPERAT IS NOT NULL
	)
	,NEW_OPERAT_VALUE_CTE AS (
		SELECT RIGHT(NEWID(), 12) AS NEW_OPERAT, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM OLD_OPERAT_CTE
	)
	,NEW_OPERAT_CTE AS (
		SELECT OAC.OPERAT, NAVC.NEW_OPERAT AS NEW_OPERAT
		FROM OLD_OPERAT_CTE AS OAC
		INNER JOIN NEW_OPERAT_VALUE_CTE	AS NAVC
			ON OAC.Row_Num = NAVC.Row_Num
	)


	UPDATE L
	SET OPERAT = NAC.NEW_OPERAT
	FROM [DWH_WONDER_ANALISI].[dbo].[facts_telemetry] AS L
	LEFT JOIN NEW_OPERAT_CTE AS NAC
		ON L.OPERAT = NAC.OPERAT;

	--G
	--censurare colonna DESART tabella DIM_LOTTIPR
	DECLARE @new_desart_arr NVARCHAR(MAX) = '[
		"Sella Brooks B17 Standard",
		"Pompa da pavimento Lezyne Steel Floor Drive",
		"Lucchetto a catena Kryptonite Evolution Series 4",
		"Portaborraccia Tacx Ciro",
		"Portapacchi Topeak Explorer",
		"Luci anteriori e posteriori Knog Blinder Road 600",
		"Borse laterali Ortlieb Back-Roller Classic",
		"Copertoni Continental Grand Prix 5000",
		"Pedali Shimano PD-M520",
		"Serraggio rapido Thule OutRide 561",
		"Campanello Spurcycle",
		"Porta smartphone Quad Lock Bike Mount",
		"Porta attrezzi Topeak Ninja Pouch+ Road",
		"Freni a disco Shimano XT M8000",
		"Copertura sella Fizik Arione",
		"Sella Superflow Boost",
		"Cassette Shimano Ultegra CS-R8000",
		"Pompa portatile Topeak Mini Morph G",
		"Guarnitura SRAM Force AXS DUB",
		"Manubrio in carbonio FSA K-Force Compact",
		"Ciclocomputer Garmin Edge 1030 Plus",
		"Parafango anteriore SKS S-Board",
		"Parafango posteriore Topeak DeFender M2",
		"Occhiali da sole Oakley Jawbreaker",
		"Guanti da ciclismo Giro Monaco 2",
		"Pantaloncini con fondello Free Race 4",
		"Copriscarpe GripGrab RaceThermo",
		"Casco Kask Protone",
		"Reggisella telescopico RockShox Reverb Stealth",
		"Portapacchi posteriore Tubus TGT",
		"Lucchetto a U Abus Granit X-Plus 540",
		"Porta bici Thule EasyFold XT 2",
		"Trasporto bici Elite Vaison Bike Box",
		"Lucchetto a cavo Abus Steel-O-Chain 880",
		"Contachilometri Cateye Wireless",
		"Forcella RockShox Pike Ultimate",
		"Cambio posteriore Shimano XT RD-M8100",
		"Catena KMC X11SL"
	]';



	WITH OLD_DESART_CTE AS (
		SELECT DESART, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM (SELECT DISTINCT DESART FROM [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR]) AS L
	)
	,NEW_DESART_VALUE_CTE AS (
		SELECT VALUE AS VAL, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Row_Num
		FROM OPENJSON(@new_desart_arr)
	)
	,NEW_DESART_CTE AS (
		SELECT OAC.DESART, NAVC.VAL AS NEW_DESART
		FROM OLD_DESART_CTE AS OAC
		INNER JOIN NEW_DESART_VALUE_CTE	AS NAVC
			ON OAC.Row_Num = NAVC.Row_Num
	)


	UPDATE L
	SET L.DESART = NAC.NEW_DESART
	FROM [DWH_WINDRIDER_ANALISI].[dbo].[DIM_LOTTIPR] AS L
	INNER JOIN NEW_DESART_CTE AS NAC
		ON L.DESART = NAC.DESART;

END;
GO

exec DWH_WINDRIDER_PER_CENSURA_PROCEDURE