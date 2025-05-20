-- CREATE DATABASE BIZ_2025
USE BIZ_2025
drop table JPKWB_Podmiot, JPKWB_Ctrl, JPKWB_Salda, JPKWB_Transakcje, JPKWB_Rachunek, JPKWB_BledyWalidacji
drop table JPKWB_Naglowek
-- Tworzenie tabel
CREATE TABLE JPKWB_Naglowek (
                                IdJPK INT IDENTITY PRIMARY KEY,
                                CelZlozenia TINYINT,
                                DataWytworzenia DATETIME,
                                DataOd DATE,
                                DataDo DATE,
                                KodWaluty CHAR(3),
                                KodUrzedu CHAR(5)
);


CREATE TABLE JPKWB_Podmiot (
                               IdJPK INT PRIMARY KEY,
                               NIP CHAR(10),
                               PelnaNazwa NVARCHAR(255),
                               Ulica NVARCHAR(255),
                               NrDomu NVARCHAR(10),
                               NrLokalu NVARCHAR(10),
                               KodPocztowy CHAR(6),
                               Miejscowosc NVARCHAR(255),
                               Wojewodztwo NVARCHAR(255),
                               Powiat NVARCHAR(255),
                               Gmina NVARCHAR(255),
                               KodKraju NVARCHAR(2),
                               FOREIGN KEY (IdJPK) REFERENCES JPKWB_Naglowek(IdJPK)
);

CREATE TABLE JPKWB_Transakcje (
                                  IdTransakcji INT IDENTITY PRIMARY KEY,
                                  IdJPK INT,
                                  NumerWiersza INT,
                                  DataOperacji DATE,
                                  NazwaPodmiotu NVARCHAR(255),
                                  OpisOperacji NVARCHAR(255),
                                  KwotaOperacji DECIMAL(18,2),
                                  SaldoOperacji DECIMAL(18,2),
                                  FOREIGN KEY (IdJPK) REFERENCES JPKWB_Naglowek(IdJPK)
);

CREATE TABLE JPKWB_Salda (
                             IdJPK INT PRIMARY KEY,
                             SaldoPoczatkowe DECIMAL(18,2),
                             SaldoKoncowe DECIMAL(18,2),
                             FOREIGN KEY (IdJPK) REFERENCES JPKWB_Naglowek(IdJPK)
);

CREATE TABLE JPKWB_Ctrl (
                            IdJPK INT PRIMARY KEY,
                            LiczbaWierszy CHAR(14),
                            SumaObciazen DECIMAL(18,2),
                            SumaUznan DECIMAL(18,2),
                            FOREIGN KEY (IdJPK) REFERENCES JPKWB_Naglowek(IdJPK)
);

CREATE TABLE JPKWB_Rachunek (
                                IdJPK INT PRIMARY KEY,
                                NumerRachunku NVARCHAR(34),
                                FOREIGN KEY (IdJPK) REFERENCES JPKWB_Naglowek(IdJPK)
);

CREATE TABLE JPKWB_BledyWalidacji (
                                      Id INT IDENTITY PRIMARY KEY,
                                      IdJPK INT,
                                      OpisBledu NVARCHAR(500),
                                      FOREIGN KEY (IdJPK) REFERENCES JPKWB_Naglowek(IdJPK)
);
-- Tworzenie tabel tymaczowych do importu danych

CREATE TABLE TmpNaglowek (
                             KodFormularza VARCHAR(10),
                             WariantFormularza INT,
                             CelZlozenia TINYINT,
                             DataWytworzenia DATETIME,
                             DataOd DATE,
                             DataDo DATE,
                             KodWaluty CHAR(3),
                             KodUrzedu CHAR(5),
                             NIP CHAR(10),
                             PelnaNazwa NVARCHAR(255),
                             Ulica NVARCHAR(255),
                             NrDomu NVARCHAR(10),
                             NrLokalu NVARCHAR(10),
                             KodPocztowy CHAR(6),
                             Miejscowosc NVARCHAR(255),
                             Wojewodztwo NVARCHAR(255),
                             Powiat NVARCHAR(255),
                             Gmina NVARCHAR(255),
                             NumerRachunku VARCHAR(34)
);
CREATE TABLE TmpTransakcje (
                               NumerWiersza INT,
                               DataOperacji DATE,
                               NazwaPodmiotu NVARCHAR(255),
                               OpisOperacji NVARCHAR(255),
                               KwotaOperacji DECIMAL(18,2),
                               SaldoPrzedOperacja DECIMAL(18,2),
                               SaldoPoOperacji DECIMAL(18,2),
                               NumerRachunku VARCHAR(34)
);
BULK INSERT TmpNaglowek
    FROM '/var/opt/mssql/data/naglowek.csv'
    WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '\t',
    ROWTERMINATOR = '\n'
    );

BULK INSERT TmpTransakcje
    FROM '/var/opt/mssql/data/transakcje.csv'
    WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '\t',
    ROWTERMINATOR = '\n'
    );
select * from TmpNaglowek

INSERT INTO JPKWB_Naglowek (CelZlozenia, DataWytworzenia, DataOd, DataDo, KodWaluty, KodUrzedu)
SELECT CelZlozenia, DataWytworzenia, DataOd, DataDo, KodWaluty, KodUrzedu
FROM TmpNaglowek;
select * from JPKWB_Naglowek
-- Pobranie ostatniego IDENTITY wygenerowanego w tym zakresie
DECLARE @IdJPK INT;
SET @IdJPK = SCOPE_IDENTITY();
PRINT @IdJPK;

-- Wstaw podmiot
INSERT INTO JPKWB_Podmiot (IdJPK, NIP, PelnaNazwa, Ulica, NrDomu, NrLokalu, KodPocztowy, Miejscowosc, Wojewodztwo, Powiat, Gmina)
SELECT TOP 1 @IdJPK, NIP, PelnaNazwa, Ulica, NrDomu, NrLokalu, KodPocztowy, Miejscowosc, Wojewodztwo, Powiat, Gmina FROM TmpNaglowek;

INSERT INTO JPKWB_Transakcje (
    IdJPK, NumerWiersza, DataOperacji, NazwaPodmiotu, OpisOperacji, KwotaOperacji,
    SaldoOperacji
)
SELECT
    @IdJPK, NumerWiersza, DataOperacji, NazwaPodmiotu, OpisOperacji, KwotaOperacji,
    KwotaOperacji
FROM TmpTransakcje
WHERE DataOperacji BETWEEN (SELECT DataOd FROM JPKWB_Naglowek WHERE IdJPK = @IdJPK)
          AND (SELECT DataDo FROM JPKWB_Naglowek WHERE IdJPK = @IdJPK);

-- Saldo
INSERT INTO JPKWB_Salda (IdJPK, SaldoPoczatkowe, SaldoKoncowe)
SELECT @IdJPK,
       MIN(SaldoPrzedOperacja),
       MAX(SaldoPoOperacji)
FROM TmpTransakcje

-- Kontrola
INSERT INTO JPKWB_Ctrl (IdJPK, LiczbaWierszy, SumaObciazen, SumaUznan)
SELECT
    @IdJPK,
    COUNT(*),
    SUM(CASE WHEN KwotaOperacji < 0 THEN ABS(KwotaOperacji) ELSE 0 END),
    SUM(CASE WHEN KwotaOperacji > 0 THEN KwotaOperacji ELSE 0 END)
FROM JPKWB_Transakcje
WHERE IdJPK = @IdJPK;

-- Numer rachunku
INSERT INTO JPKWB_Rachunek (IdJPK, NumerRachunku)
SELECT @IdJPK, NumerRachunku FROM TmpNaglowek;

CREATE OR ALTER FUNCTION JPK_TXT (
    @input NVARCHAR(MAX)
)
    RETURNS NVARCHAR(MAX)
AS
BEGIN
    RETURN LTRIM(RTRIM(@input));
END;

-- Walidacja danych
CREATE OR ALTER PROCEDURE spWalidujJPKWB
@IdJPK INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ErrorMessage NVARCHAR(MAX) = '';
    DECLARE @Opis NVARCHAR(500);

    DELETE FROM JPKWB_BledyWalidacji WHERE IdJPK = @IdJPK;

    -- Czy istnieje rekord w JPKWB_Naglowek
    IF NOT EXISTS (SELECT 1 FROM JPKWB_Naglowek WHERE IdJPK = @IdJPK)
        BEGIN
            SET @Opis = N'BŁĄD: Nie znaleziono nagłówka o podanym IdJPK.';
            INSERT INTO JPKWB_BledyWalidacji (IdJPK, OpisBledu) VALUES (@IdJPK, @Opis);
            RETURN;
        END

    DECLARE @DataOd DATE, @DataDo DATE;
    SELECT @DataOd = DataOd, @DataDo = DataDo FROM JPKWB_Naglowek WHERE IdJPK = @IdJPK;

    -- 1. Czy istnieje Podmiot
    IF NOT EXISTS (SELECT 1 FROM JPKWB_Podmiot WHERE IdJPK = @IdJPK)
        BEGIN
            SET @Opis = N'BŁĄD: Brakuje danych podmiotu.';
            INSERT INTO JPKWB_BledyWalidacji (IdJPK, OpisBledu) VALUES (@IdJPK, @Opis);
            SET @ErrorMessage += @Opis + CHAR(13);
        END

    -- 2. Czy są transakcje
    IF NOT EXISTS (SELECT 1 FROM JPKWB_Transakcje WHERE IdJPK = @IdJPK)
        BEGIN
            SET @Opis = N'BŁĄD: Brak danych transakcji.';
            INSERT INTO JPKWB_BledyWalidacji (IdJPK, OpisBledu) VALUES (@IdJPK, @Opis);
            SET @ErrorMessage += @Opis + CHAR(13);
        END

    -- 3. KwotaOperacji ≠ 0
    IF EXISTS (
        SELECT 1 FROM JPKWB_Transakcje WHERE IdJPK = @IdJPK AND KwotaOperacji = 0
    )
        BEGIN
            SET @Opis = N'BŁĄD: Kwota operacji nie może wynosić 0.';
            INSERT INTO JPKWB_BledyWalidacji (IdJPK, OpisBledu) VALUES (@IdJPK, @Opis);
            SET @ErrorMessage += @Opis + CHAR(13);
        END

    -- 4. DataOperacji w zakresie DataOd–DataDo
    IF EXISTS (
        SELECT 1 FROM JPKWB_Transakcje
        WHERE IdJPK = @IdJPK AND (DataOperacji < @DataOd OR DataOperacji > @DataDo)
    )
        BEGIN
            SET @Opis = FORMATMESSAGE(N'BŁĄD: Transakcje poza zakresem dat [%s – %s].', CONVERT(VARCHAR, @DataOd, 23), CONVERT(VARCHAR, @DataDo, 23));
            INSERT INTO JPKWB_BledyWalidacji (IdJPK, OpisBledu) VALUES (@IdJPK, @Opis);
            SET @ErrorMessage += @Opis + CHAR(13);
        END

    IF LEN(@ErrorMessage) = 0
        PRINT N'Walidacja zakończona poprawnie.';
    ELSE
        PRINT @ErrorMessage;
END

CREATE OR ALTER PROCEDURE spEksportujJPKWB
    @IdJPK INT,
    @XMLResult XML OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- Walidacja danych
    EXEC spWalidujJPKWB @IdJPK;

    -- Jeśli są błędy walidacji, zakończ
    IF EXISTS (SELECT 1 FROM JPKWB_BledyWalidacji WHERE IdJPK = @IdJPK)
        BEGIN
            PRINT N'Nie można wyeksportować XML – dane nie przeszły walidacji.';
            SET @XMLResult = NULL;
            RETURN;
        END
        ;WITH XMLNAMESPACES(
             N'http://jpk.mf.gov.pl/wzor/2016/03/09/03092/' AS tns,
            N'http://crd.gov.pl/xml/schematy/dziedzinowe/mf/2016/01/25/eD/DefinicjeTypy/' AS etd)
    SELECT @XMLResult = (
        SELECT
            -- Nagłówek
            (
                SELECT
                    N'1-0' AS [tns:KodFormularza/@wersjaSchemy],
                    N'JPK_WB (1)' AS [tns:KodFormularza/@kodSystemowy],
                    N'JPK_WB' AS [tns:KodFormularza],
                    N'1' AS [tns:WariantFormularza],
                    dbo.JPK_TXT(CelZlozenia) AS [tns:CelZlozenia],
                    GETDATE() AS [tns:DataWytworzeniaJPK],
                    DataOd AS [tns:DataOd],
                    DataDo AS [tns:DataDo],
                    dbo.JPK_TXT(KodWaluty) AS [tns:DomyslnyKodWaluty],
                    dbo.JPK_TXT(KodUrzedu) AS [tns:KodUrzedu]
                FROM JPKWB_Naglowek
                WHERE IdJPK = @IdJPK
                FOR XML PATH('tns:Naglowek'), TYPE
            ),

            -- Podmiot
            (
                SELECT (
                    SELECT
                        dbo.JPK_TXT(NIP) AS [etd:NIP],
                        dbo.JPK_TXT(PelnaNazwa) AS [etd:PelnaNazwa]
                    FROM JPKWB_Podmiot
                    WHERE IdJPK = @IdJPK
                    FOR XML PATH('tns:IdentyfikatorPodmiotu'), TYPE
                           )
                    ,
                (
                    SELECT
                        N'PL' AS [etd:KodKraju],
                        dbo.JPK_TXT(Wojewodztwo) AS [etd:Wojewodztwo],
                        dbo.JPK_TXT(Powiat) AS [etd:Powiat],
                        dbo.JPK_TXT(Gmina) AS [etd:Gmina],
                        dbo.JPK_TXT(Ulica) AS [etd:Ulica],
                        dbo.JPK_TXT(NrDomu) AS [etd:NrDomu],
                        dbo.JPK_TXT(NrLokalu) AS [etd:NrLokalu],
                        dbo.JPK_TXT(Miejscowosc) AS [etd:Miejscowosc],
                        KodPocztowy AS [etd:KodPocztowy],
                        N'Nieznana' AS [etd:Poczta]
                    FROM JPKWB_Podmiot
                    WHERE IdJPK = @IdJPK
                    FOR XML PATH('tns:AdresPodmiotu'), TYPE
                           )
                FOR XML PATH('tns:Podmiot1'), TYPE
            ),

            -- Rachunek bankowy
            (
                SELECT
                    dbo.JPK_TXT(NumerRachunku)AS [tns:NumerRachunku]
                FROM JPKWB_Rachunek
                WHERE IdJPK = @IdJPK
                FOR XML PATH(''), TYPE),

            -- Salda
            (
                SELECT
                    SaldoPoczatkowe AS [tns:SaldoPoczatkowe],
                    SaldoKoncowe AS [tns:SaldoKoncowe]
                FROM JPKWB_Salda
                WHERE IdJPK = @IdJPK
                FOR XML PATH('tns:Salda'), TYPE
            ),

            -- Wiersze (transakcje)
            (
                SELECT
                    'G' AS [@typ],
                    NumerWiersza AS [tns:NumerWiersza],
                    DataOperacji AS [tns:DataOperacji],
                    dbo.JPK_TXT(NazwaPodmiotu) AS [tns:NazwaPodmiotu],
                    dbo.JPK_TXT(OpisOperacji) AS [tns:OpisOperacji],
                    KwotaOperacji AS [tns:KwotaOperacji],
                    SaldoOperacji AS [tns:SaldoOperacji]
                FROM JPKWB_Transakcje
                WHERE IdJPK = @IdJPK
                ORDER BY NumerWiersza
                FOR XML PATH('tns:WyciagWiersz'), TYPE
            ),

            -- Ctrl
            (
                SELECT
                    LiczbaWierszy AS [tns:LiczbaWierszy],
                    SumaObciazen AS [tns:SumaObciazen],
                    SumaUznan AS [tns:SumaUznan]
                FROM JPKWB_Ctrl
                WHERE IdJPK = @IdJPK
                FOR XML PATH('tns:WyciagCtrl'), TYPE
            )

        FOR XML PATH('tns:JPK'), TYPE
    );
    SELECT @XMLResult AS JPK_XML;
END;

DECLARE @XML XML;
    EXEC spEksportujJPKWB @IdJPK = 1, @XMLResult = @XML OUTPUT;
SELECT @XML AS WynikXML;
