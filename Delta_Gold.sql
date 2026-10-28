-- ============================================================
-- POMBO DATA LAKE - GOLD DELTA
--
-- Origem:
--   Pipeline/Silver/Delta
--
-- Destino:
--   Pipeline/Gold
--
-- Regras:
--   1. A deduplicação já foi realizada pela Silver Current.
--   2. INSERT, UPDATE e DELETE são preservados.
--   3. _operation, _event_id, _batch_id e
--      _ingestion_timestamp seguem para o Gold.
--   4. Os tratamentos, conversões e chaves do Gold original
--      foram mantidos.
--   5. Produto continua limitado a TipoProd = 'CB', porém
--      operações DELETE sempre atravessam para o PostgreSQL.
--
-- Cada Parquet Gold representa o lote Delta da execução atual,
-- e não mais a fotografia completa da entidade.
-- ============================================================

-- 01. CONTAS PAGAR RECEBER
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/contaspagarreceber.parquet',
            union_by_name = true
        )
    ),
    sem_exclusoes AS (
        SELECT *
        FROM atual
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(DtEmissao AS DATE) AS _DataEmissao,
            TRY_CAST(DtVenctoOrig AS DATE) AS _DataVencimentoOriginal,
            TRY_CAST(DtVencto AS DATE) AS _DataVencimento,
            TRY_CAST(DataCancelamento AS DATE) AS _DataCancelamento
        FROM sem_exclusoes
    )
    SELECT
        * EXCLUDE (
            CGC,
            DtEmissao,
            DtVenctoOrig,
            DtVencto,
            DataCancelamento,
            _DataEmissao,
            _DataVencimentoOriginal,
            _DataVencimento,
            _DataCancelamento
        ),
        CGC AS CNPJ,
        _DataEmissao AS DataEmissao,
        _DataVencimentoOriginal AS DataVencimentoOriginal,
        _DataVencimento AS DataVencimento,
        _DataCancelamento AS DataCancelamento,
        CASE WHEN _DataEmissao IS NOT NULL
             THEN CAST(STRFTIME(_DataEmissao, '%Y%m%d') AS INTEGER) END AS DataEmissaoKey,
        CASE WHEN _DataVencimentoOriginal IS NOT NULL
             THEN CAST(STRFTIME(_DataVencimentoOriginal, '%Y%m%d') AS INTEGER) END AS DataVencimentoOriginalKey,
        CASE WHEN _DataVencimento IS NOT NULL
             THEN CAST(STRFTIME(_DataVencimento, '%Y%m%d') AS INTEGER) END AS DataVencimentoKey,
        CASE WHEN _DataCancelamento IS NOT NULL
             THEN CAST(STRFTIME(_DataCancelamento, '%Y%m%d') AS INTEGER) END AS DataCancelamentoKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(NumLancamento AS VARCHAR)) AS CPRKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey,
        CASE WHEN CodEnt IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEnt AS VARCHAR)) END AS EntidadeKey,
        CASE WHEN SeqNF IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(SeqNF AS VARCHAR)) END AS NotaFiscalKey,
        CASE WHEN SeqDocto IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(SeqDocto AS VARCHAR)) END AS TipoDocumentoKey,
        CASE WHEN SeqCC IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(SeqCC AS VARCHAR)) END AS CentroCustoKey,
        CASE WHEN NumLote IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(NumLote AS VARCHAR)) END AS LoteKey
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/contaspagarreceber.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 02. CONTAS PAGAR RECEBER MOEDA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/contaspagarreceberMoeda.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(NumLancamento AS VARCHAR)) AS CPRKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/contaspagarreceberMoeda.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 03. NOTA FISCAL
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/notafiscal.parquet', union_by_name = true)
    ),
    tipada AS (
        SELECT *, TRY_CAST(DataNF AS DATE) AS _DataNotaFiscal
        FROM atual
    )
    SELECT
        * EXCLUDE (DataNF, _DataNotaFiscal),
        _DataNotaFiscal AS DataNotaFiscal,
        CASE WHEN _DataNotaFiscal IS NOT NULL
             THEN CAST(STRFTIME(_DataNotaFiscal, '%Y%m%d') AS INTEGER) END AS DataNotaFiscalKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(Sequencial AS VARCHAR)) AS NotaFiscalKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey,
        CASE WHEN CodEnt IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEnt AS VARCHAR)) END AS EntidadeKey,
        CASE WHEN CodNatOp IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodNatOp AS VARCHAR)) END AS NaturezaOperacaoKey,
        CASE WHEN NumLote IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(NumLote AS VARCHAR)) END AS LoteKey
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/notafiscal.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 04. ITENS NF
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/itensnf.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(Sequencial AS VARCHAR)) AS NotaFiscalKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(Sequencial AS VARCHAR), CAST(Item AS VARCHAR)) AS ItemNotaFiscalKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey,
        CASE WHEN CodProd IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodProd AS VARCHAR)) END AS ProdutoKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/itensnf.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 05. MOVIMENTO PLANO CONTAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/MovimentoPlanoContas.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(NumLote AS VARCHAR)) AS LoteKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodConta AS VARCHAR)) AS PlanoContaKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/MovimentoPlanoContas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 06. CENTRO CUSTO RATEIO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/CentroCustoRateio.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(NumLote AS VARCHAR)) AS LoteKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(SeqCC AS VARCHAR)) AS CentroCustoKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/CentroCustoRateio.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 07. CENTRO CUSTO PAGAR
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/CentroCustoPagar.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(NumLancamento AS VARCHAR)) AS CPRKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(SeqCC AS VARCHAR)) AS CentroCustoKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/CentroCustoPagar.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 08. Z NOTA FISCAL CLASSIFICAÇÃO DESPESAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/Z_NotaFiscal_ClassificacaoDespesas.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR), CAST(SequencialNF AS VARCHAR)) AS NotaFiscalKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(ID_ClassificacaoDespesas AS VARCHAR)) AS ClassificacaoDespesaKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/Z_NotaFiscal_ClassificacaoDespesas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 09. EMPRESAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/empresas.parquet', union_by_name = true)
    )
    SELECT
        * EXCLUDE (CGC),
        CGC AS CNPJ,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEmpresa AS VARCHAR)) AS EmpresaKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/empresas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 10. ENTIDADE
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/Entidade.parquet', union_by_name = true)
    ),
    tipada AS (
        SELECT *, TRY_CAST(DataAlteracaoCad AS DATE) AS _DataAlteracaoCadastro
        FROM atual
    )
    SELECT
        * EXCLUDE (CGC, DataAlteracaoCad, _DataAlteracaoCadastro),
        CGC AS CNPJ,
        _DataAlteracaoCadastro AS DataAlteracaoCadastro,
        CASE WHEN _DataAlteracaoCadastro IS NOT NULL
             THEN CAST(STRFTIME(_DataAlteracaoCadastro, '%Y%m%d') AS INTEGER) END AS DataAlteracaoCadastroKey,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodEnt AS VARCHAR)) AS EntidadeKey,
        CASE WHEN Vendedor IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(Vendedor AS VARCHAR)) END AS VendedorKey
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/Entidade.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 11. VENDEDOR
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/Vendedor.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodVend AS VARCHAR)) AS VendedorKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/Vendedor.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 12. TIPO DOCUMENTO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/TipoDocumento.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(SeqDocto AS VARCHAR)) AS TipoDocumentoKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/TipoDocumento.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 13. NATUREZA OPERAÇÃO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/NaturezaOperacao.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodNatOp AS VARCHAR)) AS NaturezaOperacaoKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/NaturezaOperacao.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 14. Z CLASSIFICAÇÃO DESPESAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/z_classificacaodespesas.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(ID AS VARCHAR)) AS ClassificacaoDespesaKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/z_classificacaodespesas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 15. PLANO CONTAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/planocontas.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodConta AS VARCHAR)) AS PlanoContaKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/planocontas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 16. PORTADOR
-- Execute depois de gerar Silver/Portador.parquet.
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/Portador.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(CodPort AS VARCHAR)) AS PortadorKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/Portador.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 17. CENTRO CUSTO
-- Execute depois de gerar Silver/CentroCusto.parquet.
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Delta/CentroCusto.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(SeqCC AS VARCHAR)) AS CentroCustoKey
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/CentroCusto.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 18. PEDIDO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/Pedido.parquet',
            union_by_name = true
        )
    ),
    sem_exclusoes AS (
        SELECT *
        FROM atual
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(DataPed AS DATE) AS _DataPed,
            TRY_CAST(DtPrevFat AS DATE) AS _DtPrevFat,
            TRY_CAST(LibFinancEm AS DATE) AS _LibFinancEm,
            TRY_CAST(LibVendasEm AS DATE) AS _LibVendasEm,
            TRY_CAST(DataCancelamento AS DATE) AS _DataCancelamento,
            TRY_CAST(AprovTranspEm AS DATE) AS _AprovTranspEm,
            TRY_CAST(CR_DataPreCredito AS DATE) AS _CR_DataPreCredito,
            TRY_CAST(DataMovto AS DATE) AS _DataMovto,
            TRY_CAST(ServicoAnaliseAprovacaoVDData AS DATE) AS _ServicoAnaliseAprovacaoVDData,
            TRY_CAST(DataInicioExecucao AS DATE) AS _DataInicioExecucao,
            TRY_CAST(DataFimExecucao AS DATE) AS _DataFimExecucao
        FROM sem_exclusoes
    )
    SELECT
        * EXCLUDE (
            DataPed,
            DtPrevFat,
            LibFinancEm,
            LibVendasEm,
            DataCancelamento,
            AprovTranspEm,
            CR_DataPreCredito,
            DataMovto,
            ServicoAnaliseAprovacaoVDData,
            DataInicioExecucao,
            DataFimExecucao,
            RazaoSocial,
            descrTransp,
            RzSocialEnt,
            EnderecoCobranca,
            EnderecoEntrega,
            EnderecoFat,
            ComplementoEntrega,
            ComplementoCobranca,
            ComplementoFat,
            BairroCobranca,
            BairroEntrega,
            BairroFat,
            _DataPed,
            _DtPrevFat,
            _LibFinancEm,
            _LibVendasEm,
            _DataCancelamento,
            _AprovTranspEm,
            _CR_DataPreCredito,
            _DataMovto,
            _ServicoAnaliseAprovacaoVDData,
            _DataInicioExecucao,
            _DataFimExecucao
        ),

        _DataPed AS DataPed,
        _DtPrevFat AS DtPrevFat,
        _LibFinancEm AS LibFinancEm,
        _LibVendasEm AS LibVendasEm,
        _DataCancelamento AS DataCancelamento,
        _AprovTranspEm AS AprovTranspEm,
        _CR_DataPreCredito AS CR_DataPreCredito,
        _DataMovto AS DataMovto,
        _ServicoAnaliseAprovacaoVDData AS ServicoAnaliseAprovacaoVDData,
        _DataInicioExecucao AS DataInicioExecucao,
        _DataFimExecucao AS DataFimExecucao,

        CASE WHEN _DataPed IS NOT NULL
             THEN CAST(STRFTIME(_DataPed, '%Y%m%d') AS INTEGER) END AS DataPedKey,
        CASE WHEN _DtPrevFat IS NOT NULL
             THEN CAST(STRFTIME(_DtPrevFat, '%Y%m%d') AS INTEGER) END AS DtPrevFatKey,
        CASE WHEN _LibFinancEm IS NOT NULL
             THEN CAST(STRFTIME(_LibFinancEm, '%Y%m%d') AS INTEGER) END AS LibFinancEmKey,
        CASE WHEN _LibVendasEm IS NOT NULL
             THEN CAST(STRFTIME(_LibVendasEm, '%Y%m%d') AS INTEGER) END AS LibVendasEmKey,
        CASE WHEN _DataCancelamento IS NOT NULL
             THEN CAST(STRFTIME(_DataCancelamento, '%Y%m%d') AS INTEGER) END AS DataCancelamentoKey,
        CASE WHEN _AprovTranspEm IS NOT NULL
             THEN CAST(STRFTIME(_AprovTranspEm, '%Y%m%d') AS INTEGER) END AS AprovTranspEmKey,
        CASE WHEN _CR_DataPreCredito IS NOT NULL
             THEN CAST(STRFTIME(_CR_DataPreCredito, '%Y%m%d') AS INTEGER) END AS CR_DataPreCreditoKey,
        CASE WHEN _DataMovto IS NOT NULL
             THEN CAST(STRFTIME(_DataMovto, '%Y%m%d') AS INTEGER) END AS DataMovtoKey,
        CASE WHEN _ServicoAnaliseAprovacaoVDData IS NOT NULL
             THEN CAST(STRFTIME(_ServicoAnaliseAprovacaoVDData, '%Y%m%d') AS INTEGER) END AS ServicoAnaliseAprovacaoVDDataKey,
        CASE WHEN _DataInicioExecucao IS NOT NULL
             THEN CAST(STRFTIME(_DataInicioExecucao, '%Y%m%d') AS INTEGER) END AS DataInicioExecucaoKey,
        CASE WHEN _DataFimExecucao IS NOT NULL
             THEN CAST(STRFTIME(_DataFimExecucao, '%Y%m%d') AS INTEGER) END AS DataFimExecucaoKey,

        UPPER(STRIP_ACCENTS(TRIM(RazaoSocial))) AS RazaoSocial,
        UPPER(STRIP_ACCENTS(TRIM(descrTransp))) AS descrTransp,
        UPPER(STRIP_ACCENTS(TRIM(RzSocialEnt))) AS RzSocialEnt,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(EnderecoCobranca), '(^|\\s)AV\\.?\\s+', '\\1AVENIDA ', 'gi'),
                    '(^|\\s)R\\.?\\s+', '\\1RUA ', 'gi'
                ),
                '(^|\\s)ROD\\.?\\s+', '\\1RODOVIA ', 'gi'
            )
        )) AS EnderecoCobranca,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(EnderecoEntrega), '(^|\\s)AV\\.?\\s+', '\\1AVENIDA ', 'gi'),
                    '(^|\\s)R\\.?\\s+', '\\1RUA ', 'gi'
                ),
                '(^|\\s)ROD\\.?\\s+', '\\1RODOVIA ', 'gi'
            )
        )) AS EnderecoEntrega,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(EnderecoFat), '(^|\\s)AV\\.?\\s+', '\\1AVENIDA ', 'gi'),
                    '(^|\\s)R\\.?\\s+', '\\1RUA ', 'gi'
                ),
                '(^|\\s)ROD\\.?\\s+', '\\1RODOVIA ', 'gi'
            )
        )) AS EnderecoFat,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(TRIM(ComplementoEntrega), '\\bS\\s*/?\\s*N\\.?\\b', 'SN.', 'gi')
        )) AS ComplementoEntrega,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(TRIM(ComplementoCobranca), '\\bS\\s*/?\\s*N\\.?\\b', 'SN.', 'gi')
        )) AS ComplementoCobranca,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(TRIM(ComplementoFat), '\\bS\\s*/?\\s*N\\.?\\b', 'SN.', 'gi')
        )) AS ComplementoFat,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(BairroCobranca), '(^|\\s)JD\\.?\\s*', '\\1JARDIM ', 'gi'),
                '(^|\\s)VL\\.?\\s*', '\\1VILA ', 'gi'
            )
        )) AS BairroCobranca,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(BairroEntrega), '(^|\\s)JD\\.?\\s*', '\\1JARDIM ', 'gi'),
                '(^|\\s)VL\\.?\\s*', '\\1VILA ', 'gi'
            )
        )) AS BairroEntrega,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(BairroFat), '(^|\\s)JD\\.?\\s*', '\\1JARDIM ', 'gi'),
                '(^|\\s)VL\\.?\\s*', '\\1VILA ', 'gi'
            )
        )) AS BairroFat,

        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(CodEmpresa AS VARCHAR),
            CAST(NumPed AS VARCHAR)
        ) AS PedidoKey
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/Pedido.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 19. ITENS PEDIDO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/ItensPedido.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(DtPrevEntrega AS DATE) AS _DtPrevEntrega
        FROM atual
    )
    SELECT
        * EXCLUDE (
            DtPrevEntrega,
            _DtPrevEntrega
        ),
        _DtPrevEntrega AS DtPrevEntrega,
        CASE WHEN _DtPrevEntrega IS NOT NULL
             THEN CAST(STRFTIME(_DtPrevEntrega, '%Y%m%d') AS INTEGER) END AS DtPrevEntregaKey,
        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(CodEmpresa AS VARCHAR),
            CAST(NumPed AS VARCHAR)
        ) AS PedidoKey
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/ItensPedido.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 20. CIDADE
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/Cidade.parquet',
            union_by_name = true
        )
    )
    SELECT
        * EXCLUDE (
            Descricao
        ),
        UPPER(STRIP_ACCENTS(TRIM(Descricao))) AS Descricao
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/Cidade.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 21. BANDEIRA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/Bandeira.parquet',
            union_by_name = true
        )
    )
    SELECT
        * EXCLUDE (
            descricao
        ),
        UPPER(STRIP_ACCENTS(TRIM(descricao))) AS descricao
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/Bandeira.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 22. BAIXA ANTECIPADA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/BaixaAntecipada.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(Data AS DATE) AS _Data,
            TRY_CAST(DataCheque AS DATE) AS _DataCheque,
            TRY_CAST(DataFluxo AS DATE) AS _DataFluxo,
            TRY_CAST(dataDesbloqueio AS DATE) AS _dataDesbloqueio,
            TRY_CAST(DesbloqueadoEm AS DATE) AS _DesbloqueadoEm,
            TRY_CAST(PMM_PDAjuste_Data AS DATE) AS _PMM_PDAjuste_Data,
            TRY_CAST(DataCancelamento AS DATE) AS _DataCancelamento,
            TRY_CAST(DataSimples AS DATE) AS _DataSimples,
            TRY_CAST(DataInclusao AS TIMESTAMP) AS _DataInclusao
        FROM atual
    )
    SELECT
        * EXCLUDE (
            Data,
            DataCheque,
            DataFluxo,
            dataDesbloqueio,
            DesbloqueadoEm,
            PMM_PDAjuste_Data,
            DataCancelamento,
            DataSimples,
            DataInclusao,
            RzSocial,
            rzSocialComprador,
            _Data,
            _DataCheque,
            _DataFluxo,
            _dataDesbloqueio,
            _DesbloqueadoEm,
            _PMM_PDAjuste_Data,
            _DataCancelamento,
            _DataSimples,
            _DataInclusao
        ),

        _Data AS Data,
        _DataCheque AS DataCheque,
        _DataFluxo AS DataFluxo,
        _dataDesbloqueio AS dataDesbloqueio,
        _DesbloqueadoEm AS DesbloqueadoEm,
        _PMM_PDAjuste_Data AS PMM_PDAjuste_Data,
        _DataCancelamento AS DataCancelamento,
        _DataSimples AS DataSimples,
        _DataInclusao AS DataInclusao,

        CASE WHEN _Data IS NOT NULL
             THEN CAST(STRFTIME(_Data, '%Y%m%d') AS INTEGER) END AS DataKey,
        CASE WHEN _DataCheque IS NOT NULL
             THEN CAST(STRFTIME(_DataCheque, '%Y%m%d') AS INTEGER) END AS DataChequeKey,
        CASE WHEN _DataFluxo IS NOT NULL
             THEN CAST(STRFTIME(_DataFluxo, '%Y%m%d') AS INTEGER) END AS DataFluxoKey,
        CASE WHEN _dataDesbloqueio IS NOT NULL
             THEN CAST(STRFTIME(_dataDesbloqueio, '%Y%m%d') AS INTEGER) END AS dataDesbloqueioKey,
        CASE WHEN _DesbloqueadoEm IS NOT NULL
             THEN CAST(STRFTIME(_DesbloqueadoEm, '%Y%m%d') AS INTEGER) END AS DesbloqueadoEmKey,
        CASE WHEN _PMM_PDAjuste_Data IS NOT NULL
             THEN CAST(STRFTIME(_PMM_PDAjuste_Data, '%Y%m%d') AS INTEGER) END AS PMM_PDAjuste_DataKey,
        CASE WHEN _DataCancelamento IS NOT NULL
             THEN CAST(STRFTIME(_DataCancelamento, '%Y%m%d') AS INTEGER) END AS DataCancelamentoKey,
        CASE WHEN _DataSimples IS NOT NULL
             THEN CAST(STRFTIME(_DataSimples, '%Y%m%d') AS INTEGER) END AS DataSimplesKey,
        CASE WHEN _DataInclusao IS NOT NULL
             THEN CAST(STRFTIME(CAST(_DataInclusao AS DATE), '%Y%m%d') AS INTEGER) END AS DataInclusaoKey,

        UPPER(STRIP_ACCENTS(TRIM(RzSocial))) AS RzSocial,
        UPPER(STRIP_ACCENTS(TRIM(rzSocialComprador))) AS rzSocialComprador,

        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(CodEmpresa AS VARCHAR),
            CAST(SeqCredito AS VARCHAR)
        ) AS BaixaAntecipadaKey
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/BaixaAntecipada.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 23. ITENS BAIXA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/ItensBaixa.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(DtBaixa AS DATE) AS _DtBaixa
        FROM atual
    )
    SELECT
        * EXCLUDE (
            DtBaixa,
            _DtBaixa
        ),
        _DtBaixa AS DtBaixa,
        CASE WHEN _DtBaixa IS NOT NULL
             THEN CAST(STRFTIME(_DtBaixa, '%Y%m%d') AS INTEGER) END AS DtBaixaKey,
        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(CodEmpresa AS VARCHAR),
            CAST(NumBaixa AS VARCHAR)
        ) AS BaixaKey
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/ItensBaixa.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 24. BAIXA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/Baixa.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(DtBaixa AS DATE) AS _DtBaixa,
            TRY_CAST(DataCheque AS DATE) AS _DataCheque,
            TRY_CAST(DataFluxo AS DATE) AS _DataFluxo,
            TRY_CAST(DataProgDesbloqueio AS DATE) AS _DataProgDesbloqueio,
            TRY_CAST(DataDesbloqueio AS DATE) AS _DataDesbloqueio,
            TRY_CAST(DataInclusaoBaixa AS TIMESTAMP) AS _DataInclusaoBaixa,
            TRY_CAST(DataCancelamento AS DATE) AS _DataCancelamento,
            TRY_CAST(DataCanctoUsu AS DATE) AS _DataCanctoUsu
        FROM atual
    )
    SELECT
        * EXCLUDE (
            DtBaixa,
            DataCheque,
            DataFluxo,
            DataProgDesbloqueio,
            DataDesbloqueio,
            DataInclusaoBaixa,
            DataCancelamento,
            DataCanctoUsu,
            _DtBaixa,
            _DataCheque,
            _DataFluxo,
            _DataProgDesbloqueio,
            _DataDesbloqueio,
            _DataInclusaoBaixa,
            _DataCancelamento,
            _DataCanctoUsu
        ),

        _DtBaixa AS DtBaixa,
        _DataCheque AS DataCheque,
        _DataFluxo AS DataFluxo,
        _DataProgDesbloqueio AS DataProgDesbloqueio,
        _DataDesbloqueio AS DataDesbloqueio,
        _DataInclusaoBaixa AS DataInclusaoBaixa,
        _DataCancelamento AS DataCancelamento,
        _DataCanctoUsu AS DataCanctoUsu,

        CASE WHEN _DtBaixa IS NOT NULL
             THEN CAST(STRFTIME(_DtBaixa, '%Y%m%d') AS INTEGER) END AS DtBaixaKey,
        CASE WHEN _DataCheque IS NOT NULL
             THEN CAST(STRFTIME(_DataCheque, '%Y%m%d') AS INTEGER) END AS DataChequeKey,
        CASE WHEN _DataFluxo IS NOT NULL
             THEN CAST(STRFTIME(_DataFluxo, '%Y%m%d') AS INTEGER) END AS DataFluxoKey,
        CASE WHEN _DataProgDesbloqueio IS NOT NULL
             THEN CAST(STRFTIME(_DataProgDesbloqueio, '%Y%m%d') AS INTEGER) END AS DataProgDesbloqueioKey,
        CASE WHEN _DataDesbloqueio IS NOT NULL
             THEN CAST(STRFTIME(_DataDesbloqueio, '%Y%m%d') AS INTEGER) END AS DataDesbloqueioKey,
        CASE WHEN _DataInclusaoBaixa IS NOT NULL
             THEN CAST(STRFTIME(CAST(_DataInclusaoBaixa AS DATE), '%Y%m%d') AS INTEGER) END AS DataInclusaoBaixaKey,
        CASE WHEN _DataCancelamento IS NOT NULL
             THEN CAST(STRFTIME(_DataCancelamento, '%Y%m%d') AS INTEGER) END AS DataCancelamentoKey,
        CASE WHEN _DataCanctoUsu IS NOT NULL
             THEN CAST(STRFTIME(_DataCanctoUsu, '%Y%m%d') AS INTEGER) END AS DataCanctoUsuKey,

        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(CodEmpresa AS VARCHAR),
            CAST(NumBaixa AS VARCHAR)
        ) AS BaixaKey
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/Baixa.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 25. PRODUTO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/Produto.parquet',
            union_by_name = true
        )
    )
    SELECT
        * EXCLUDE (
            DescricaoCompleta,
            DescricaoNF,
            DescricaoReduzida,
            ANP_Descricao
        ),

        UPPER(STRIP_ACCENTS(TRIM(DescricaoCompleta))) AS DescricaoCompleta,
        UPPER(STRIP_ACCENTS(TRIM(DescricaoNF))) AS DescricaoNF,
        UPPER(STRIP_ACCENTS(TRIM(DescricaoReduzida))) AS DescricaoReduzida,
        UPPER(STRIP_ACCENTS(TRIM(ANP_Descricao))) AS ANP_Descricao
    FROM atual
    WHERE (
        UPPER(COALESCE(_operation, '')) IN ('DELETE', 'DELETED', 'D')
        OR UPPER(TRIM(TipoProd)) = 'CB'
    )
)
TO '//10.68.152.218/d$/Pipeline/Gold/Produto.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 26. DEPOSITO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Delta/Deposito.parquet',
            union_by_name = true
        )
    ),
    tratado AS (
        SELECT
            * EXCLUDE (
            Descricao,
            RzSocial,
            endereco,
            Complemento,
            Bairro,
            Telefone, codprod
        ),

            UPPER(STRIP_ACCENTS(TRIM(Descricao))) AS Descricao,
            UPPER(STRIP_ACCENTS(TRIM(RzSocial))) AS RzSocial,

            UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(endereco),
                            '(^|\\s)AV\\.?\\s+',
                            '\\1AVENIDA ',
                            'gi'
                        ),
                        '(^|\\s)R\\.?\\s+',
                        '\\1RUA ',
                        'gi'
                    ),
                    '(^|\\s)ROD\\.?\\s+',
                    '\\1RODOVIA ',
                    'gi'
                )
            )) AS endereco,

            UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    TRIM(Complemento),
                    '\\bS\\s*/?\\s*N\\.?\\b',
                    'SN.',
                    'gi'
                )
            )) AS Complemento,

            UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        TRIM(Bairro),
                        '(^|\\s)JD\\.?\\s*',
                        '\\1JARDIM ',
                        'gi'
                    ),
                    '(^|\\s)VL\\.?\\s*',
                    '\\1VILA ',
                    'gi'
                )
            )) AS Bairro

        FROM atual
    )

    SELECT
        *
    FROM tratado
)
TO '//10.68.152.218/d$/Pipeline/Gold/Deposito.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);