-- POMBO DATA LAKE - GOLD CURRENT (SNAKE_CASE)
--
-- Origem:
--   Pipeline/Silver/Current
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
--   6. Complementos equivalentes a S/N são normalizados para SN.
--   7. CEPs válidos com 8 dígitos são formatados como 00000-000.
--   8. Telefones válidos são formatados com DDD.
--   9. Endereços expandem EST/ROD/AV/R para ESTRADA/RODOVIA/AVENIDA/RUA.
--  10. Complementos expandem SL/SALA para SALA e números 1-9 recebem zero.
--  11. Bairros expandem DIST/PQ/JD/VL para DISTRITO/PARQUE/JARDIM/VILA.
--  12. CNPJ/CGC válidos com 14 dígitos são formatados.
--  13. IE e IM permanecem sem formatação.
--
-- Cada Parquet Gold representa a fotografia completa da entidade
-- reconstruída pela Silver Current.
-- ============================================================

-- 01. CONTAS PAGAR RECEBER
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/contas_pagar_receber.parquet',
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
            TRY_CAST(dt_emissao AS DATE) AS _data_emissao,
            TRY_CAST(dt_vencto_orig AS DATE) AS _data_vencimento_original,
            TRY_CAST(dt_vencto AS DATE) AS _data_vencimento,
            TRY_CAST(data_cancelamento AS DATE) AS _data_cancelamento,
            TRY_CAST(data_pagamento AS DATE) AS _data_pagamento,
            TRY_CAST(data_hora_atualizacao AS TIMESTAMPTZ) AS _data_hora_atualizacao
        FROM sem_exclusoes
    )
    SELECT
        * EXCLUDE (
            cgc,
            dt_emissao,
            dt_vencto_orig,
            dt_vencto,
            data_cancelamento,
            _data_emissao,
            _data_vencimento_original,
            _data_vencimento,
            _data_cancelamento,
            data_pagamento,
            data_hora_atualizacao,
            _data_pagamento,
            _data_hora_atualizacao
        ),
        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g')) = 14
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 3)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
                     || '/'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 9, 4)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 13, 2)
            ELSE NULLIF(TRIM(CAST(cgc AS VARCHAR)), '')
        END AS cnpj,
        _data_emissao AS data_emissao,
        _data_vencimento_original AS data_vencimento_original,
        _data_vencimento AS data_vencimento,
        _data_cancelamento AS data_cancelamento,
        _data_pagamento AS data_pagamento,
        _data_hora_atualizacao AS data_hora_atualizacao,
        CASE WHEN _data_emissao IS NOT NULL
             THEN CAST(STRFTIME(_data_emissao, '%Y%m%d') AS INTEGER) END AS data_emissao_key,
        CASE WHEN _data_vencimento_original IS NOT NULL
             THEN CAST(STRFTIME(_data_vencimento_original, '%Y%m%d') AS INTEGER) END AS data_vencimento_original_key,
        CASE WHEN _data_vencimento IS NOT NULL
             THEN CAST(STRFTIME(_data_vencimento, '%Y%m%d') AS INTEGER) END AS data_vencimento_key,
        CASE WHEN _data_cancelamento IS NOT NULL
             THEN CAST(STRFTIME(_data_cancelamento, '%Y%m%d') AS INTEGER) END AS data_cancelamento_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(num_lancamento AS VARCHAR)) AS cpr_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR)) AS empresa_key,
        CASE WHEN cod_ent IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_ent AS VARCHAR)) END AS entidade_key,
        CASE WHEN seq_nf IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(seq_nf AS VARCHAR)) END AS nota_fiscal_key,
        CASE WHEN seq_docto IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(seq_docto AS VARCHAR)) END AS tipo_documento_key,
        CASE WHEN seq_cc IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(seq_cc AS VARCHAR)) END AS centro_custo_key,
        CASE WHEN num_lote IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(num_lote AS VARCHAR)) END AS lote_key,
        UPPER(
            CONCAT_WS(
                '-',
                TRIM(CAST(nome_alias AS VARCHAR)),
                TRIM(CAST(cod_tp_cobr AS VARCHAR))
            )
        ) AS cod_tp_cobr_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/contas_pagar_receber.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 02. CONTAS PAGAR RECEBER MOEDA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/contas_pagar_receber_moeda.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(num_lancamento AS VARCHAR)) AS cpr_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR)) AS empresa_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/contas_pagar_receber_moeda.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 03. NOTA FISCAL
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/nota_fiscal.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_nf AS DATE) AS _data_nota_fiscal,
            TRY_CAST(data_movto AS DATE) AS _data_movto,
            TRY_CAST(val_prod AS DECIMAL(18,2)) AS _val_prod,
            TRY_CAST(base_icms AS DECIMAL(18,2)) AS _base_icms,
            TRY_CAST(val_icms AS DECIMAL(18,2)) AS _val_icms,
            TRY_CAST(base_icmssubst AS DECIMAL(18,2)) AS _base_icmssubst,
            TRY_CAST(icmssubst AS DECIMAL(18,2)) AS _icmssubst,
            TRY_CAST(val_ipi AS DECIMAL(18,2)) AS _val_ipi,
            TRY_CAST(val_frete AS DECIMAL(18,2)) AS _val_frete,
            TRY_CAST(val_desconto AS DECIMAL(18,2)) AS _val_desconto,
            TRY_CAST(val_seguro AS DECIMAL(18,2)) AS _val_seguro,
            TRY_CAST(val_desp_acess AS DECIMAL(18,2)) AS _val_desp_acess
        FROM atual
    )
    SELECT
        * EXCLUDE (
            data_nf,
            data_movto,
            val_prod,
            base_icms,
            val_icms,
            base_icmssubst,
            icmssubst,
            val_ipi,
            val_frete,
            val_desconto,
            val_seguro,
            val_desp_acess,
            _data_nota_fiscal,
            _data_movto,
            _val_prod,
            _base_icms,
            _val_icms,
            _base_icmssubst,
            _icmssubst,
            _val_ipi,
            _val_frete,
            _val_desconto,
            _val_seguro,
            _val_desp_acess
        ),
        _data_nota_fiscal AS data_nota_fiscal,
        _data_movto AS data_movto,
        _val_prod AS val_prod,
        _base_icms AS base_icms,
        _val_icms AS val_icms,
        _base_icmssubst AS base_icmssubst,
        _icmssubst AS icmssubst,
        _val_ipi AS val_ipi,
        _val_frete AS val_frete,
        _val_desconto AS val_desconto,
        _val_seguro AS val_seguro,
        _val_desp_acess AS val_desp_acess,
        CASE WHEN _data_nota_fiscal IS NOT NULL
             THEN CAST(STRFTIME(_data_nota_fiscal, '%Y%m%d') AS INTEGER) END AS data_nota_fiscal_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(sequencial AS VARCHAR)) AS nota_fiscal_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR)) AS empresa_key,
        CASE WHEN cod_ent IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_ent AS VARCHAR)) END AS entidade_key,
        CASE WHEN cod_nat_op IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_nat_op AS VARCHAR)) END AS natureza_operacao_key,
        CASE WHEN num_lote IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(num_lote AS VARCHAR)) END AS lote_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/nota_fiscal.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 04. ITENS NF
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/itens_nf.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(sequencial AS VARCHAR)) AS nota_fiscal_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(sequencial AS VARCHAR), CAST(item AS VARCHAR)) AS item_nota_fiscal_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR)) AS empresa_key,
        CASE WHEN cod_prod IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_prod AS VARCHAR)) END AS produto_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/itens_nf.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 05. MOVIMENTO PLANO CONTAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/movimento_plano_contas.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(num_lote AS VARCHAR)) AS lote_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_conta AS VARCHAR)) AS plano_conta_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR)) AS empresa_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/movimento_plano_contas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 06. CENTRO CUSTO RATEIO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/centro_custo_rateio.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(num_lote AS VARCHAR)) AS lote_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(seq_cc AS VARCHAR)) AS centro_custo_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR)) AS empresa_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/centro_custo_rateio.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 07. CENTRO CUSTO PAGAR
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/centro_custo_pagar.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(num_lancamento AS VARCHAR)) AS cpr_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(seq_cc AS VARCHAR)) AS centro_custo_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR)) AS empresa_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/centro_custo_pagar.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 08. Z NOTA FISCAL CLASSIFICAÇÃO DESPESAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/z_nota_fiscal_classificacao_despesas.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR), CAST(sequencial_nf AS VARCHAR)) AS nota_fiscal_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(id_classificacao_despesas AS VARCHAR)) AS classificacao_despesa_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_empresa AS VARCHAR)) AS empresa_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/z_nota_fiscal_classificacao_despesas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 09. EMPRESAS
COPY
(
    WITH atual AS
    (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/empresas.parquet',
            union_by_name = true
        )
    ),
    tratado AS
    (
        SELECT
            atual.*,
            REGEXP_REPLACE(
                COALESCE(CAST(cgc AS VARCHAR), ''),
                '[^0-9]', '', 'g'
            ) AS cnpj_limpo
        FROM atual
    )
    SELECT
        * EXCLUDE (
            cgc,
            nome_usual,
            razao_social,
            endereco,
            bairro,
            numero,
            cep,
            cidade,
            uf,
            empresa_codigo_externo,
            sigla,
            tipo_imposto,
            ie,
            cnae,
            cnpj_limpo,
            im),

        CASE
            WHEN LENGTH(cnpj_limpo) = 14
                THEN SUBSTR(cnpj_limpo, 1, 2) || '.'
                     || SUBSTR(cnpj_limpo, 3, 3) || '.'
                     || SUBSTR(cnpj_limpo, 6, 3) || '/'
                     || SUBSTR(cnpj_limpo, 9, 4) || '-'
                     || SUBSTR(cnpj_limpo, 13, 2)
            ELSE NULLIF(TRIM(CAST(cgc AS VARCHAR)), '')
        END AS cnpj,

        UPPER(STRIP_ACCENTS(
            CASE cnpj_limpo
                WHEN '10775497000254' THEN 'Flagler RJ'
                WHEN '10775497000416' THEN 'Flagler SP'
                WHEN '05068412000500' THEN 'Rodo CN'
                WHEN '69209575000115' THEN 'Fera RJ'
                WHEN '14498445000239' THEN 'BCAG SP 02'
                WHEN '14498445000158' THEN 'BCAG SP 01'
                WHEN '69209575000387' THEN 'Fera SP'
                WHEN '11989750000154' THEN '76 OIL'
                ELSE
                    CASE
                        WHEN TRIM(COALESCE(CAST(nome_usual AS VARCHAR), '')) LIKE '76%'
                            THEN TRIM(COALESCE(CAST(nome_usual AS VARCHAR), ''))
                        ELSE TRIM(
                            REGEXP_REPLACE(
                                TRIM(COALESCE(CAST(nome_usual AS VARCHAR), '')),
                                '^[0-9]+[[:space:]-]*',
                                ''
                            )
                        )
                    END
            END
        )) AS nome_usual,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                TRIM(CAST(razao_social AS VARCHAR)),
                '[[:space:]]+',
                ' ',
                'g'
            )
        )) AS razao_social,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(endereco AS VARCHAR)),
                            '(^|\\s)EST\\.?\\s+',
                            '\\1ESTRADA ',
                            'gi'
                        ),
                        '(^|\\s)ROD\\.?\\s+',
                        '\\1RODOVIA ',
                        'gi'
                    ),
                    '(^|\\s)AV\\.?\\s+',
                    '\\1AVENIDA ',
                    'gi'
                ),
                '(^|\\s)R\\.?\\s+',
                '\\1RUA ',
                'gi'
            )
        )) AS endereco,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(bairro AS VARCHAR)),
                            '(^|\\s)DIST\\.?\\s*',
                            '\\1DISTRITO ',
                            'gi'
                        ),
                        '(^|\\s)PQ\\.?\\s*',
                        '\\1PARQUE ',
                        'gi'
                    ),
                    '(^|\\s)JD\\.?\\s*',
                    '\\1JARDIM ',
                    'gi'
                ),
                '(^|\\s)VL\\.?\\s*',
                '\\1VILA ',
                'gi'
            )
        )) AS bairro,

        CASE
            WHEN NULLIF(TRIM(CAST(numero AS VARCHAR)), '') IS NULL THEN NULL
            WHEN REGEXP_REPLACE(
                    UPPER(STRIP_ACCENTS(TRIM(CAST(numero AS VARCHAR)))),
                    '[^A-Z0-9]', '', 'g'
                 ) IN ('SN', 'SEMNUMERO')
                THEN 'SN'
            ELSE UPPER(STRIP_ACCENTS(TRIM(CAST(numero AS VARCHAR))))
        END AS numero,

        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g')) = 8
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 5)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
            ELSE NULLIF(TRIM(CAST(cep AS VARCHAR)), '')
        END AS cep,

        UPPER(STRIP_ACCENTS(TRIM(CAST(cidade AS VARCHAR)))) AS cidade,
        UPPER(STRIP_ACCENTS(TRIM(CAST(uf AS VARCHAR)))) AS uf,
        UPPER(STRIP_ACCENTS(TRIM(CAST(empresa_codigo_externo AS VARCHAR)))) AS empresa_codigo_externo,
        UPPER(STRIP_ACCENTS(TRIM(CAST(sigla AS VARCHAR)))) AS sigla,
        UPPER(STRIP_ACCENTS(TRIM(CAST(tipo_imposto AS VARCHAR)))) AS tipo_imposto,
        UPPER(STRIP_ACCENTS(TRIM(CAST(ie AS VARCHAR)))) AS ie,
        UPPER(STRIP_ACCENTS(TRIM(CAST(cnae AS VARCHAR)))) AS cnae,
        UPPER(STRIP_ACCENTS(TRIM(CAST(im AS VARCHAR)))) AS inscricao_municipal,

        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(cod_empresa AS VARCHAR)
        ) AS empresa_key

    FROM tratado
)
TO '//10.68.152.218/d$/Pipeline/Gold/empresas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);


-- 10. ENTIDADE
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/entidade.parquet', union_by_name = true)
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_alteracao_cad AS DATE) AS _data_alteracao_cadastro,
            TRY_CAST(data_cadastro AS DATE) AS _data_cadastro,
            TRY_CAST(data_hora_atualizacao AS TIMESTAMPTZ) AS _data_hora_atualizacao
        FROM atual
    )
    SELECT
        * EXCLUDE (
            cgc,
            cnpj_pagamento,
            data_alteracao_cad,
            data_cadastro,
            data_hora_atualizacao,
            endereco,
            bairro,
            cep,
            fone,
            complemento,
            _data_alteracao_cadastro,
            _data_cadastro,
            _data_hora_atualizacao
        ),
        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g')) = 14
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 3)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
                     || '/'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 9, 4)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 13, 2)
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g')) = 11
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 3)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 4, 3)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 7, 3)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 10, 2)
            ELSE NULLIF(TRIM(CAST(cgc AS VARCHAR)), '')
        END AS cnpj,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cnpj_pagamento AS VARCHAR), ''), '[^0-9]', '', 'g')) = 14
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_pagamento AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_pagamento AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 3)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_pagamento AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
                     || '/'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_pagamento AS VARCHAR), ''), '[^0-9]', '', 'g'), 9, 4)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_pagamento AS VARCHAR), ''), '[^0-9]', '', 'g'), 13, 2)
            ELSE NULLIF(TRIM(CAST(cnpj_pagamento AS VARCHAR)), '')
        END AS cnpj_pagamento,
        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(endereco AS VARCHAR)),
                            '(^|\\s)EST\\.?\\s+',
                            '\\1ESTRADA ',
                            'gi'
                        ),
                        '(^|\\s)ROD\\.?\\s+',
                        '\\1RODOVIA ',
                        'gi'
                    ),
                    '(^|\\s)AV\\.?\\s+',
                    '\\1AVENIDA ',
                    'gi'
                ),
                '(^|\\s)R\\.?\\s+',
                '\\1RUA ',
                'gi'
            )
        )) AS endereco,
        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(bairro AS VARCHAR)),
                            '(^|\\s)DIST\\.?\\s*',
                            '\\1DISTRITO ',
                            'gi'
                        ),
                        '(^|\\s)PQ\\.?\\s*',
                        '\\1PARQUE ',
                        'gi'
                    ),
                    '(^|\\s)JD\\.?\\s*',
                    '\\1JARDIM ',
                    'gi'
                ),
                '(^|\\s)VL\\.?\\s*',
                '\\1VILA ',
                'gi'
            )
        )) AS bairro,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g')) = 8
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 5)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
            ELSE NULLIF(TRIM(CAST(cep AS VARCHAR)), '')
        END AS cep,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(fone AS VARCHAR), ''), '[^0-9]', '', 'g')) = 11
                THEN '('
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(fone AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                     || ') '
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(fone AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 5)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(fone AS VARCHAR), ''), '[^0-9]', '', 'g'), 8, 4)
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(fone AS VARCHAR), ''), '[^0-9]', '', 'g')) = 10
                THEN '('
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(fone AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                     || ') '
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(fone AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 4)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(fone AS VARCHAR), ''), '[^0-9]', '', 'g'), 7, 4)
            ELSE NULLIF(TRIM(CAST(fone AS VARCHAR)), '')
        END AS fone,
        CASE
            WHEN NULLIF(TRIM(CAST(complemento AS VARCHAR)), '') IS NULL
                THEN NULL
            WHEN REGEXP_REPLACE(
                    UPPER(STRIP_ACCENTS(TRIM(CAST(complemento AS VARCHAR)))),
                    '[^A-Z0-9]',
                    '',
                    'g'
                 ) IN ('SN', 'SEMNUMERO')
                THEN 'SN'
            ELSE UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(complemento AS VARCHAR)),
                            '(^|\\s)SL\\.?\\s*',
                            '\\1SALA ',
                            'gi'
                        ),
                        '(^|\\s)SALA\\.?\\s*',
                        '\\1SALA ',
                        'gi'
                    ),
                    '\\b([1-9])\\b',
                    '0\\1',
                    'g'
                )
            ))
        END AS complemento,
        _data_alteracao_cadastro AS data_alteracao_cadastro,
        _data_cadastro AS data_cadastro,
        _data_hora_atualizacao AS data_hora_atualizacao,
        CASE WHEN _data_alteracao_cadastro IS NOT NULL
             THEN CAST(STRFTIME(_data_alteracao_cadastro, '%Y%m%d') AS INTEGER) END AS data_alteracao_cadastro_key,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_ent AS VARCHAR)) AS entidade_key,
        CASE WHEN vendedor IS NOT NULL
             THEN CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(vendedor AS VARCHAR)) END AS vendedor_key,
        UPPER(
            CONCAT_WS(
                '-',
                TRIM(CAST(nome_alias AS VARCHAR)),
                TRIM(CAST(cod_tp_cobr AS VARCHAR))
            )
        ) AS cod_tp_cobr_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/entidade.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 11. VENDEDOR
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/vendedor.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_vend AS VARCHAR)) AS vendedor_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/vendedor.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 12. TIPO DOCUMENTO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/tipo_documento.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(seq_docto AS VARCHAR)) AS tipo_documento_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/tipo_documento.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 13. NATUREZA OPERAÇÃO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/natureza_operacao.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_nat_op AS VARCHAR)) AS natureza_operacao_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/natureza_operacao.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 14. Z CLASSIFICAÇÃO DESPESAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/z_classificacao_despesas.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(id AS VARCHAR)) AS classificacao_despesa_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/z_classificacao_despesas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 15. PLANO CONTAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/plano_contas.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_conta AS VARCHAR)) AS plano_conta_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/plano_contas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 16. PORTADOR
-- Execute depois de gerar Silver/portador.parquet.
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/portador.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(cod_port AS VARCHAR)) AS portador_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/portador.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 17. CENTRO CUSTO
-- Execute depois de gerar Silver/centro_custo.parquet.
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet('//10.68.152.218/d$/Pipeline/Silver/Current/centro_custo.parquet', union_by_name = true)
    )
    SELECT
        *,
        CONCAT_WS('-', CAST(nome_alias AS VARCHAR), CAST(seq_cc AS VARCHAR)) AS centro_custo_key
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/centro_custo.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 18. PEDIDO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/pedido.parquet',
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
            TRY_CAST(data_ped AS DATE) AS _data_ped,
            TRY_CAST(dt_prev_fat AS DATE) AS _dt_prev_fat,
            TRY_CAST(lib_financ_em AS DATE) AS _lib_financ_em,
            TRY_CAST(lib_vendas_em AS DATE) AS _lib_vendas_em,
            TRY_CAST(data_cancelamento AS DATE) AS _data_cancelamento,
            TRY_CAST(aprov_transp_em AS DATE) AS _aprov_transp_em,
            TRY_CAST(cr_data_pre_credito AS DATE) AS _cr_data_pre_credito,
            TRY_CAST(data_movto AS DATE) AS _data_movto,
            TRY_CAST(servico_analise_aprovacao_vd_data AS DATE) AS _servico_analise_aprovacao_vd_data,
            TRY_CAST(data_inicio_execucao AS DATE) AS _data_inicio_execucao,
            TRY_CAST(data_fim_execucao AS DATE) AS _data_fim_execucao
        FROM sem_exclusoes
    )
    SELECT
        * EXCLUDE (
            data_ped,
            dt_prev_fat,
            lib_financ_em,
            lib_vendas_em,
            data_cancelamento,
            aprov_transp_em,
            cr_data_pre_credito,
            data_movto,
            servico_analise_aprovacao_vd_data,
            data_inicio_execucao,
            data_fim_execucao,
            razao_social,
            descr_transp,
            rz_social_ent,
            cnpj_ent,
            endereco_cobranca,
            endereco_entrega,
            endereco_fat,
            complemento_entrega,
            complemento_cobranca,
            complemento_fat,
            cep_cobranca,
            cep_entrega,
            cep_fat,
            bairro_cobranca,
            bairro_entrega,
            bairro_fat,
            _data_ped,
            _dt_prev_fat,
            _lib_financ_em,
            _lib_vendas_em,
            _data_cancelamento,
            _aprov_transp_em,
            _cr_data_pre_credito,
            _data_movto,
            _servico_analise_aprovacao_vd_data,
            _data_inicio_execucao,
            _data_fim_execucao
        ),

        _data_ped AS data_ped,
        _dt_prev_fat AS dt_prev_fat,
        _lib_financ_em AS lib_financ_em,
        _lib_vendas_em AS lib_vendas_em,
        _data_cancelamento AS data_cancelamento,
        _aprov_transp_em AS aprov_transp_em,
        _cr_data_pre_credito AS cr_data_pre_credito,
        _data_movto AS data_movto,
        _servico_analise_aprovacao_vd_data AS servico_analise_aprovacao_vd_data,
        _data_inicio_execucao AS data_inicio_execucao,
        _data_fim_execucao AS data_fim_execucao,

        CASE WHEN _data_ped IS NOT NULL
             THEN CAST(STRFTIME(_data_ped, '%Y%m%d') AS INTEGER) END AS data_ped_key,
        CASE WHEN _dt_prev_fat IS NOT NULL
             THEN CAST(STRFTIME(_dt_prev_fat, '%Y%m%d') AS INTEGER) END AS dt_prev_fat_key,
        CASE WHEN _lib_financ_em IS NOT NULL
             THEN CAST(STRFTIME(_lib_financ_em, '%Y%m%d') AS INTEGER) END AS lib_financ_em_key,
        CASE WHEN _lib_vendas_em IS NOT NULL
             THEN CAST(STRFTIME(_lib_vendas_em, '%Y%m%d') AS INTEGER) END AS lib_vendas_em_key,
        CASE WHEN _data_cancelamento IS NOT NULL
             THEN CAST(STRFTIME(_data_cancelamento, '%Y%m%d') AS INTEGER) END AS data_cancelamento_key,
        CASE WHEN _aprov_transp_em IS NOT NULL
             THEN CAST(STRFTIME(_aprov_transp_em, '%Y%m%d') AS INTEGER) END AS aprov_transp_em_key,
        CASE WHEN _cr_data_pre_credito IS NOT NULL
             THEN CAST(STRFTIME(_cr_data_pre_credito, '%Y%m%d') AS INTEGER) END AS cr_data_pre_credito_key,
        CASE WHEN _data_movto IS NOT NULL
             THEN CAST(STRFTIME(_data_movto, '%Y%m%d') AS INTEGER) END AS data_movto_key,
        CASE WHEN _servico_analise_aprovacao_vd_data IS NOT NULL
             THEN CAST(STRFTIME(_servico_analise_aprovacao_vd_data, '%Y%m%d') AS INTEGER) END AS servico_analise_aprovacao_vd_data_key,
        CASE WHEN _data_inicio_execucao IS NOT NULL
             THEN CAST(STRFTIME(_data_inicio_execucao, '%Y%m%d') AS INTEGER) END AS data_inicio_execucao_key,
        CASE WHEN _data_fim_execucao IS NOT NULL
             THEN CAST(STRFTIME(_data_fim_execucao, '%Y%m%d') AS INTEGER) END AS data_fim_execucao_key,

        UPPER(STRIP_ACCENTS(TRIM(CAST(razao_social AS VARCHAR)))) AS razao_social,
        UPPER(STRIP_ACCENTS(TRIM(CAST(descr_transp AS VARCHAR)))) AS descr_transp,
        UPPER(STRIP_ACCENTS(TRIM(CAST(rz_social_ent AS VARCHAR)))) AS rz_social_ent,

        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cnpj_ent AS VARCHAR), ''), '[^0-9]', '', 'g')) = 14
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_ent AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_ent AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 3)
                     || '.'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_ent AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
                     || '/'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_ent AS VARCHAR), ''), '[^0-9]', '', 'g'), 9, 4)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cnpj_ent AS VARCHAR), ''), '[^0-9]', '', 'g'), 13, 2)
            ELSE NULLIF(TRIM(CAST(cnpj_ent AS VARCHAR)), '')
        END AS cnpj_ent,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(endereco_cobranca AS VARCHAR)),
                            '(^|\\s)EST\\.?\\s+',
                            '\\1ESTRADA ',
                            'gi'
                        ),
                        '(^|\\s)ROD\\.?\\s+',
                        '\\1RODOVIA ',
                        'gi'
                    ),
                    '(^|\\s)AV\\.?\\s+',
                    '\\1AVENIDA ',
                    'gi'
                ),
                '(^|\\s)R\\.?\\s+',
                '\\1RUA ',
                'gi'
            )
        )) AS endereco_cobranca,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(endereco_entrega AS VARCHAR)),
                            '(^|\\s)EST\\.?\\s+',
                            '\\1ESTRADA ',
                            'gi'
                        ),
                        '(^|\\s)ROD\\.?\\s+',
                        '\\1RODOVIA ',
                        'gi'
                    ),
                    '(^|\\s)AV\\.?\\s+',
                    '\\1AVENIDA ',
                    'gi'
                ),
                '(^|\\s)R\\.?\\s+',
                '\\1RUA ',
                'gi'
            )
        )) AS endereco_entrega,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(endereco_fat AS VARCHAR)),
                            '(^|\\s)EST\\.?\\s+',
                            '\\1ESTRADA ',
                            'gi'
                        ),
                        '(^|\\s)ROD\\.?\\s+',
                        '\\1RODOVIA ',
                        'gi'
                    ),
                    '(^|\\s)AV\\.?\\s+',
                    '\\1AVENIDA ',
                    'gi'
                ),
                '(^|\\s)R\\.?\\s+',
                '\\1RUA ',
                'gi'
            )
        )) AS endereco_fat,

        CASE
            WHEN NULLIF(TRIM(CAST(complemento_entrega AS VARCHAR)), '') IS NULL
                THEN NULL
            WHEN REGEXP_REPLACE(
                    UPPER(STRIP_ACCENTS(TRIM(CAST(complemento_entrega AS VARCHAR)))),
                    '[^A-Z0-9]',
                    '',
                    'g'
                 ) IN ('SN', 'SEMNUMERO')
                THEN 'SN'
            ELSE UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(complemento_entrega AS VARCHAR)),
                            '(^|\\s)SL\\.?\\s*',
                            '\\1SALA ',
                            'gi'
                        ),
                        '(^|\\s)SALA\\.?\\s*',
                        '\\1SALA ',
                        'gi'
                    ),
                    '\\b([1-9])\\b',
                    '0\\1',
                    'g'
                )
            ))
        END AS complemento_entrega,

        CASE
            WHEN NULLIF(TRIM(CAST(complemento_cobranca AS VARCHAR)), '') IS NULL
                THEN NULL
            WHEN REGEXP_REPLACE(
                    UPPER(STRIP_ACCENTS(TRIM(CAST(complemento_cobranca AS VARCHAR)))),
                    '[^A-Z0-9]',
                    '',
                    'g'
                 ) IN ('SN', 'SEMNUMERO')
                THEN 'SN'
            ELSE UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(complemento_cobranca AS VARCHAR)),
                            '(^|\\s)SL\\.?\\s*',
                            '\\1SALA ',
                            'gi'
                        ),
                        '(^|\\s)SALA\\.?\\s*',
                        '\\1SALA ',
                        'gi'
                    ),
                    '\\b([1-9])\\b',
                    '0\\1',
                    'g'
                )
            ))
        END AS complemento_cobranca,

        CASE
            WHEN NULLIF(TRIM(CAST(complemento_fat AS VARCHAR)), '') IS NULL
                THEN NULL
            WHEN REGEXP_REPLACE(
                    UPPER(STRIP_ACCENTS(TRIM(CAST(complemento_fat AS VARCHAR)))),
                    '[^A-Z0-9]',
                    '',
                    'g'
                 ) IN ('SN', 'SEMNUMERO')
                THEN 'SN'
            ELSE UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(complemento_fat AS VARCHAR)),
                            '(^|\\s)SL\\.?\\s*',
                            '\\1SALA ',
                            'gi'
                        ),
                        '(^|\\s)SALA\\.?\\s*',
                        '\\1SALA ',
                        'gi'
                    ),
                    '\\b([1-9])\\b',
                    '0\\1',
                    'g'
                )
            ))
        END AS complemento_fat,

        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cep_cobranca AS VARCHAR), ''), '[^0-9]', '', 'g')) = 8
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep_cobranca AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 5)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep_cobranca AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
            ELSE NULLIF(TRIM(CAST(cep_cobranca AS VARCHAR)), '')
        END AS cep_cobranca,

        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cep_entrega AS VARCHAR), ''), '[^0-9]', '', 'g')) = 8
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep_entrega AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 5)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep_entrega AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
            ELSE NULLIF(TRIM(CAST(cep_entrega AS VARCHAR)), '')
        END AS cep_entrega,

        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cep_fat AS VARCHAR), ''), '[^0-9]', '', 'g')) = 8
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep_fat AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 5)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep_fat AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
            ELSE NULLIF(TRIM(CAST(cep_fat AS VARCHAR)), '')
        END AS cep_fat,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(bairro_cobranca AS VARCHAR)),
                            '(^|\\s)DIST\\.?\\s*',
                            '\\1DISTRITO ',
                            'gi'
                        ),
                        '(^|\\s)PQ\\.?\\s*',
                        '\\1PARQUE ',
                        'gi'
                    ),
                    '(^|\\s)JD\\.?\\s*',
                    '\\1JARDIM ',
                    'gi'
                ),
                '(^|\\s)VL\\.?\\s*',
                '\\1VILA ',
                'gi'
            )
        )) AS bairro_cobranca,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(bairro_entrega AS VARCHAR)),
                            '(^|\\s)DIST\\.?\\s*',
                            '\\1DISTRITO ',
                            'gi'
                        ),
                        '(^|\\s)PQ\\.?\\s*',
                        '\\1PARQUE ',
                        'gi'
                    ),
                    '(^|\\s)JD\\.?\\s*',
                    '\\1JARDIM ',
                    'gi'
                ),
                '(^|\\s)VL\\.?\\s*',
                '\\1VILA ',
                'gi'
            )
        )) AS bairro_entrega,

        UPPER(STRIP_ACCENTS(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            TRIM(CAST(bairro_fat AS VARCHAR)),
                            '(^|\\s)DIST\\.?\\s*',
                            '\\1DISTRITO ',
                            'gi'
                        ),
                        '(^|\\s)PQ\\.?\\s*',
                        '\\1PARQUE ',
                        'gi'
                    ),
                    '(^|\\s)JD\\.?\\s*',
                    '\\1JARDIM ',
                    'gi'
                ),
                '(^|\\s)VL\\.?\\s*',
                '\\1VILA ',
                'gi'
            )
        )) AS bairro_fat,

        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(cod_empresa AS VARCHAR),
            CAST(num_ped AS VARCHAR)
        ) AS pedido_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/pedido.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 19. ITENS PEDIDO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/itens_pedido.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(dt_prev_entrega AS DATE) AS _dt_prev_entrega
        FROM atual
    )
    SELECT
        * EXCLUDE (
            dt_prev_entrega,
            _dt_prev_entrega
        ),
        _dt_prev_entrega AS dt_prev_entrega,
        CASE WHEN _dt_prev_entrega IS NOT NULL
             THEN CAST(STRFTIME(_dt_prev_entrega, '%Y%m%d') AS INTEGER) END AS dt_prev_entrega_key,
        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(cod_empresa AS VARCHAR),
            CAST(num_ped AS VARCHAR)
        ) AS pedido_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/itens_pedido.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 20. CIDADE
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/cidade.parquet',
            union_by_name = true
        )
    )
    SELECT
        * EXCLUDE (
            descricao
        ),
        UPPER(STRIP_ACCENTS(TRIM(CAST(descricao AS VARCHAR)))) AS descricao
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/cidade.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 21. BANDEIRA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/bandeira.parquet',
            union_by_name = true
        )
    )
    SELECT
        * EXCLUDE (
            descricao
        ),
        UPPER(STRIP_ACCENTS(TRIM(CAST(descricao AS VARCHAR)))) AS descricao
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/bandeira.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 22. BAIXA ANTECIPADA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/baixa_antecipada.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data AS DATE) AS _data,
            TRY_CAST(data_cheque AS DATE) AS _data_cheque,
            TRY_CAST(data_fluxo AS DATE) AS _data_fluxo,
            TRY_CAST(data_desbloqueio AS DATE) AS _data_desbloqueio,
            TRY_CAST(desbloqueado_em AS DATE) AS _desbloqueado_em,
            TRY_CAST(pmm_pd_ajuste_data AS DATE) AS _pmm_pd_ajuste_data,
            TRY_CAST(data_cancelamento AS DATE) AS _data_cancelamento,
            TRY_CAST(data_simples AS DATE) AS _data_simples,
            TRY_CAST(data_inclusao AS TIMESTAMP) AS _data_inclusao
        FROM atual
    )
    SELECT
        * EXCLUDE (
            data,
            data_cheque,
            data_fluxo,
            data_desbloqueio,
            desbloqueado_em,
            pmm_pd_ajuste_data,
            data_cancelamento,
            data_simples,
            data_inclusao,
            rz_social,
            rz_social_comprador,
            _data,
            _data_cheque,
            _data_fluxo,
            _data_desbloqueio,
            _desbloqueado_em,
            _pmm_pd_ajuste_data,
            _data_cancelamento,
            _data_simples,
            _data_inclusao
        ),

        _data AS data,
        _data_cheque AS data_cheque,
        _data_fluxo AS data_fluxo,
        _data_desbloqueio AS data_desbloqueio,
        _desbloqueado_em AS desbloqueado_em,
        _pmm_pd_ajuste_data AS pmm_pd_ajuste_data,
        _data_cancelamento AS data_cancelamento,
        _data_simples AS data_simples,
        _data_inclusao AS data_inclusao,

        CASE WHEN _data IS NOT NULL
             THEN CAST(STRFTIME(_data, '%Y%m%d') AS INTEGER) END AS data_key,
        CASE WHEN _data_cheque IS NOT NULL
             THEN CAST(STRFTIME(_data_cheque, '%Y%m%d') AS INTEGER) END AS data_cheque_key,
        CASE WHEN _data_fluxo IS NOT NULL
             THEN CAST(STRFTIME(_data_fluxo, '%Y%m%d') AS INTEGER) END AS data_fluxo_key,
        CASE WHEN _data_desbloqueio IS NOT NULL
             THEN CAST(STRFTIME(_data_desbloqueio, '%Y%m%d') AS INTEGER) END AS data_desbloqueio_key,
        CASE WHEN _desbloqueado_em IS NOT NULL
             THEN CAST(STRFTIME(_desbloqueado_em, '%Y%m%d') AS INTEGER) END AS desbloqueado_em_key,
        CASE WHEN _pmm_pd_ajuste_data IS NOT NULL
             THEN CAST(STRFTIME(_pmm_pd_ajuste_data, '%Y%m%d') AS INTEGER) END AS pmm_pd_ajuste_data_key,
        CASE WHEN _data_cancelamento IS NOT NULL
             THEN CAST(STRFTIME(_data_cancelamento, '%Y%m%d') AS INTEGER) END AS data_cancelamento_key,
        CASE WHEN _data_simples IS NOT NULL
             THEN CAST(STRFTIME(_data_simples, '%Y%m%d') AS INTEGER) END AS data_simples_key,
        CASE WHEN _data_inclusao IS NOT NULL
             THEN CAST(STRFTIME(CAST(_data_inclusao AS DATE), '%Y%m%d') AS INTEGER) END AS data_inclusao_key,

        UPPER(STRIP_ACCENTS(TRIM(CAST(rz_social AS VARCHAR)))) AS rz_social,
        UPPER(STRIP_ACCENTS(TRIM(CAST(rz_social_comprador AS VARCHAR)))) AS rz_social_comprador,

        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(cod_empresa AS VARCHAR),
            CAST(seq_credito AS VARCHAR)
        ) AS baixa_antecipada_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/baixa_antecipada.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 23. ITENS BAIXA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/itens_baixa.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(dt_baixa AS DATE) AS _dt_baixa
        FROM atual
    )
    SELECT
        * EXCLUDE (
            dt_baixa,
            _dt_baixa
        ),
        _dt_baixa AS dt_baixa,
        CASE WHEN _dt_baixa IS NOT NULL
             THEN CAST(STRFTIME(_dt_baixa, '%Y%m%d') AS INTEGER) END AS dt_baixa_key,
        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(cod_empresa AS VARCHAR),
            CAST(num_baixa AS VARCHAR)
        ) AS baixa_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/itens_baixa.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 24. BAIXA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/baixa.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(dt_baixa AS DATE) AS _dt_baixa,
            TRY_CAST(data_cheque AS DATE) AS _data_cheque,
            TRY_CAST(data_fluxo AS DATE) AS _data_fluxo,
            TRY_CAST(data_prog_desbloqueio AS DATE) AS _data_prog_desbloqueio,
            TRY_CAST(data_desbloqueio AS DATE) AS _data_desbloqueio,
            TRY_CAST(data_inclusao_baixa AS TIMESTAMP) AS _data_inclusao_baixa,
            TRY_CAST(data_cancelamento AS DATE) AS _data_cancelamento,
            TRY_CAST(data_cancto_usu AS DATE) AS _data_cancto_usu
        FROM atual
    )
    SELECT
        * EXCLUDE (
            dt_baixa,
            data_cheque,
            data_fluxo,
            data_prog_desbloqueio,
            data_desbloqueio,
            data_inclusao_baixa,
            data_cancelamento,
            data_cancto_usu,
            _dt_baixa,
            _data_cheque,
            _data_fluxo,
            _data_prog_desbloqueio,
            _data_desbloqueio,
            _data_inclusao_baixa,
            _data_cancelamento,
            _data_cancto_usu
        ),

        _dt_baixa AS dt_baixa,
        _data_cheque AS data_cheque,
        _data_fluxo AS data_fluxo,
        _data_prog_desbloqueio AS data_prog_desbloqueio,
        _data_desbloqueio AS data_desbloqueio,
        _data_inclusao_baixa AS data_inclusao_baixa,
        _data_cancelamento AS data_cancelamento,
        _data_cancto_usu AS data_cancto_usu,

        CASE WHEN _dt_baixa IS NOT NULL
             THEN CAST(STRFTIME(_dt_baixa, '%Y%m%d') AS INTEGER) END AS dt_baixa_key,
        CASE WHEN _data_cheque IS NOT NULL
             THEN CAST(STRFTIME(_data_cheque, '%Y%m%d') AS INTEGER) END AS data_cheque_key,
        CASE WHEN _data_fluxo IS NOT NULL
             THEN CAST(STRFTIME(_data_fluxo, '%Y%m%d') AS INTEGER) END AS data_fluxo_key,
        CASE WHEN _data_prog_desbloqueio IS NOT NULL
             THEN CAST(STRFTIME(_data_prog_desbloqueio, '%Y%m%d') AS INTEGER) END AS data_prog_desbloqueio_key,
        CASE WHEN _data_desbloqueio IS NOT NULL
             THEN CAST(STRFTIME(_data_desbloqueio, '%Y%m%d') AS INTEGER) END AS data_desbloqueio_key,
        CASE WHEN _data_inclusao_baixa IS NOT NULL
             THEN CAST(STRFTIME(CAST(_data_inclusao_baixa AS DATE), '%Y%m%d') AS INTEGER) END AS data_inclusao_baixa_key,
        CASE WHEN _data_cancelamento IS NOT NULL
             THEN CAST(STRFTIME(_data_cancelamento, '%Y%m%d') AS INTEGER) END AS data_cancelamento_key,
        CASE WHEN _data_cancto_usu IS NOT NULL
             THEN CAST(STRFTIME(_data_cancto_usu, '%Y%m%d') AS INTEGER) END AS data_cancto_usu_key,

        CONCAT_WS(
            '-',
            CAST(nome_alias AS VARCHAR),
            CAST(cod_empresa AS VARCHAR),
            CAST(num_baixa AS VARCHAR)
        ) AS baixa_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/baixa.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 25. PRODUTO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/produto.parquet',
            union_by_name = true
        )
    )
    SELECT
        * EXCLUDE (
            descricao_completa,
            descricao_nf,
            descricao_reduzida,
            anp_descricao
        ),

        UPPER(STRIP_ACCENTS(TRIM(CAST(descricao_completa AS VARCHAR)))) AS descricao_completa,
        UPPER(STRIP_ACCENTS(TRIM(CAST(descricao_nf AS VARCHAR)))) AS descricao_nf,
        UPPER(STRIP_ACCENTS(TRIM(CAST(descricao_reduzida AS VARCHAR)))) AS descricao_reduzida,
        UPPER(STRIP_ACCENTS(TRIM(CAST(anp_descricao AS VARCHAR)))) AS anp_descricao
    FROM atual
    WHERE (
        UPPER(COALESCE(_operation, '')) IN ('DELETE', 'DELETED', 'D')
        OR UPPER(TRIM(CAST(tipo_prod AS VARCHAR))) = 'CB'
    )
)
TO '//10.68.152.218/d$/Pipeline/Gold/produto.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 26. DEPOSITO
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/deposito.parquet',
            union_by_name = true
        )
    ),
    tratado AS (
        SELECT
            * EXCLUDE (
                descricao,
                rz_social,
                cgc,
                endereco,
                complemento,
                bairro,
                cep,
                telefone,
                cod_prod
            ),

            UPPER(STRIP_ACCENTS(TRIM(CAST(descricao AS VARCHAR)))) AS descricao,
            UPPER(STRIP_ACCENTS(TRIM(CAST(rz_social AS VARCHAR)))) AS rz_social,

            CASE
                WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g')) = 14
                    THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                         || '.'
                         || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 3)
                         || '.'
                         || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
                         || '/'
                         || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 9, 4)
                         || '-'
                         || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cgc AS VARCHAR), ''), '[^0-9]', '', 'g'), 13, 2)
                ELSE NULLIF(TRIM(CAST(cgc AS VARCHAR)), '')
            END AS cgc,

            UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                TRIM(CAST(endereco AS VARCHAR)),
                                '(^|\\s)EST\\.?\\s+',
                                '\\1ESTRADA ',
                                'gi'
                            ),
                            '(^|\\s)ROD\\.?\\s+',
                            '\\1RODOVIA ',
                            'gi'
                        ),
                        '(^|\\s)AV\\.?\\s+',
                        '\\1AVENIDA ',
                        'gi'
                    ),
                    '(^|\\s)R\\.?\\s+',
                    '\\1RUA ',
                    'gi'
                )
            )) AS endereco,

            CASE
                WHEN NULLIF(TRIM(CAST(complemento AS VARCHAR)), '') IS NULL
                    THEN NULL
                WHEN REGEXP_REPLACE(
                        UPPER(STRIP_ACCENTS(TRIM(CAST(complemento AS VARCHAR)))),
                        '[^A-Z0-9]',
                        '',
                        'g'
                     ) IN ('SN', 'SEMNUMERO')
                    THEN 'SN'
                ELSE UPPER(STRIP_ACCENTS(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                TRIM(CAST(complemento AS VARCHAR)),
                                '(^|\\s)SL\\.?\\s*',
                                '\\1SALA ',
                                'gi'
                            ),
                            '(^|\\s)SALA\\.?\\s*',
                            '\\1SALA ',
                            'gi'
                        ),
                        '\\b([1-9])\\b',
                        '0\\1',
                        'g'
                    )
                ))
            END AS complemento,

        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g')) = 8
                THEN SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 5)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(cep AS VARCHAR), ''), '[^0-9]', '', 'g'), 6, 3)
            ELSE NULLIF(TRIM(CAST(cep AS VARCHAR)), '')
        END AS cep,

        CASE
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(telefone AS VARCHAR), ''), '[^0-9]', '', 'g')) = 11
                THEN '('
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(telefone AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                     || ') '
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(telefone AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 5)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(telefone AS VARCHAR), ''), '[^0-9]', '', 'g'), 8, 4)
            WHEN LENGTH(REGEXP_REPLACE(COALESCE(CAST(telefone AS VARCHAR), ''), '[^0-9]', '', 'g')) = 10
                THEN '('
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(telefone AS VARCHAR), ''), '[^0-9]', '', 'g'), 1, 2)
                     || ') '
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(telefone AS VARCHAR), ''), '[^0-9]', '', 'g'), 3, 4)
                     || '-'
                     || SUBSTR(REGEXP_REPLACE(COALESCE(CAST(telefone AS VARCHAR), ''), '[^0-9]', '', 'g'), 7, 4)
            ELSE NULLIF(TRIM(CAST(telefone AS VARCHAR)), '')
        END AS telefone,

            UPPER(STRIP_ACCENTS(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                TRIM(CAST(bairro AS VARCHAR)),
                                '(^|\\s)DIST\\.?\\s*',
                                '\\1DISTRITO ',
                                'gi'
                            ),
                            '(^|\\s)PQ\\.?\\s*',
                            '\\1PARQUE ',
                            'gi'
                        ),
                        '(^|\\s)JD\\.?\\s*',
                        '\\1JARDIM ',
                        'gi'
                    ),
                    '(^|\\s)VL\\.?\\s*',
                    '\\1VILA ',
                    'gi'
                )
            )) AS bairro,

            CASE UPPER(TRIM(CAST(cod_dep AS VARCHAR)))
                WHEN 'FL' THEN 'ARUJA'
                WHEN '76' THEN 'CAPIVARI'
                WHEN 'RF' THEN 'REFIT'
                WHEN 'DI' THEN 'DIRECIONAL MTZ'
                WHEN 'DF' THEN 'DIRECIONAL FILIAL'
                WHEN 'BG' THEN 'BCAG'
                WHEN 'TL' THEN 'TLIQ'
                WHEN 'TP' THEN 'TPLAN'
                WHEN 'BM' THEN 'BARRA MANSA'
                WHEN 'CX' THEN 'CAXIAS'
                WHEN 'PN' THEN 'PETRONORTE'
                WHEN 'CV' THEN 'CAVALINI'
                WHEN 'CT' THEN 'CAVALINI'
                ELSE UPPER(STRIP_ACCENTS(TRIM(CAST(descricao AS VARCHAR))))
            END AS nome_deposito

        FROM atual
    )

    SELECT *
    FROM tratado
)
TO '//10.68.152.218/d$/Pipeline/Gold/deposito.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- 27. TIPO COBRANCA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/tipo_cobranca.parquet',
            union_by_name = true
        )
    )

    SELECT
        UPPER(TRIM(CAST(nome_alias AS VARCHAR))) AS nome_alias,
        UPPER(TRIM(CAST(cod_tp_cobr AS VARCHAR))) AS cod_tp_cobr,
        UPPER(STRIP_ACCENTS(TRIM(CAST(descricao AS VARCHAR)))) AS descricao,

        UPPER(
            CONCAT_WS(
                '-',
                TRIM(CAST(nome_alias AS VARCHAR)),
                TRIM(CAST(cod_tp_cobr AS VARCHAR))
            )
        ) AS cod_tp_cobr_key,

        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp

    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/tipo_cobranca.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- ------------------------------------------------------------
-- VENDAS POSTOS
-- ------------------------------------------------------------
-- Compatibilidade com a Silver Current legada da API (camel_case).
-- A Gold entrega o contrato snake_case e cria a chave de negócio.
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos.parquet',
            union_by_name = true
        )
    )
    SELECT
        codigo AS id,
        codigo,
        cancelada,
        nome_alias,
        empresa_codigo AS empresa_codigo,
        venda_codigo AS venda_codigo,
        nota_codigo AS nota_codigo,
        funcionario_codigo AS funcionario_codigo,
        cliente_codigo AS cliente_codigo,
        destaca_acrescimo_desconto AS destaca_acrescimo_desconto,
        UPPER(STRIP_ACCENTS(TRIM(CAST(cliente_cpf_cnpj AS VARCHAR)))) AS cliente_cpf_cnpj,
        TRY_CAST(data_hora AS TIMESTAMP) AS data_hora,
        UPPER(STRIP_ACCENTS(TRIM(CAST(nota_numero AS VARCHAR)))) AS nota_numero,
        UPPER(STRIP_ACCENTS(TRIM(CAST(nota_serie AS VARCHAR)))) AS nota_serie,
        total_venda AS total_venda,
        caixa_codigo AS caixa_codigo,
        UPPER(STRIP_ACCENTS(TRIM(CAST(nota_chave AS VARCHAR)))) AS nota_chave,
        UPPER(STRIP_ACCENTS(TRIM(CAST(modelo_documento AS VARCHAR)))) AS modelo_documento,
        UPPER(STRIP_ACCENTS(TRIM(CAST(placa_veiculo AS VARCHAR)))) AS placa_veiculo,
        cliente_codigo_externo AS cliente_codigo_externo,
        centro_custo_codigo AS centro_custo_codigo,
        centro_custo_veiculo AS centro_custo_veiculo,
        UPPER(STRIP_ACCENTS(TRIM(CAST(identificacao_fidelidade AS VARCHAR)))) AS identificacao_fidelidade,
        UPPER(STRIP_ACCENTS(TRIM(CAST(venda_uuid AS VARCHAR)))) AS venda_uuid,
        motorista_codigo AS motorista_codigo,

        CONCAT_WS(
            '-',
            CAST(empresa_codigo AS VARCHAR),
            CAST(venda_codigo AS VARCHAR)
        ) AS venda_key,

        _source_id,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM atual
)
TO '//10.68.152.218/d$/Pipeline/Gold/vendas_postos.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);


-- ------------------------------------------------------------
-- VENDAS POSTOS FORMA PAGAMENTO
-- ------------------------------------------------------------
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos_forma_pagamento.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_movimento AS DATE) AS _data_movimento,
            TRY_CAST(vencimento AS DATE) AS _vencimento,
            TRY_CAST(venda_prazo_codigo AS BIGINT) AS _venda_prazo_codigo
        FROM atual
    )
    SELECT
        * EXCLUDE (
            
            nome_alias,
            data_movimento,
            vencimento,
            venda_prazo_codigo,
            tipo_forma_pagamento,
            nome_forma_pagamento,
            _data_movimento,
            _vencimento,
            _venda_prazo_codigo
        ),

        UPPER(TRIM(CAST(nome_alias AS VARCHAR))) AS nome_alias,

        _data_movimento AS data_movimento,
        _vencimento AS vencimento,
        _venda_prazo_codigo AS venda_prazo_codigo,

        UPPER(
            STRIP_ACCENTS(
                REGEXP_REPLACE(
                    TRIM(CAST(tipo_forma_pagamento AS VARCHAR)),
                    '[[:space:]]+',
                    ' ',
                    'g'
                )
            )
        ) AS tipo_forma_pagamento,

        UPPER(
            STRIP_ACCENTS(
                REGEXP_REPLACE(
                    TRIM(CAST(nome_forma_pagamento AS VARCHAR)),
                    '[[:space:]]+',
                    ' ',
                    'g'
                )
            )
        ) AS nome_forma_pagamento,

        CONCAT_WS(
            '-',
            CAST(empresa_codigo AS VARCHAR),
            CAST(venda_codigo AS VARCHAR)
        ) AS venda_key,

        CONCAT_WS(
            '-',
            CAST(empresa_codigo AS VARCHAR),
            CAST(codigo AS VARCHAR)
        ) AS venda_forma_pagamento_key

    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/vendas_postos_forma_pagamento.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);


-- ------------------------------------------------------------
-- VENDAS POSTOS ITENS
-- ------------------------------------------------------------
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos_itens.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_movimento AS DATE) AS _data_movimento
        FROM atual
    )
    SELECT
        * EXCLUDE (
            
            nome_alias,
            data_movimento,
            cfop,
            cst,
            produto_codigo_externo,
            cst_pis,
            cst_cofins,
            tributacao_ad_rem,
            cst_cbs,
            class_tributaria_cbs,
            cst_ibs,
            class_tributaria_ibs,
            _data_movimento
        ),

        UPPER(TRIM(CAST(nome_alias AS VARCHAR))) AS nome_alias,

        _data_movimento AS data_movimento,

        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(cfop AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS cfop,
        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(cst AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS cst,
        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(produto_codigo_externo AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS produto_codigo_externo,
        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(cst_pis AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS cst_pis,
        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(cst_cofins AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS cst_cofins,
        TRY_CAST(tributacao_ad_rem AS DECIMAL(18,4)) AS tributacao_ad_rem,
        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(cst_cbs AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS cst_cbs,
        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(class_tributaria_cbs AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS class_tributaria_cbs,
        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(cst_ibs AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS cst_ibs,
        UPPER(STRIP_ACCENTS(REGEXP_REPLACE(TRIM(CAST(class_tributaria_ibs AS VARCHAR)), '[[:space:]]+', ' ', 'g'))) AS class_tributaria_ibs,



    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/vendas_postos_itens.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);


-- ------------------------------------------------------------
-- 28. POSTOS ABASTECIMENTOS
-- ------------------------------------------------------------
-- Silver entrega snake_case com tipos crus (payload da API).
-- Aqui: tipagem (TRY_CAST), data_key yyyymmdd, placa normalizada
-- e chaves de negocio. Mesmo padrao de vendas_postos_itens.
-- ------------------------------------------------------------
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_abastecimentos.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_fiscal AS DATE) AS _data_fiscal,
            TRY_CAST(data_hora_abastecimento AS TIMESTAMP) AS _data_hora_abastecimento,
            TRY_CAST(quantidade AS DECIMAL(18,4)) AS _quantidade,
            TRY_CAST(valor_unitario AS DECIMAL(18,4)) AS _valor_unitario,
            TRY_CAST(valor_total AS DECIMAL(18,2)) AS _valor_total,
            TRY_CAST(preco_cadastro AS DECIMAL(18,4)) AS _preco_cadastro,
            TRY_CAST(tabela_preco_a AS DECIMAL(18,4)) AS _tabela_preco_a,
            TRY_CAST(tabela_preco_b AS DECIMAL(18,4)) AS _tabela_preco_b,
            TRY_CAST(tabela_preco_c AS DECIMAL(18,4)) AS _tabela_preco_c,
            TRY_CAST(encerrante AS DECIMAL(18,4)) AS _encerrante
        FROM atual
    )
    SELECT
        * EXCLUDE (
            data_fiscal,
            data_hora_abastecimento,
            quantidade,
            valor_unitario,
            valor_total,
            preco_cadastro,
            tabela_preco_a,
            tabela_preco_b,
            tabela_preco_c,
            encerrante,
            placa,
            _data_fiscal,
            _data_hora_abastecimento,
            _quantidade,
            _valor_unitario,
            _valor_total,
            _preco_cadastro,
            _tabela_preco_a,
            _tabela_preco_b,
            _tabela_preco_c,
            _encerrante
        ),
        _data_fiscal AS data_fiscal,
        _data_hora_abastecimento AS data_hora_abastecimento,
        _quantidade AS quantidade,
        _valor_unitario AS valor_unitario,
        _valor_total AS valor_total,
        _preco_cadastro AS preco_cadastro,
        _tabela_preco_a AS tabela_preco_a,
        _tabela_preco_b AS tabela_preco_b,
        _tabela_preco_c AS tabela_preco_c,
        _encerrante AS encerrante,
        UPPER(STRIP_ACCENTS(TRIM(CAST(placa AS VARCHAR)))) AS placa,
        CASE WHEN _data_fiscal IS NOT NULL
             THEN CAST(STRFTIME(_data_fiscal, '%Y%m%d') AS INTEGER) END AS data_fiscal_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_abastecimentos.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- ------------------------------------------------------------
-- 29. POSTOS DUPLICATAS
-- ------------------------------------------------------------
-- Silver entrega snake_case com tipos crus (payload da API).
-- Aqui: tipagem (TRY_CAST), data_key yyyymmdd, cliente normalizado.
-- Mesmo padrao de vendas_postos_itens.
-- ------------------------------------------------------------
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_duplicatas.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_movimento AS DATE) AS _data_movimento,
            TRY_CAST(vencimento AS DATE) AS _vencimento,
            TRY_CAST(data_pagamento AS DATE) AS _data_pagamento,
            TRY_CAST(valor_duplicata AS DECIMAL(18,2)) AS _valor_duplicata,
            TRY_CAST(valor_pago AS DECIMAL(18,2)) AS _valor_pago,
            TRY_CAST(valor_acrescimo AS DECIMAL(18,2)) AS _valor_acrescimo,
            TRY_CAST(valor_desconto AS DECIMAL(18,2)) AS _valor_desconto,
            TRY_CAST(valor_liquido AS DECIMAL(18,2)) AS _valor_liquido
        FROM atual
    )
    SELECT
        * EXCLUDE (
            data_movimento,
            vencimento,
            data_pagamento,
            valor_duplicata,
            valor_pago,
            valor_acrescimo,
            valor_desconto,
            valor_liquido,
            nome_cliente,
            cpf_cnpj_cliente,
            _data_movimento,
            _vencimento,
            _data_pagamento,
            _valor_duplicata,
            _valor_pago,
            _valor_acrescimo,
            _valor_desconto,
            _valor_liquido
        ),
        _data_movimento AS data_movimento,
        _vencimento AS vencimento,
        _data_pagamento AS data_pagamento,
        _valor_duplicata AS valor_duplicata,
        _valor_pago AS valor_pago,
        _valor_acrescimo AS valor_acrescimo,
        _valor_desconto AS valor_desconto,
        _valor_liquido AS valor_liquido,
        UPPER(STRIP_ACCENTS(TRIM(CAST(nome_cliente AS VARCHAR)))) AS nome_cliente,
        UPPER(TRIM(CAST(cpf_cnpj_cliente AS VARCHAR))) AS cpf_cnpj_cliente,
        CASE WHEN _data_movimento IS NOT NULL
             THEN CAST(STRFTIME(_data_movimento, '%Y%m%d') AS INTEGER) END AS data_movimento_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_duplicatas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- ------------------------------------------------------------
-- 30. POSTOS MOVIMENTOS CONTA
-- ------------------------------------------------------------
-- Silver entrega snake_case com tipos crus (payload da API).
-- Aqui: tipagem (TRY_CAST), data_key yyyymmdd, textos normalizados.
-- Mesmo padrao de vendas_postos_itens.
-- ------------------------------------------------------------
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_movimentos_conta.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_movimento AS DATE) AS _data_movimento,
            TRY_CAST(evento AS TIMESTAMPTZ) AS _evento,
            TRY_CAST(data_hora_conciliacao AS TIMESTAMPTZ) AS _data_hora_conciliacao,
            TRY_CAST(valor AS DECIMAL(18,2)) AS _valor,
            TRY_CAST(saldo AS DECIMAL(18,2)) AS _saldo
        FROM atual
    )
    SELECT
        * EXCLUDE (
            data_movimento, evento, data_hora_conciliacao, valor, saldo,
            descricao, tipo, tipo_documento_origem, documento,
            usuario_conciliacao, tipo_pessoa,
            _data_movimento, _evento, _data_hora_conciliacao, _valor, _saldo
        ),
        _data_movimento AS data_movimento,
        _evento AS evento,
        _data_hora_conciliacao AS data_hora_conciliacao,
        _valor AS valor,
        _saldo AS saldo,
        UPPER(STRIP_ACCENTS(TRIM(CAST(descricao AS VARCHAR)))) AS descricao,
        UPPER(STRIP_ACCENTS(TRIM(CAST(tipo AS VARCHAR)))) AS tipo,
        UPPER(STRIP_ACCENTS(TRIM(CAST(tipo_documento_origem AS VARCHAR)))) AS tipo_documento_origem,
        UPPER(STRIP_ACCENTS(TRIM(CAST(documento AS VARCHAR)))) AS documento,
        UPPER(STRIP_ACCENTS(TRIM(CAST(usuario_conciliacao AS VARCHAR)))) AS usuario_conciliacao,
        UPPER(STRIP_ACCENTS(TRIM(CAST(tipo_pessoa AS VARCHAR)))) AS tipo_pessoa,
        CASE WHEN _data_movimento IS NOT NULL
             THEN CAST(STRFTIME(_data_movimento, '%Y%m%d') AS INTEGER) END AS data_movimento_key
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_movimentos_conta.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);


-- 28. POSTOS TANQUES
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_tanques.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(capacidade AS DECIMAL(18,4)) AS _capacidade,
            TRY_CAST(lastro AS DECIMAL(18,4)) AS _lastro,
            TRY_CAST(estoque_escritural AS DECIMAL(18,4)) AS _estoque_escritural,
            TRY_CAST(data_hora_medidor AS TIMESTAMPTZ) AS _data_hora_medidor
        FROM atual
    )
    SELECT
        * EXCLUDE (
            nome,
            capacidade,
            lastro,
            estoque_escritural,
            data_hora_medidor,
            _capacidade,
            _lastro,
            _estoque_escritural,
            _data_hora_medidor
        ),
        UPPER(STRIP_ACCENTS(TRIM(CAST(nome AS VARCHAR)))) AS nome,
        _capacidade AS capacidade,
        _lastro AS lastro,
        _estoque_escritural AS estoque_escritural,
        _data_hora_medidor AS data_hora_medidor
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_tanques.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 29. POSTOS TANQUES ENTRADAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_tanques_entradas.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(volume_inicio AS DECIMAL(18,4)) AS _volume_inicio,
            TRY_CAST(volume_fim AS DECIMAL(18,4)) AS _volume_fim,
            TRY_CAST(volume_entrada AS DECIMAL(18,4)) AS _volume_entrada,
            -- a API devolve dataHora sem offset; anexa -03:00 (Brasilia)
            -- para nao deixar o DuckDB assumir UTC no cast para TIMESTAMPTZ
            TRY_CAST(data_hora_inicio || '-03:00' AS TIMESTAMPTZ) AS _data_hora_inicio,
            TRY_CAST(data_hora_fim || '-03:00' AS TIMESTAMPTZ) AS _data_hora_fim
        FROM atual
    )
    SELECT
        * EXCLUDE (
            volume_inicio,
            volume_fim,
            volume_entrada,
            data_hora_inicio,
            data_hora_fim,
            _volume_inicio,
            _volume_fim,
            _volume_entrada,
            _data_hora_inicio,
            _data_hora_fim
        ),
        _volume_inicio AS volume_inicio,
        _volume_fim AS volume_fim,
        _volume_entrada AS volume_entrada,
        _data_hora_inicio AS data_hora_inicio,
        _data_hora_fim AS data_hora_fim
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_tanques_entradas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 30. POSTOS TANQUES MEDIDAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_tanques_medidas.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_medida AS DATE) AS _data_medida,
            TRY_CAST(hora_medida AS TIME) AS _hora_medida,
            TRY_CAST(volume AS DECIMAL(18,4)) AS _volume
        FROM atual
    )
    SELECT
        * EXCLUDE (
            data_medida,
            hora_medida,
            volume,
            _data_medida,
            _hora_medida,
            _volume
        ),
        _data_medida AS data_medida,
        _hora_medida AS hora_medida,
        _volume AS volume
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_tanques_medidas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);



-- 31. POSTOS CAIXAS
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_caixas.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_movimento AS DATE) AS _data_movimento,
            TRY_CAST(abertura AS TIMESTAMPTZ) AS _abertura,
            TRY_CAST(fechamento AS TIMESTAMPTZ) AS _fechamento,
            TRY_CAST(data_hora_atualizacao AS TIMESTAMPTZ) AS _data_hora_atualizacao,
            TRY_CAST(apurado AS DECIMAL(18,2)) AS _apurado,
            TRY_CAST(diferenca AS DECIMAL(18,2)) AS _diferenca
        FROM atual
    )
    SELECT
        * EXCLUDE (
            turno,
            tipo_inclusao,
            tipo_bloqueio,
            data_movimento,
            abertura,
            fechamento,
            data_hora_atualizacao,
            apurado,
            diferenca,
            _data_movimento,
            _abertura,
            _fechamento,
            _data_hora_atualizacao,
            _apurado,
            _diferenca
        ),
        UPPER(STRIP_ACCENTS(TRIM(CAST(turno AS VARCHAR)))) AS turno,
        UPPER(STRIP_ACCENTS(TRIM(CAST(tipo_inclusao AS VARCHAR)))) AS tipo_inclusao,
        UPPER(STRIP_ACCENTS(TRIM(CAST(tipo_bloqueio AS VARCHAR)))) AS tipo_bloqueio,
        _data_movimento AS data_movimento,
        _abertura AS abertura,
        _fechamento AS fechamento,
        _data_hora_atualizacao AS data_hora_atualizacao,
        _apurado AS apurado,
        _diferenca AS diferenca
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_caixas.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- 32. POSTOS SANGRIAS CAIXA
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_sangrias_caixa.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(data_sangria AS DATE) AS _data_sangria,
            TRY_CAST(hora_sangria AS TIME) AS _hora_sangria,
            -- a API devolve dataHora SEM offset; anexa -03:00 (Brasilia)
            -- para nao deixar o DuckDB assumir UTC no cast para TIMESTAMPTZ
            TRY_CAST(data_hora_coleta || '-03:00' AS TIMESTAMPTZ) AS _data_hora_coleta,
            TRY_CAST(data_hora_atualizacao || '-03:00' AS TIMESTAMPTZ) AS _data_hora_atualizacao,
            TRY_CAST(dinheiro AS DECIMAL(18,2)) AS _dinheiro,
            TRY_CAST(cheque AS DECIMAL(18,2)) AS _cheque,
            TRY_CAST(cartao AS DECIMAL(18,2)) AS _cartao,
            TRY_CAST(nota AS DECIMAL(18,2)) AS _nota,
            TRY_CAST(carta_frete AS DECIMAL(18,2)) AS _carta_frete,
            TRY_CAST(emprestimo AS DECIMAL(18,2)) AS _emprestimo,
            TRY_CAST(despesa AS DECIMAL(18,2)) AS _despesa,
            TRY_CAST(cheque_pre AS DECIMAL(18,2)) AS _cheque_pre,
            TRY_CAST(vale AS DECIMAL(18,2)) AS _vale,
            TRY_CAST(transferencia AS DECIMAL(18,2)) AS _transferencia
        FROM atual
    )
    SELECT
        * EXCLUDE (
            observacao,
            data_sangria,
            hora_sangria,
            data_hora_coleta,
            data_hora_atualizacao,
            dinheiro,
            cheque,
            cartao,
            nota,
            carta_frete,
            emprestimo,
            despesa,
            cheque_pre,
            vale,
            transferencia,
            _data_sangria,
            _hora_sangria,
            _data_hora_coleta,
            _data_hora_atualizacao,
            _dinheiro,
            _cheque,
            _cartao,
            _nota,
            _carta_frete,
            _emprestimo,
            _despesa,
            _cheque_pre,
            _vale,
            _transferencia
        ),
        UPPER(STRIP_ACCENTS(TRIM(CAST(observacao AS VARCHAR)))) AS observacao,
        _data_sangria AS data_sangria,
        _hora_sangria AS hora_sangria,
        _data_hora_coleta AS data_hora_coleta,
        _data_hora_atualizacao AS data_hora_atualizacao,
        _dinheiro AS dinheiro,
        _cheque AS cheque,
        _cartao AS cartao,
        _nota AS nota,
        _carta_frete AS carta_frete,
        _emprestimo AS emprestimo,
        _despesa AS despesa,
        _cheque_pre AS cheque_pre,
        _vale AS vale,
        _transferencia AS transferencia
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_sangrias_caixa.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);


-- 33. POSTOS CARTOES
COPY (
    WITH atual AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_cartoes.parquet',
            union_by_name = true
        )
    ),
    tipada AS (
        SELECT
            *,
            TRY_CAST(vencimento AS DATE) AS _vencimento,
            TRY_CAST(valor AS DECIMAL(18,2)) AS _valor,
            TRY_CAST(taxa_percentual AS DECIMAL(18,4)) AS _taxa_percentual,
            TRY_CAST(data_pagamento AS DATE) AS _data_pagamento,
            TRY_CAST(data_movimento AS DATE) AS _data_movimento,
            TRY_CAST(hora_movimento AS TIME) AS _hora_movimento,
            TRY_CAST(data_fiscal AS DATE) AS _data_fiscal
        FROM atual
    )
    SELECT
        * EXCLUDE (
            administradora_descricao,
            cliente_razao,
            centro_custo_descricao,
            tipo_inclusao,
            vencimento,
            valor,
            taxa_percentual,
            data_pagamento,
            data_movimento,
            hora_movimento,
            data_fiscal,
            _vencimento,
            _valor,
            _taxa_percentual,
            _data_pagamento,
            _data_movimento,
            _hora_movimento,
            _data_fiscal
        ),
        UPPER(STRIP_ACCENTS(TRIM(CAST(administradora_descricao AS VARCHAR)))) AS administradora_descricao,
        UPPER(STRIP_ACCENTS(TRIM(CAST(cliente_razao AS VARCHAR)))) AS cliente_razao,
        UPPER(STRIP_ACCENTS(TRIM(CAST(centro_custo_descricao AS VARCHAR)))) AS centro_custo_descricao,
        UPPER(STRIP_ACCENTS(TRIM(CAST(tipo_inclusao AS VARCHAR)))) AS tipo_inclusao,
        _vencimento AS vencimento,
        _valor AS valor,
        _taxa_percentual AS taxa_percentual,
        _data_pagamento AS data_pagamento,
        _data_movimento AS data_movimento,
        _hora_movimento AS hora_movimento,
        _data_fiscal AS data_fiscal
    FROM tipada
)
TO '//10.68.152.218/d$/Pipeline/Gold/postos_cartoes.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);