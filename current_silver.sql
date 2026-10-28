-- ============================================================
-- POMBO DATA LAKE - SILVER CURRENT - PADRÃO SNAKE_CASE
--
-- Regras desta camada:
--   1. A Bronze permanece com os nomes originais do sistema-fonte.
--   2. A Silver Current grava colunas em snake_case.
--   3. Os arquivos/tabelas da Silver em diante usam snake_case.
--   4. A deduplicação continua usando as colunas originais da Bronze.
--   5. Mantém o evento mais recente por chave de negócio.
--   6. Preserva os metadados técnicos para controle da sincronização:
--      _source_id, _source_name, _entity_id, _entity, _event_table,
--      _operation, _event_id, _batch_id e _ingestion_timestamp.
-- ============================================================

-- ------------------------------------------------------------
-- contas_pagar_receber
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        Tipo,
        NumLancamento,
        CodEnt,
        NumDocto,
        NumLote,
        DtEmissao,
        DtVenctoOrig,
        DtVencto,
        Moeda,
        ValorOrig,
        Saldo,
        Historico,
        SeqNF,
        Parcela,
        CodTpCobr,
        CGC,
        CodRec,
        RzSocial,
        Observ,
        ValorPago,
        NumeroNF,
        NumPedido,
        CCentidade,
        CCCustoVenda,
        CCEstoque,
        Custo,
        CCDspCom,
        CCComAPg,
        ValComissao,
        Usuario,
        Comprador,
        RzSocialComprador,
        Sinistro,
        TxJurosDia,
        NossoNumero,
        BaixaEletronica,
        BaixaConfirmada,
        PostergacaoConfirmada,
        SeqCC,
        ProcessadoPelaTabelaPreco,
        Competencia,
        Convenio,
        Cartorio,
        Previsao,
        SeqContabilizacao,
        PodeContabilizar,
        SeqLoteReal,
        SeqPadrao,
        SeqDocto,
        Banco,
        Agencia,
        Conta,
        ContadorSeqDocto,
        ValorBruto,
        SequencialReneg,
        Renegociado,
        InfoAdic,
        ComandoAuxiliar,
        ChequeDevolvido,
        GeraAdiantamento,
        ContabilizaReal,
        FatDFin,
        ValFat,
        Autorizado,
        AutorizadoPor,
        AutorizadoEm,
        DigAgencia,
        DigConta,
        CR_NossoNumero,
        SeqBanco,
        CodEmpresaAdto,
        SeqCreditoAdto,
        ComAdtoQuantificado,
        UsuarioCancelamento,
        DataCancelamento,
        SeqContrato,
        DataInclusao,
        Carteira,
        ValorAcrescimoTipoCobr,
        DataCancelamentoLote,
        SeqCPRRecorrente,
        UtilizaConversao,
        TotalParcelas,
        SequencialProcessoPMM,
        SequencialEtapaPMM,
        ValorContas,
        ContabilizaNaNF,
        vendaCodigo,
        duplicataCodigo,
        tipoOriginal,
        dataPagamento,
        planoContaGerencialCodigo,
        convertido,
        tituloNumero,
        dataHoraAtualizacao,
        codigo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumLancamento
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/contaspagarreceber/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        Tipo AS tipo,
        NumLancamento AS num_lancamento,
        CodEnt AS cod_ent,
        NumDocto AS num_docto,
        NumLote AS num_lote,
        DtEmissao AS dt_emissao,
        DtVenctoOrig AS dt_vencto_orig,
        DtVencto AS dt_vencto,
        Moeda AS moeda,
        ValorOrig AS valor_orig,
        Saldo AS saldo,
        Historico AS historico,
        SeqNF AS seq_nf,
        Parcela AS parcela,
        CodTpCobr AS cod_tp_cobr,
        CGC AS cgc,
        CodRec AS cod_rec,
        RzSocial AS rz_social,
        Observ AS observ,
        ValorPago AS valor_pago,
        NumeroNF AS numero_nf,
        NumPedido AS num_pedido,
        CCentidade AS cc_entidade,
        CCCustoVenda AS cc_custo_venda,
        CCEstoque AS cc_estoque,
        Custo AS custo,
        CCDspCom AS cc_dsp_com,
        CCComAPg AS cc_com_a_pg,
        ValComissao AS val_comissao,
        Usuario AS usuario,
        Comprador AS comprador,
        RzSocialComprador AS rz_social_comprador,
        Sinistro AS sinistro,
        TxJurosDia AS tx_juros_dia,
        NossoNumero AS nosso_numero,
        BaixaEletronica AS baixa_eletronica,
        BaixaConfirmada AS baixa_confirmada,
        PostergacaoConfirmada AS postergacao_confirmada,
        SeqCC AS seq_cc,
        ProcessadoPelaTabelaPreco AS processado_pela_tabela_preco,
        Competencia AS competencia,
        Convenio AS convenio,
        Cartorio AS cartorio,
        Previsao AS previsao,
        SeqContabilizacao AS seq_contabilizacao,
        PodeContabilizar AS pode_contabilizar,
        SeqLoteReal AS seq_lote_real,
        SeqPadrao AS seq_padrao,
        SeqDocto AS seq_docto,
        Banco AS banco,
        Agencia AS agencia,
        Conta AS conta,
        ContadorSeqDocto AS contador_seq_docto,
        ValorBruto AS valor_bruto,
        SequencialReneg AS sequencial_reneg,
        Renegociado AS renegociado,
        InfoAdic AS info_adic,
        ComandoAuxiliar AS comando_auxiliar,
        ChequeDevolvido AS cheque_devolvido,
        GeraAdiantamento AS gera_adiantamento,
        ContabilizaReal AS contabiliza_real,
        FatDFin AS fat_d_fin,
        ValFat AS val_fat,
        Autorizado AS autorizado,
        AutorizadoPor AS autorizado_por,
        AutorizadoEm AS autorizado_em,
        DigAgencia AS dig_agencia,
        DigConta AS dig_conta,
        CR_NossoNumero AS cr_nosso_numero,
        SeqBanco AS seq_banco,
        CodEmpresaAdto AS cod_empresa_adto,
        SeqCreditoAdto AS seq_credito_adto,
        ComAdtoQuantificado AS com_adto_quantificado,
        UsuarioCancelamento AS usuario_cancelamento,
        DataCancelamento AS data_cancelamento,
        SeqContrato AS seq_contrato,
        DataInclusao AS data_inclusao,
        Carteira AS carteira,
        ValorAcrescimoTipoCobr AS valor_acrescimo_tipo_cobr,
        DataCancelamentoLote AS data_cancelamento_lote,
        SeqCPRRecorrente AS seq_cpr_recorrente,
        UtilizaConversao AS utiliza_conversao,
        TotalParcelas AS total_parcelas,
        SequencialProcessoPMM AS sequencial_processo_pmm,
        SequencialEtapaPMM AS sequencial_etapa_pmm,
        ValorContas AS valor_contas,
        ContabilizaNaNF AS contabiliza_na_nf,
        vendaCodigo AS venda_codigo,
        duplicataCodigo AS duplicata_codigo,
        tipoOriginal AS tipo_original,
        dataPagamento AS data_pagamento,
        planoContaGerencialCodigo AS plano_conta_gerencial_codigo,
        convertido AS convertido,
        tituloNumero AS titulo_numero,
        dataHoraAtualizacao AS data_hora_atualizacao,
        codigo AS codigo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/contas_pagar_receber.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'contas_pagar_receber'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('contaspagarreceber', '_', ''));


-- ------------------------------------------------------------
-- contas_pagar_receber_moeda
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        NumLancamento,
        ValorBrutoMoeda,
        ValorLiquidoMoeda,
        SaldoMoeda,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumLancamento
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/contaspagarreceberMoeda/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        NumLancamento AS num_lancamento,
        ValorBrutoMoeda AS valor_bruto_moeda,
        ValorLiquidoMoeda AS valor_liquido_moeda,
        SaldoMoeda AS saldo_moeda,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/contas_pagar_receber_moeda.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'contas_pagar_receber_moeda'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('contaspagarreceberMoeda', '_', ''));


-- ------------------------------------------------------------
-- nota_fiscal
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        Sequencial,
        CodEmpresa,
        NumeroNF,
        DataNF,
        CodNatOp,
        ValFin,
        NumLote,
        CodEnt,
        ChaveAcesso,
        CodTransp,
        SeqFrota,
        Documento,
        NumConhecimento,
        PlacaVeiTransp,
        CARRETA,
        ValProd,
        BaseICMS,
        ValICMS,
        BaseICMSSubst,
        ICMSSubst,
        ValIPI,
        ValFrete,
        ValDesconto,
        ValSeguro,
        ValDespAcess,
        DataMovto,
        Tipo,
        ES,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, Sequencial
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/notafiscal/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        Sequencial AS sequencial,
        CodEmpresa AS cod_empresa,
        NumeroNF AS numero_nf,
        DataNF AS data_nf,
        CodNatOp AS cod_nat_op,
        ValFin AS val_fin,
        NumLote AS num_lote,
        CodEnt AS cod_ent,
        ChaveAcesso AS chave_acesso,
        CodTransp AS cod_transp,
        SeqFrota AS seq_frota,
        Documento AS documento,
        NumConhecimento AS num_conhecimento,
        PlacaVeiTransp AS placa_vei_transp,
        CARRETA AS carreta,
        ValProd AS val_prod,
        BaseICMS AS base_icms,
        ValICMS AS val_icms,
        BaseICMSSubst AS base_icmssubst,
        ICMSSubst AS icmssubst,
        ValIPI AS val_ipi,
        ValFrete AS val_frete,
        ValDesconto AS val_desconto,
        ValSeguro AS val_seguro,
        ValDespAcess AS val_desp_acess,
        DataMovto AS data_movto,
        ES AS es,
        Tipo AS tipo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/nota_fiscal.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'nota_fiscal'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('notafiscal', '_', ''));


-- ------------------------------------------------------------
-- itens_nf
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        Sequencial,
        CodProd,
        Descricao,
        Item,
        NumeroOC,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, Sequencial, Item
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/itensnf/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        Sequencial AS sequencial,
        CodProd AS cod_prod,
        Descricao AS descricao,
        Item AS item,
        NumeroOC AS numero_oc,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/itens_nf.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'itens_nf'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('itensnf', '_', ''));


-- ------------------------------------------------------------
-- movimento_plano_contas
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        NumLote,
        CodConta,
        ValorDebito,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumLote, CodConta
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/movimentoplanoContas/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        NumLote AS num_lote,
        CodConta AS cod_conta,
        ValorDebito AS valor_debito,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/movimento_plano_contas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'movimento_plano_contas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('movimentoplanoContas', '_', ''));


-- ------------------------------------------------------------
-- centro_custo_rateio
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        NumLote,
        SeqCC,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumLote, SeqCC
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/CentroCustoRateio/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        NumLote AS num_lote,
        SeqCC AS seq_cc,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/centro_custo_rateio.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'centro_custo_rateio'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('CentroCustoRateio', '_', ''));


-- ------------------------------------------------------------
-- centro_custo_pagar
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        NumLancamento,
        SeqCC,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumLancamento, SeqCC
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/CentroCustoPagar/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        NumLancamento AS num_lancamento,
        SeqCC AS seq_cc,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/centro_custo_pagar.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'centro_custo_pagar'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('CentroCustoPagar', '_', ''));


-- ------------------------------------------------------------
-- z_nota_fiscal_classificacao_despesas
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        SequencialNF,
        ID_ClassificacaoDespesas,
        DetalhamentoDespesa,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, SequencialNF, ID_ClassificacaoDespesas
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Z_NotaFiscal_ClassificacaoDespesas/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        SequencialNF AS sequencial_nf,
        ID_ClassificacaoDespesas AS id_classificacao_despesas,
        DetalhamentoDespesa AS detalhamento_despesa,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/z_nota_fiscal_classificacao_despesas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'z_nota_fiscal_classificacao_despesas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Z_NotaFiscal_ClassificacaoDespesas', '_', ''));


-- ------------------------------------------------------------
-- empresas
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        NomeUsual,
        CGC,
        RazaoSocial,
        Endereco,
        Bairro,
        Numero,
        CEP,
        Cidade,
        Municipio,
        UF,
        Estado,
        EmpresaCodigoExterno,
        Sigla,
        TipoImposto,
        IE,
        InscricaoMunicipal,
        -- CNAE: origem e a tabela auxiliar EmpresasCNAE (fase 2); NULL ate la
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/empresas/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        NomeUsual AS nome_usual,
        CGC AS cgc,
        RazaoSocial AS razao_social,
        Endereco AS endereco,
        Bairro AS bairro,
        Numero AS numero,
        CEP AS cep,
        COALESCE(Cidade, Municipio) AS cidade,
        COALESCE(UF, Estado) AS uf,
        EmpresaCodigoExterno AS empresa_codigo_externo,
        Sigla AS sigla,
        TipoImposto AS tipo_imposto,
        IE AS ie,
        InscricaoMunicipal AS im,
        CAST(NULL AS VARCHAR) AS cnae, -- fase 2: join com EmpresasCNAE (Bronze)
        -- TODO fase 2: CNAE vem da tabela auxiliar EmpresasCNAE (Bronze) via LEFT JOIN
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/empresas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'empresas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('empresas', '_', ''));


-- ------------------------------------------------------------
-- entidade
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEnt,
        RzSocial,
        NomeUsual,
        ClienteReferencia,
        Endereco,
        CEP,
        Bairro,
        Municipio,
        Cidade,
        CodCidade,
        UF,
        CGC,
        Fone,
        Fax,
        IE,
        Vendedor,
        DataAlteracaoCad,
        DataCadastro,
        DataHoraAtualizacao,
        ConsumidorFinal,
        Status,
        Tipo,
        Comprador,
        CodTpCobr,
        CNPJPagamento,
        CNAE,
        InscricaoMunicipal,
        IM,
        RG,
        Suframa,
        Numero,
        Complemento,
        UsaLimiteLitros,
        LimiteLitros,
        UsaLimiteReais,
        LimiteReais,
        Bloqueado,
        UltimoUsuarioAlteracao,
        ClienteGrupoCodigo,
        ClienteCodigoExterno,
        Celular,
        OutroTelefone,
        Observacoes,
        Website,
        Pais,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEnt
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Entidade/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEnt AS cod_ent,
        RzSocial AS rz_social,
        NomeUsual AS nome_usual,
        ClienteReferencia AS cliente_referencia,
        Endereco AS endereco,
        CEP AS cep,
        Bairro AS bairro,
        COALESCE(Municipio, Cidade) AS municipio,
        CodCidade AS cod_cidade,
        UF AS uf,
        CGC AS cgc,
        Fone AS fone,
        Fax AS fax,
        IE AS ie,
        Vendedor AS vendedor,
        DataAlteracaoCad AS data_alteracao_cad,
        DataCadastro AS data_cadastro,
        DataHoraAtualizacao AS data_hora_atualizacao,
        ConsumidorFinal AS consumidor_final,
        Status AS status,
        Tipo AS tipo,
        Comprador AS comprador,
        CodTpCobr AS cod_tp_cobr,
        CNPJPagamento AS cnpj_pagamento,
        CNAE AS cnae,
        COALESCE(InscricaoMunicipal, IM) AS inscricao_municipal,
        RG AS rg,
        Suframa AS suframa,
        Numero AS numero,
        Complemento AS complemento,
        UsaLimiteLitros AS usa_limite_litros,
        LimiteLitros AS limite_litros,
        UsaLimiteReais AS usa_limite_reais,
        LimiteReais AS limite_reais,
        Bloqueado AS bloqueado,
        UltimoUsuarioAlteracao AS ultimo_usuario_alteracao,
        ClienteGrupoCodigo AS cliente_grupo_codigo,
        ClienteCodigoExterno AS cliente_codigo_externo,
        Celular AS celular,
        OutroTelefone AS outro_telefone,
        Observacoes AS observacoes,
        Website AS website,
        Pais AS pais,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/entidade.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'entidade'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Entidade', '_', ''));


-- ------------------------------------------------------------
-- vendedor
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodVend,
        Descricao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodVend
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Vendedor/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodVend AS cod_vend,
        Descricao AS descricao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/vendedor.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'vendedor'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Vendedor', '_', ''));


-- ------------------------------------------------------------
-- tipo_documento
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        SeqDocto,
        GNRE,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, SeqDocto
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/TipoDocumento/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        SeqDocto AS seq_docto,
        GNRE AS gnre,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/tipo_documento.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'tipo_documento'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('TipoDocumento', '_', ''));


-- ------------------------------------------------------------
-- natureza_operacao
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodNatOp,
        Descricao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodNatOp
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/NaturezaOperacao/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodNatOp AS cod_nat_op,
        Descricao AS descricao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/natureza_operacao.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'natureza_operacao'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('NaturezaOperacao', '_', ''));


-- ------------------------------------------------------------
-- z_classificacao_despesas
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        ID,
        Descricao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, ID
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/z_classificacaodespesas/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        ID AS id,
        Descricao AS descricao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/z_classificacao_despesas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'z_classificacao_despesas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('z_classificacaodespesas', '_', ''));


-- ------------------------------------------------------------
-- plano_contas
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodConta,
        Descricao,
        CodCompl,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodConta
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/planocontas/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodConta AS cod_conta,
        Descricao AS descricao,
        CodCompl AS cod_compl,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/plano_contas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'plano_contas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('planocontas', '_', ''));


-- ------------------------------------------------------------
-- portador
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodPort,
        BloqueiaBaixa,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodPort
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Portador/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodPort AS cod_port,
        BloqueiaBaixa AS bloqueia_baixa,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/portador.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'portador'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Portador', '_', ''));


-- ------------------------------------------------------------
-- centro_custo
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        SeqCC,
        Descricao,
        MascaraOrdenacao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, SeqCC
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/CentroCusto/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        SeqCC AS seq_cc,
        Descricao AS descricao,
        MascaraOrdenacao AS mascara_ordenacao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/centro_custo.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'centro_custo'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('CentroCusto', '_', ''));


-- ------------------------------------------------------------
-- pedido
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        "CodEmpresa",
        "NumPed",
        "Tipo",
        "CodEnt",
        "RefPed",
        "DataPed",
        "Usuario",
        "Desconto",
        "Observ",
        "StatusPedido",
        "ValorMerc",
        "CodNatOp",
        "FreteInc",
        "ValFrete",
        "CodTpCobr",
        "TabPreco",
        "ValPedido",
        "DtPrevFat",
        "CodTransp",
        "CodEntrega",
        "Vendedor",
        "CtaContab",
        "LibFinanc",
        "LibVenda",
        "CodCobranca",
        "StatusFat",
        "RazaoSocial",
        "LibFinancEm",
        "LibFinancPor",
        "LibVendasEm",
        "LibVendasPor",
        "GeraNumeroSerie",
        "QtdePedido",
        "QtdePendente",
        "QtdeNaoAtendida",
        "QtdeSeparada",
        "QtdeFaturada",
        "EnderecoCobranca",
        "BairroCobranca",
        "CEPCobranca",
        "MunicipioCobranca",
        "UFCobranca",
        "EnderecoEntrega",
        "BairroEntrega",
        "CEPEntrega",
        "MunicipioEntrega",
        "UFEntrega",
        "EntregaParcial",
        "SerieNF",
        "EnderecoFat",
        "BairroFat",
        "CEPFat",
        "MunicipioFat",
        "UFFat",
        "ProntoFaturar",
        "CodPg",
        "PrazoEntrega",
        "PercICMS",
        "RefPedVenda",
        "ObsLongo",
        "RetiraEntrega",
        "Aprovado",
        "DepositoPadrao",
        "TemPendencia",
        "PrazoPagto",
        "ForaEstado",
        "segurado",
        "DataCancelamento",
        "UsuarioCancelamento",
        "ValorPendente",
        "ValorCredito",
        "ValorBloqueado",
        "ValorLiberado",
        "Documento",
        "descrTransp",
        "placaVei",
        "carreta",
        "capacidade",
        "documentoTransp",
        "treminhao",
        "tipoTransp",
        "AprovTransp",
        "AprovTranspEm",
        "AprovTranspPor",
        "CodCodTransp",
        "SeqFrota",
        "SeqFrotaCarreta",
        "SeqFrotaTrem",
        "valorCreditoBloqueado",
        "CodVendAssist",
        "SeqMotivoCancto",
        "CodTranspAnterior",
        "SeqFrotaAnterior",
        "SeqCarretaAnterior",
        "SeqTreminhaoAnterior",
        "CargaFechada",
        "CR_UsuarioPreCredito",
        "CR_DataPreCredito",
        "DataMovto",
        "PMM_CodEmpresaOrigemOC",
        "ValorTabela",
        "PMM_ControlaQtdeAdto",
        "CodSetor",
        "NumeroEntrega",
        "ComplementoEntrega",
        "NumeroCobranca",
        "ComplementoCobranca",
        "NumeroFat",
        "ComplementoFat",
        "CodigoDNCEntrega",
        "CodigoDNCFat",
        "ObsAprovacao",
        "SeqCarga",
        "DPMP",
        "ForcaDepDestino",
        "DepositoDestino",
        "PMM_Tipo",
        "Contrato",
        "Suspenso",
        "PesoTotal",
        "CodEntCorretora",
        "CodEntForneCarregto",
        "IDConsultaOnLine",
        "id",
        "web_origem_pedido",
        "TipoOperacao",
        "TipoOperacaoCalculado",
        "AprovacaoAutomaticaVendas",
        "ES",
        "AjusteEstoque",
        "IncluiPreAdiantamento",
        "Finalizado",
        "QtdeTransferidaEntrada",
        "QtdeTransferidaSaida",
        "AprovadoClienteGACNaoOK",
        "ServicoAnaliseAprovacaoVD",
        "ServicoAnaliseAprovacaoVDData",
        "IDTabelaPreco",
        "IDTabelaPrecoAuxUsu",
        "ValorIPI",
        "ID_PIX",
        "DepositoAlterado",
        "IDTabelaPrecoNovoDep",
        "DataInicioExecucao",
        "DataFimExecucao",
        "CNPJEnt",
        "RzSocialEnt",
        "DescontoCondicionalTipo",
        "CMV_CustoMedioSaida",
        "CodTipoAntecipacao",
        "ID_ClassificacaoDespesas",
        "DetalhamentoDespesa",
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumPed
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Pedido/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        "CodEmpresa" AS cod_empresa,
        "NumPed" AS num_ped,
        "Tipo" AS tipo,
        "CodEnt" AS cod_ent,
        "RefPed" AS ref_ped,
        "DataPed" AS data_ped,
        "Usuario" AS usuario,
        "Desconto" AS desconto,
        "Observ" AS observ,
        "StatusPedido" AS status_pedido,
        "ValorMerc" AS valor_merc,
        "CodNatOp" AS cod_nat_op,
        "FreteInc" AS frete_inc,
        "ValFrete" AS val_frete,
        "CodTpCobr" AS cod_tp_cobr,
        "TabPreco" AS tab_preco,
        "ValPedido" AS val_pedido,
        "DtPrevFat" AS dt_prev_fat,
        "CodTransp" AS cod_transp,
        "CodEntrega" AS cod_entrega,
        "Vendedor" AS vendedor,
        "CtaContab" AS cta_contab,
        "LibFinanc" AS lib_financ,
        "LibVenda" AS lib_venda,
        "CodCobranca" AS cod_cobranca,
        "StatusFat" AS status_fat,
        "RazaoSocial" AS razao_social,
        "LibFinancEm" AS lib_financ_em,
        "LibFinancPor" AS lib_financ_por,
        "LibVendasEm" AS lib_vendas_em,
        "LibVendasPor" AS lib_vendas_por,
        "GeraNumeroSerie" AS gera_numero_serie,
        "QtdePedido" AS qtde_pedido,
        "QtdePendente" AS qtde_pendente,
        "QtdeNaoAtendida" AS qtde_nao_atendida,
        "QtdeSeparada" AS qtde_separada,
        "QtdeFaturada" AS qtde_faturada,
        "EnderecoCobranca" AS endereco_cobranca,
        "BairroCobranca" AS bairro_cobranca,
        "CEPCobranca" AS cep_cobranca,
        "MunicipioCobranca" AS municipio_cobranca,
        "UFCobranca" AS uf_cobranca,
        "EnderecoEntrega" AS endereco_entrega,
        "BairroEntrega" AS bairro_entrega,
        "CEPEntrega" AS cep_entrega,
        "MunicipioEntrega" AS municipio_entrega,
        "UFEntrega" AS uf_entrega,
        "EntregaParcial" AS entrega_parcial,
        "SerieNF" AS serie_nf,
        "EnderecoFat" AS endereco_fat,
        "BairroFat" AS bairro_fat,
        "CEPFat" AS cep_fat,
        "MunicipioFat" AS municipio_fat,
        "UFFat" AS uf_fat,
        "ProntoFaturar" AS pronto_faturar,
        "CodPg" AS cod_pg,
        "PrazoEntrega" AS prazo_entrega,
        "PercICMS" AS perc_icms,
        "RefPedVenda" AS ref_ped_venda,
        "ObsLongo" AS obs_longo,
        "RetiraEntrega" AS retira_entrega,
        "Aprovado" AS aprovado,
        "DepositoPadrao" AS deposito_padrao,
        "TemPendencia" AS tem_pendencia,
        "PrazoPagto" AS prazo_pagto,
        "ForaEstado" AS fora_estado,
        "segurado",
        "DataCancelamento" AS data_cancelamento,
        "UsuarioCancelamento" AS usuario_cancelamento,
        "ValorPendente" AS valor_pendente,
        "ValorCredito" AS valor_credito,
        "ValorBloqueado" AS valor_bloqueado,
        "ValorLiberado" AS valor_liberado,
        "Documento" AS documento,
        "descrTransp" AS descr_transp,
        "placaVei" AS placa_vei,
        "carreta",
        "capacidade",
        "documentoTransp" AS documento_transp,
        "treminhao",
        "tipoTransp" AS tipo_transp,
        "AprovTransp" AS aprov_transp,
        "AprovTranspEm" AS aprov_transp_em,
        "AprovTranspPor" AS aprov_transp_por,
        "CodCodTransp" AS cod_cod_transp,
        "SeqFrota" AS seq_frota,
        "SeqFrotaCarreta" AS seq_frota_carreta,
        "SeqFrotaTrem" AS seq_frota_trem,
        "valorCreditoBloqueado" AS valor_credito_bloqueado,
        "CodVendAssist" AS cod_vend_assist,
        "SeqMotivoCancto" AS seq_motivo_cancto,
        "CodTranspAnterior" AS cod_transp_anterior,
        "SeqFrotaAnterior" AS seq_frota_anterior,
        "SeqCarretaAnterior" AS seq_carreta_anterior,
        "SeqTreminhaoAnterior" AS seq_treminhao_anterior,
        "CargaFechada" AS carga_fechada,
        "CR_UsuarioPreCredito" AS cr_usuario_pre_credito,
        "CR_DataPreCredito" AS cr_data_pre_credito,
        "DataMovto" AS data_movto,
        "PMM_CodEmpresaOrigemOC" AS pmm_cod_empresa_origem_oc,
        "ValorTabela" AS valor_tabela,
        "PMM_ControlaQtdeAdto" AS pmm_controla_qtde_adto,
        "CodSetor" AS cod_setor,
        "NumeroEntrega" AS numero_entrega,
        "ComplementoEntrega" AS complemento_entrega,
        "NumeroCobranca" AS numero_cobranca,
        "ComplementoCobranca" AS complemento_cobranca,
        "NumeroFat" AS numero_fat,
        "ComplementoFat" AS complemento_fat,
        "CodigoDNCEntrega" AS codigo_dnc_entrega,
        "CodigoDNCFat" AS codigo_dnc_fat,
        "ObsAprovacao" AS obs_aprovacao,
        "SeqCarga" AS seq_carga,
        "DPMP" AS dpmp,
        "ForcaDepDestino" AS forca_dep_destino,
        "DepositoDestino" AS deposito_destino,
        "PMM_Tipo" AS pmm_tipo,
        "Contrato" AS contrato,
        "Suspenso" AS suspenso,
        "PesoTotal" AS peso_total,
        "CodEntCorretora" AS cod_ent_corretora,
        "CodEntForneCarregto" AS cod_ent_forne_carregto,
        "IDConsultaOnLine" AS id_consulta_on_line,
        "id",
        "web_origem_pedido",
        "TipoOperacao" AS tipo_operacao,
        "TipoOperacaoCalculado" AS tipo_operacao_calculado,
        "AprovacaoAutomaticaVendas" AS aprovacao_automatica_vendas,
        "ES" AS es,
        "AjusteEstoque" AS ajuste_estoque,
        "IncluiPreAdiantamento" AS inclui_pre_adiantamento,
        "Finalizado" AS finalizado,
        "QtdeTransferidaEntrada" AS qtde_transferida_entrada,
        "QtdeTransferidaSaida" AS qtde_transferida_saida,
        "AprovadoClienteGACNaoOK" AS aprovado_cliente_gac_nao_ok,
        "ServicoAnaliseAprovacaoVD" AS servico_analise_aprovacao_vd,
        "ServicoAnaliseAprovacaoVDData" AS servico_analise_aprovacao_vd_data,
        "IDTabelaPreco" AS id_tabela_preco,
        "IDTabelaPrecoAuxUsu" AS id_tabela_preco_aux_usu,
        "ValorIPI" AS valor_ipi,
        "ID_PIX" AS id_pix,
        "DepositoAlterado" AS deposito_alterado,
        "IDTabelaPrecoNovoDep" AS id_tabela_preco_novo_dep,
        "DataInicioExecucao" AS data_inicio_execucao,
        "DataFimExecucao" AS data_fim_execucao,
        "CNPJEnt" AS cnpj_ent,
        "RzSocialEnt" AS rz_social_ent,
        "DescontoCondicionalTipo" AS desconto_condicional_tipo,
        "CMV_CustoMedioSaida" AS cmv_custo_medio_saida,
        "CodTipoAntecipacao" AS cod_tipo_antecipacao,
        "ID_ClassificacaoDespesas" AS id_classificacao_despesas,
        "DetalhamentoDespesa" AS detalhamento_despesa,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/pedido.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'pedido'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Pedido', '_', ''));


-- ------------------------------------------------------------
-- itens_pedido
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        NumPed,
        Seq,
        CodProd,
        Qtde,
        QtdePendente,
        QtdeSeparada,
        PrecoFOB,
        PrecoCIF,
        CustoCIFSI,
        Tipo,
        Tabela,
        QtdeFaturada,
        DepositoEntrada,
        DepositoSaida,
        QtdeNaoAtendida,
        CodEnt,
        PercDesconto,
        TipoProd,
        CancelamentoParcial,
        ValorIPI,
        DtPrevEntrega,
        CodUnid,
        QtdeDevolv,
        ValorPendente,
        DoctoClienteFaturado,
        IECliente,
        ConsumidorFinal,
        FreteUnitPadrao,
        FreteUnitReal,
        FatorConversao,
        Confirmado,
        NumeroOC,
        PercentualDesconto,
        SeqCarga,
        PrecoTabela,
        CustoMedio,
        PMM_QtdePaga,
        PrecoMinimoAVista,
        ValorDescontoCondicional,
        Custo,
        Custo_Alocado,
        PMM_SaldoExcedente,
        PMM_SaldoExcedenteNaoUtilizado,
        id,
        pedido_id,
        UFFat,
        ItemRefPed,
        QtdeTransferidaEntrada,
        QtdeTransferidaSaida,
        ItemOC,
        QtdeConvertida,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumPed, Seq
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/ItensPedido/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        NumPed AS num_ped,
        Seq AS seq,
        CodProd AS cod_prod,
        Qtde AS qtde,
        QtdePendente AS qtde_pendente,
        QtdeSeparada AS qtde_separada,
        PrecoFOB AS preco_fob,
        PrecoCIF AS preco_cif,
        CustoCIFSI AS custo_cifsi,
        Tipo AS tipo,
        Tabela AS tabela,
        QtdeFaturada AS qtde_faturada,
        DepositoEntrada AS deposito_entrada,
        DepositoSaida AS deposito_saida,
        QtdeNaoAtendida AS qtde_nao_atendida,
        CodEnt AS cod_ent,
        PercDesconto AS perc_desconto,
        TipoProd AS tipo_prod,
        CancelamentoParcial AS cancelamento_parcial,
        ValorIPI AS valor_ipi,
        DtPrevEntrega AS dt_prev_entrega,
        CodUnid AS cod_unid,
        QtdeDevolv AS qtde_devolv,
        ValorPendente AS valor_pendente,
        DoctoClienteFaturado AS docto_cliente_faturado,
        IECliente AS ie_cliente,
        ConsumidorFinal AS consumidor_final,
        FreteUnitPadrao AS frete_unit_padrao,
        FreteUnitReal AS frete_unit_real,
        FatorConversao AS fator_conversao,
        Confirmado AS confirmado,
        NumeroOC AS numero_oc,
        PercentualDesconto AS percentual_desconto,
        SeqCarga AS seq_carga,
        PrecoTabela AS preco_tabela,
        CustoMedio AS custo_medio,
        PMM_QtdePaga AS pmm_qtde_paga,
        PrecoMinimoAVista AS preco_minimo_a_vista,
        ValorDescontoCondicional AS valor_desconto_condicional,
        Custo AS custo,
        Custo_Alocado AS custo_alocado,
        PMM_SaldoExcedente AS pmm_saldo_excedente,
        PMM_SaldoExcedenteNaoUtilizado AS pmm_saldo_excedente_nao_utilizado,
        id,
        pedido_id,
        UFFat AS uf_fat,
        ItemRefPed AS item_ref_ped,
        QtdeTransferidaEntrada AS qtde_transferida_entrada,
        QtdeTransferidaSaida AS qtde_transferida_saida,
        ItemOC AS item_oc,
        QtdeConvertida AS qtde_convertida,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/itens_pedido.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'itens_pedido'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('ItensPedido', '_', ''));


-- ------------------------------------------------------------
-- cidade
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodigoDNC,
        Descricao,
        UF,
        PrecoMaxDiesel,
        Regiao,
        CodigoMunicipio,
        CodigoDNC_NFE,
        Urbano,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodigoDNC
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Cidade/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodigoDNC AS codigo_dnc,
        Descricao AS descricao,
        UF AS uf,
        PrecoMaxDiesel AS preco_max_diesel,
        Regiao AS regiao,
        CodigoMunicipio AS codigo_municipio,
        CodigoDNC_NFE AS codigo_dnc_nfe,
        Urbano AS urbano,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/cidade.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'cidade'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Cidade', '_', ''));


-- ------------------------------------------------------------
-- bandeira
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        codBandeira,
        descricao,
        BloqueadoVendas,
        BandeiraANP,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, codBandeira
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Bandeira/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        codBandeira AS cod_bandeira,
        descricao,
        BloqueadoVendas AS bloqueado_vendas,
        BandeiraANP AS bandeira_anp,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/bandeira.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'bandeira'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Bandeira', '_', ''));


-- ------------------------------------------------------------
-- baixa_antecipada
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        SeqCredito,
        NumDocto,
        Data,
        ValorOrig,
        Moeda,
        Historico,
        Saldo,
        NumCheque,
        DataCheque,
        DataFluxo,
        EmCheque,
        NumLote,
        CodEnt,
        RzSocial,
        CCBanco,
        CCEntidade,
        Tipo,
        Usuario,
        Status,
        CreditoDestino,
        CreditoOrigem,
        CodEntOrigem,
        comprador,
        rzSocialComprador,
        NaoIdentificado,
        bloqueado,
        dataDesbloqueio,
        DesbloqueadoPor,
        DesbloqueadoEm,
        CodEmpresaOrigemCreditoEE,
        SeqCreditoOrigemCreditoEE,
        CliFor,
        InfoAdic,
        NumPedCompra,
        PMM_PDAjuste_CodEmp,
        PMM_PDAjuste_NumPed,
        PMM_PDAjuste_Data,
        PMM_CodDep,
        PermiteRetiraEntrega,
        UsuarioCancelamento,
        DataCancelamento,
        DataSimples,
        PMM_DesprezaLibQtdePend,
        PagamentoEletronico,
        StatusProcPagElet,
        BloqueadoBaixa,
        ID_DepositoIdentifItens,
        DataInclusao,
        ID_PIXRecebido,
        ID,
        CodTipoAntecipacao,
        ID_ClassificacaoDespesas,
        DetalhamentoDespesa,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, SeqCredito
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/BaixaAntecipada/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        SeqCredito AS seq_credito,
        NumDocto AS num_docto,
        Data AS data,
        ValorOrig AS valor_orig,
        Moeda AS moeda,
        Historico AS historico,
        Saldo AS saldo,
        NumCheque AS num_cheque,
        DataCheque AS data_cheque,
        DataFluxo AS data_fluxo,
        EmCheque AS em_cheque,
        NumLote AS num_lote,
        CodEnt AS cod_ent,
        RzSocial AS rz_social,
        CCBanco AS cc_banco,
        CCEntidade AS cc_entidade,
        Tipo AS tipo,
        Usuario AS usuario,
        Status AS status,
        CreditoDestino AS credito_destino,
        CreditoOrigem AS credito_origem,
        CodEntOrigem AS cod_ent_origem,
        comprador,
        rzSocialComprador AS rz_social_comprador,
        NaoIdentificado AS nao_identificado,
        bloqueado,
        dataDesbloqueio AS data_desbloqueio,
        DesbloqueadoPor AS desbloqueado_por,
        DesbloqueadoEm AS desbloqueado_em,
        CodEmpresaOrigemCreditoEE AS cod_empresa_origem_credito_ee,
        SeqCreditoOrigemCreditoEE AS seq_credito_origem_credito_ee,
        CliFor AS cli_for,
        InfoAdic AS info_adic,
        NumPedCompra AS num_ped_compra,
        PMM_PDAjuste_CodEmp AS pmm_pd_ajuste_cod_emp,
        PMM_PDAjuste_NumPed AS pmm_pd_ajuste_num_ped,
        PMM_PDAjuste_Data AS pmm_pd_ajuste_data,
        PMM_CodDep AS pmm_cod_dep,
        PermiteRetiraEntrega AS permite_retira_entrega,
        UsuarioCancelamento AS usuario_cancelamento,
        DataCancelamento AS data_cancelamento,
        DataSimples AS data_simples,
        PMM_DesprezaLibQtdePend AS pmm_despreza_lib_qtde_pend,
        PagamentoEletronico AS pagamento_eletronico,
        StatusProcPagElet AS status_proc_pag_elet,
        BloqueadoBaixa AS bloqueado_baixa,
        ID_DepositoIdentifItens AS id_deposito_identif_itens,
        DataInclusao AS data_inclusao,
        ID_PIXRecebido AS id_pix_recebido,
        ID AS id,
        CodTipoAntecipacao AS cod_tipo_antecipacao,
        ID_ClassificacaoDespesas AS id_classificacao_despesas,
        DetalhamentoDespesa AS detalhamento_despesa,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/baixa_antecipada.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'baixa_antecipada'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('BaixaAntecipada', '_', ''));


-- ------------------------------------------------------------
-- itens_baixa
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        NumBaixa,
        Item,
        ValorOriginal,
        ValorDesc,
        ValorAcrescimo,
        ValorMovimento,
        NumLancamento,
        Cancelado,
        CodEnt,
        SeqCredito,
        ValCredito,
        valCreditoGerado,
        SeqCreditoGerado,
        Bloqueado,
        NaoIdentificado,
        DtBaixa,
        ValorDevolvido,
        Historico,
        InfoAdic,
        OutrosDescontos_Valor,
        ID,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumBaixa, Item
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/ItensBaixa/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        NumBaixa AS num_baixa,
        Item AS item,
        ValorOriginal AS valor_original,
        ValorDesc AS valor_desc,
        ValorAcrescimo AS valor_acrescimo,
        ValorMovimento AS valor_movimento,
        NumLancamento AS num_lancamento,
        Cancelado AS cancelado,
        CodEnt AS cod_ent,
        SeqCredito AS seq_credito,
        ValCredito AS val_credito,
        valCreditoGerado AS val_credito_gerado,
        SeqCreditoGerado AS seq_credito_gerado,
        Bloqueado AS bloqueado,
        NaoIdentificado AS nao_identificado,
        DtBaixa AS dt_baixa,
        ValorDevolvido AS valor_devolvido,
        Historico AS historico,
        InfoAdic AS info_adic,
        OutrosDescontos_Valor AS outros_descontos_valor,
        ID AS id,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/itens_baixa.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'itens_baixa'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('ItensBaixa', '_', ''));


-- ------------------------------------------------------------
-- baixa
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodEmpresa,
        NumBaixa,
        CCBaixa,
        DtBaixa,
        NumDocto,
        Observacao,
        ValorMovimentado,
        ValorDesconto,
        ValorAcrescimo,
        ValorOriginal,
        CCAcrescimo,
        CCDesconto,
        NumCheque,
        DataCheque,
        DataFluxo,
        ItensBaixa,
        NumCredito,
        NumLoteBX,
        Cancelado,
        EmCheque,
        Tipo,
        Moeda,
        Usuario,
        ValCredito,
        SeqCaucao,
        Bloqueado,
        DataProgDesbloqueio,
        UsuarioDesbloqueio,
        DataDesbloqueio,
        DataInclusaoBaixa,
        valCreditoGerado,
        DataCancelamento,
        SeqLoteReal,
        SeqDocto,
        InfoAdic,
        UsuarioCancto,
        DataCanctoUsu,
        Observacoes,
        OutrosDescontos_Valor,
        SeqBanco,
        NumAgencia,
        ID_PIXRecebido,
        ID,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodEmpresa, NumBaixa
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Baixa/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodEmpresa AS cod_empresa,
        NumBaixa AS num_baixa,
        CCBaixa AS cc_baixa,
        DtBaixa AS dt_baixa,
        NumDocto AS num_docto,
        Observacao AS observacao,
        ValorMovimentado AS valor_movimentado,
        ValorDesconto AS valor_desconto,
        ValorAcrescimo AS valor_acrescimo,
        ValorOriginal AS valor_original,
        CCAcrescimo AS cc_acrescimo,
        CCDesconto AS cc_desconto,
        NumCheque AS num_cheque,
        DataCheque AS data_cheque,
        DataFluxo AS data_fluxo,
        ItensBaixa AS itens_baixa,
        NumCredito AS num_credito,
        NumLoteBX AS num_lote_bx,
        Cancelado AS cancelado,
        EmCheque AS em_cheque,
        Tipo AS tipo,
        Moeda AS moeda,
        Usuario AS usuario,
        ValCredito AS val_credito,
        SeqCaucao AS seq_caucao,
        Bloqueado AS bloqueado,
        DataProgDesbloqueio AS data_prog_desbloqueio,
        UsuarioDesbloqueio AS usuario_desbloqueio,
        DataDesbloqueio AS data_desbloqueio,
        DataInclusaoBaixa AS data_inclusao_baixa,
        valCreditoGerado AS val_credito_gerado,
        DataCancelamento AS data_cancelamento,
        SeqLoteReal AS seq_lote_real,
        SeqDocto AS seq_docto,
        InfoAdic AS info_adic,
        UsuarioCancto AS usuario_cancto,
        DataCanctoUsu AS data_cancto_usu,
        Observacoes AS observacoes,
        OutrosDescontos_Valor AS outros_descontos_valor,
        SeqBanco AS seq_banco,
        NumAgencia AS num_agencia,
        ID_PIXRecebido AS id_pix_recebido,
        ID AS id,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/baixa.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'baixa'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Baixa', '_', ''));


-- ------------------------------------------------------------
-- produto
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodProd,
        DescricaoCompleta,
        DescricaoNF,
        Classific,
        UCC,
        CMC,
        TipoProd,
        CodIPI,
        PesoBruto,
        PesoLiq,
        CodUnid,
        PrecoBase,
        CMF,
        UCF,
        Moeda,
        CodICMS,
        MenorFOB,
        MenorCIF,
        MaiorFOB,
        MaiorCIF,
        CMCSI,
        Especie,
        Marca,
        CdOrgMerc,
        AlicICMS,
        Localiza,
        Ativo,
        ControlaNumeroSerie,
        UnidadePorSerie,
        NomeUsual,
        CST,
        PercVdaPis,
        PercVdaCOF,
        CodNatOpVendaEstado,
        CodNatOpVendaFEstado,
        CodigoBarra,
        CustoBaseNF,
        CodigoDCP,
        CodigoSicopi,
        CodigoNBM,
        CodigoNCM,
        Aplicacao,
        CodProdTanque,
        ProporcaoDCP,
        DPMP_Codigo,
        TipoItem,
        Observacoes,
        CodigoDNF,
        ANPComerc_CodigoProduto,
        NAT_REC,
        CEST,
        DescricaoReduzida,
        ID,
        ANP_Descricao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodProd
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Produto/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodProd AS cod_prod,
        DescricaoCompleta AS descricao_completa,
        DescricaoNF AS descricao_nf,
        Classific AS classific,
        UCC AS ucc,
        CMC AS cmc,
        TipoProd AS tipo_prod,
        CodIPI AS cod_ipi,
        PesoBruto AS peso_bruto,
        PesoLiq AS peso_liq,
        CodUnid AS cod_unid,
        PrecoBase AS preco_base,
        CMF AS cmf,
        UCF AS ucf,
        Moeda AS moeda,
        CodICMS AS cod_icms,
        MenorFOB AS menor_fob,
        MenorCIF AS menor_cif,
        MaiorFOB AS maior_fob,
        MaiorCIF AS maior_cif,
        CMCSI AS cmcsi,
        Especie AS especie,
        Marca AS marca,
        CdOrgMerc AS cd_org_merc,
        AlicICMS AS alic_icms,
        Localiza AS localiza,
        Ativo AS ativo,
        ControlaNumeroSerie AS controla_numero_serie,
        UnidadePorSerie AS unidade_por_serie,
        NomeUsual AS nome_usual,
        CST AS cst,
        PercVdaPis AS perc_vda_pis,
        PercVdaCOF AS perc_vda_cof,
        CodNatOpVendaEstado AS cod_nat_op_venda_estado,
        CodNatOpVendaFEstado AS cod_nat_op_venda_f_estado,
        CodigoBarra AS codigo_barra,
        CustoBaseNF AS custo_base_nf,
        CodigoDCP AS codigo_dcp,
        CodigoSicopi AS codigo_sicopi,
        CodigoNBM AS codigo_nbm,
        CodigoNCM AS codigo_ncm,
        Aplicacao AS aplicacao,
        CodProdTanque AS cod_prod_tanque,
        ProporcaoDCP AS proporcao_dcp,
        DPMP_Codigo AS dpmp_codigo,
        TipoItem AS tipo_item,
        Observacoes AS observacoes,
        CodigoDNF AS codigo_dnf,
        ANPComerc_CodigoProduto AS anp_comerc_codigo_produto,
        NAT_REC AS nat_rec,
        CEST AS cest,
        DescricaoReduzida AS descricao_reduzida,
        ID AS id,
        ANP_Descricao AS anp_descricao,
        CAST(NULL AS VARCHAR) AS referencia_codigo,
        CAST(NULL AS BIGINT) AS grupo_codigo,
        CAST(NULL AS BOOLEAN) AS combustivel,
        CAST(NULL AS BIGINT) AS produto_lmc_codigo,
        CAST(NULL AS VARCHAR) AS tipo_combustivel,
        CAST(NULL AS VARCHAR) AS unidade_compra,
        CAST(NULL AS BIGINT) AS sub_grupo1_codigo,
        CAST(NULL AS BIGINT) AS sub_grupo2_codigo,
        CAST(NULL AS BIGINT) AS sub_grupo3_codigo,
        CAST(NULL AS VARCHAR) AS produto_codigo_externo,
        CAST(NULL AS DECIMAL(18,4)) AS tributacao_ad_rem,
        CAST(NULL AS VARCHAR) AS descricao_fabricante,
        CAST(NULL AS VARCHAR) AS registra_inventario,
        CAST(NULL AS DECIMAL(18,4)) AS mistura_biocombustivel,
        CAST(NULL AS DECIMAL(18,4)) AS perc_uf_origem_scanc,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/produto.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'produto'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Produto', '_', ''));


-- ------------------------------------------------------------
-- deposito
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        CodDep,
        Descricao,
        Tipo,
        RzSocial,
        endereco,
        Numero,
        Complemento,
        Bairro,
        municipio,
        UF,
        CEP,
        CGC,
        IE,
        DepositoExterno,
        BasePropria,
        CodigoDNC,
        CodigoDCP,
        CodigoDCPRefinaria,
        Ativo,
        ParticipaDCP,
        PermiteVenda,
        CodProd,
        CodDepVirtual,
        CodEnt,
        Telefone,
        CodPais,
        Latitude,
        Longitude,
        IDPerfil,
        PerfilUtilizacao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, CodDep
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Deposito/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        CodDep AS cod_dep,
        Descricao AS descricao,
        Tipo AS tipo,
        RzSocial AS rz_social,
        endereco,
        Numero AS numero,
        Complemento AS complemento,
        Bairro AS bairro,
        municipio,
        UF AS uf,
        CEP AS cep,
        CGC AS cgc,
        IE AS ie,
        DepositoExterno AS deposito_externo,
        BasePropria AS base_propria,
        CodigoDNC AS codigo_dnc,
        CodigoDCP AS codigo_dcp,
        CodigoDCPRefinaria AS codigo_dcp_refinaria,
        Ativo AS ativo,
        ParticipaDCP AS participa_dcp,
        PermiteVenda AS permite_venda,
        CodProd AS cod_prod,
        CodDepVirtual AS cod_dep_virtual,
        CodEnt AS cod_ent,
        Telefone AS telefone,
        CodPais AS cod_pais,
        Latitude AS latitude,
        Longitude AS longitude,
        IDPerfil AS id_perfil,
        PerfilUtilizacao AS perfil_utilizacao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/deposito.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'deposito'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Deposito', '_', ''));


-- Tipo Cobranca

COPY
(
    WITH origem_rankeada AS
    (
        SELECT
            nome_alias,
            CodTpCobr,
            Descricao,
            _source_id,
            _source_name,
            _entity_id,
            _entity,
            _event_table,
            _operation,
            _event_id,
            _batch_id,
            _ingestion_timestamp,

            ROW_NUMBER() OVER
            (
                PARTITION BY
                    nome_alias,
                    CodTpCobr
                ORDER BY
                    _ingestion_timestamp DESC,
                    _event_id DESC
            ) AS _rn

        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/tipocobranca/**/*.parquet',
            union_by_name = true
        )
    )

    SELECT
        nome_alias,
        CodTpCobr AS cod_tp_cobr,
        Descricao AS descricao,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp

    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/tipo_cobranca.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
-- Atualiza todas as fontes ainda sem tabela_hub para esta entidade.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'tipo_cobranca'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('tipocobranca', '_', ''));


-- ------------------------------------------------------------
-- vendas_postos
-- ------------------------------------------------------------
-- INCREMENTAL POSTOS (anti-join)
-- Rank apenas sobre o delta Bronze novo; o Current é varrido uma
-- vez em hash anti-join (NOT EXISTS). Nao ordena Current ∪ delta.
-- Exige Current snake_case (migrado).
-- ------------------------------------------------------------
COPY (
    WITH checkpoint AS (
        SELECT MAX(_ingestion_timestamp) AS ultima_ingestao
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos.parquet',
            union_by_name = true
        )
    ),
    novos_raw AS (
        SELECT
            nome_alias,
            codigo AS id,
            empresaCodigo AS empresa_codigo,
            vendaCodigo AS venda_codigo,
            notaCodigo AS nota_codigo,
            funcionarioCodigo AS funcionario_codigo,
            clienteCodigo AS cliente_codigo,
            destacaAcrescimoDesconto AS destaca_acrescimo_desconto,
            clienteCpfCnpj AS cliente_cpf_cnpj,
            dataHora AS data_hora,
            notaNumero AS nota_numero,
            notaSerie AS nota_serie,
            totalVenda AS total_venda,
            caixaCodigo AS caixa_codigo,
            notaChave AS nota_chave,
            modeloDocumento AS modelo_documento,
            cancelada,
            placaVeiculo AS placa_veiculo,
            clienteCodigoExterno AS cliente_codigo_externo,
            centroCustoCodigo AS centro_custo_codigo,
            centroCustoVeiculo AS centro_custo_veiculo,
            identificacaoFidelidade AS identificacao_fidelidade,
            vendaUuid AS venda_uuid,
            motoristaCodigo AS motorista_codigo,
            codigo,
            _source_id,
            _source_name,
            _entity_id,
            _entity,
            _event_table,
            _operation,
            _event_id,
            _batch_id,
            _ingestion_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY nome_alias, empresaCodigo, vendaCodigo
                ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/vendas_postos/**/*.parquet',
            union_by_name = true
        ) b
        CROSS JOIN checkpoint c
        WHERE b._ingestion_timestamp > c.ultima_ingestao
    ),
    novos AS (
        SELECT
            *,
            CAST(empresa_codigo AS VARCHAR) || '-' || CAST(venda_codigo AS VARCHAR) AS venda_key
        FROM (
            SELECT * EXCLUDE (_rn)
            FROM novos_raw
            WHERE _rn = 1
        )
    ),
    atual_sem_afetados AS (
        SELECT a.*
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos.parquet',
            union_by_name = true
        ) a
        WHERE NOT EXISTS (
            SELECT 1
            FROM novos n
            WHERE n.nome_alias IS NOT DISTINCT FROM a.nome_alias
              AND n.empresa_codigo IS NOT DISTINCT FROM a.empresa_codigo
              AND n.venda_codigo IS NOT DISTINCT FROM a.venda_codigo
        )
    )
    SELECT * FROM atual_sem_afetados
    UNION ALL BY NAME
    SELECT * FROM novos
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/tmp_vendas_postos_incremental.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

UPDATE central.public.configuracao_entidades
SET tabela_hub = 'vendas_postos'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('vendas_postos', '_', ''));


-- ------------------------------------------------------------
-- vendas_postos_forma_pagamento
-- ------------------------------------------------------------
-- INCREMENTAL POSTOS (anti-join)
-- Mesmo padrao de vendas_postos: rank so no delta, anti-join por
-- hash no Current. Nao ordena Current ∪ delta inteiro.
-- Exige Current snake_case.
-- ------------------------------------------------------------
COPY (
    WITH checkpoint AS (
        SELECT MAX(_ingestion_timestamp) AS ultima_ingestao
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos_forma_pagamento.parquet',
            union_by_name = true
        )
    ),
    novos_raw AS (
        SELECT
            nome_alias,
            empresaCodigo AS empresa_codigo,
            vendaCodigo AS venda_codigo,
            NULL::BIGINT AS venda_prazo_codigo,
            financeiroCodigo AS financeiro_codigo,
            dataMovimento AS data_movimento,
            vencimento,
            valorPagamento AS valor_pagamento,
            taxaPercentual AS taxa_percentual,
            taxaValor AS taxa_valor,
            formaPagamentoCodigo AS forma_pagamento_codigo,
            administradoraCodigo AS administradora_codigo,
            turnoCodigo AS turno_codigo,
            tipoFormaPagamento AS tipo_forma_pagamento,
            nomeFormaPagamento AS nome_forma_pagamento,
            codigo,
            _source_id,
            _source_name,
            _entity_id,
            _entity,
            _event_table,
            _operation,
            _event_id,
            _batch_id,
            _ingestion_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY nome_alias, empresaCodigo, codigo
                ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/vendas_postos_forma_pagamento/**/*.parquet',
            union_by_name = true
        ) b
        CROSS JOIN checkpoint c
        WHERE b._ingestion_timestamp > c.ultima_ingestao
    ),
    novos AS (
        SELECT * EXCLUDE (_rn)
        FROM novos_raw
        WHERE _rn = 1
    ),
    atual_sem_afetados AS (
        SELECT a.*
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos_forma_pagamento.parquet',
            union_by_name = true
        ) a
        WHERE NOT EXISTS (
            SELECT 1
            FROM novos n
            WHERE n.nome_alias IS NOT DISTINCT FROM a.nome_alias
              AND n.empresa_codigo IS NOT DISTINCT FROM a.empresa_codigo
              AND n.codigo IS NOT DISTINCT FROM a.codigo
        )
    )
    SELECT * FROM atual_sem_afetados
    UNION ALL BY NAME
    SELECT * FROM novos
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/tmp_vendas_postos_forma_pagamento_incremental.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

UPDATE central.public.configuracao_entidades
SET tabela_hub = 'vendas_postos_forma_pagamento'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('vendas_postos_forma_pagamento', '_', ''));

-- ------------------------------------------------------------
-- vendas_postos_itens
-- ------------------------------------------------------------
-- INCREMENTAL POSTOS (anti-join)
-- Depois do primeiro Current (feito pelos buckets acima), o
-- refinador usa este bloco: rank so no delta, anti-join no Current.
-- ------------------------------------------------------------
COPY (
    WITH checkpoint AS (
        SELECT MAX(_ingestion_timestamp) AS ultima_ingestao
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos_itens.parquet',
            union_by_name = true
        )
    ),
    novos_raw AS (
        SELECT
            nome_alias,
            empresaCodigo AS empresa_codigo,
            vendaCodigo AS venda_codigo,
            vendaItemCodigo AS venda_item_codigo,
            dataMovimento AS data_movimento,
            produtoCodigo AS produto_codigo,
            quantidade,
            precoCusto AS preco_custo,
            totalCusto AS total_custo,
            precoVenda AS preco_venda,
            totalVenda AS total_venda,
            totalDesconto AS total_desconto,
            totalAcrescimo AS total_acrescimo,
            bicoCodigo AS bico_codigo,
            tanqueCodigo AS tanque_codigo,
            produtoLmcCodigo AS produto_lmc_codigo,
            funcionarioCodigo AS funcionario_codigo,
            produtoKitCodigo AS produto_kit_codigo,
            controleItem AS controle_item,
            icmsValor AS icms_valor,
            icmsBase AS icms_base,
            icmsAliquota AS icms_aliquota,
            cfop,
            cst,
            produtoCodigoExterno AS produto_codigo_externo,
            cstPis AS cst_pis,
            aliquotaPis AS aliquota_pis,
            basePis AS base_pis,
            valorPis AS valor_pis,
            cstCofins AS cst_cofins,
            aliquotaCofins AS aliquota_cofins,
            baseCofins AS base_cofins,
            valorCofins AS valor_cofins,
            tributacaoAdRem AS tributacao_ad_rem,
            valorTributacaoAdRem AS valor_tributacao_ad_rem,
            totalVendaBruto AS total_venda_bruto,
            totalDescontoBruto AS total_desconto_bruto,
            totalAcrescimoBruto AS total_acrescimo_bruto,
            precoVendaBruto AS preco_venda_bruto,
            cstCbs AS cst_cbs,
            classTributariaCbs AS class_tributaria_cbs,
            bcCbs AS bc_cbs,
            aliquotaEfetivaCbs AS aliquota_efetiva_cbs,
            valorCbs AS valor_cbs,
            cstIbs AS cst_ibs,
            classTributariaIbs AS class_tributaria_ibs,
            bcIbs AS bc_ibs,
            aliquotaEfetivaIbs AS aliquota_efetiva_ibs,
            valorIbs AS valor_ibs,
            aliquotaAdRemCbs AS aliquota_ad_rem_cbs,
            valorAdRemCbs AS valor_ad_rem_cbs,
            aliquotaAdRemIbs AS aliquota_ad_rem_ibs,
            valorAdRemIbs AS valor_ad_rem_ibs,
            aliquotaCbs AS aliquota_cbs,
            aliquotaIbs AS aliquota_ibs,
            codigo,
            _source_id,
            _source_name,
            _entity_id,
            _entity,
            _event_table,
            _operation,
            _event_id,
            _batch_id,
            _ingestion_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY nome_alias, empresaCodigo, vendaItemCodigo
                ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/vendas_postos_itens/**/*.parquet',
            union_by_name = true
        ) b
        CROSS JOIN checkpoint c
        WHERE b._ingestion_timestamp > c.ultima_ingestao
    ),
    novos AS (
        SELECT
            *,
            CAST(empresa_codigo AS VARCHAR) || '-' || CAST(venda_codigo AS VARCHAR) AS venda_key,
            CAST(empresa_codigo AS VARCHAR) || '-' || CAST(venda_item_codigo AS VARCHAR) AS venda_item_key
        FROM (
            SELECT * EXCLUDE (_rn)
            FROM novos_raw
            WHERE _rn = 1
        )
    ),
    atual_sem_afetados AS (
        SELECT a.*
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/vendas_postos_itens.parquet',
            union_by_name = true
        ) a
        WHERE NOT EXISTS (
            SELECT 1
            FROM novos n
            WHERE n.nome_alias IS NOT DISTINCT FROM a.nome_alias
              AND n.empresa_codigo IS NOT DISTINCT FROM a.empresa_codigo
              AND n.venda_item_codigo IS NOT DISTINCT FROM a.venda_item_codigo
        )
    )
    SELECT * FROM atual_sem_afetados
    UNION ALL BY NAME
    SELECT * FROM novos
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/tmp_vendas_postos_itens_incremental.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

UPDATE central.public.configuracao_entidades
SET tabela_hub = 'vendas_postos_itens'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('vendas_postos_itens', '_', ''));


-- ============================================================
-- POSTOS_ABASTECIMENTOS (API Quality, id 10009)
-- Bloco incremental anti-join - padrao vendas_postos
-- Bronze: payload cru (harmoniza = FALSE), pasta Bronze/*/Abastecimentos/**
-- Chave de negocio: nome_alias + empresaCodigo + abastecimentoCodigo
-- ============================================================

-- ------------------------------------------------------------
-- postos_abastecimentos
-- ------------------------------------------------------------
-- INCREMENTAL POSTOS (anti-join)
-- Rank apenas sobre o delta Bronze novo; o Current e varrido uma
-- vez em hash anti-join (NOT EXISTS). Nao ordena Current U+ delta.
-- Exige Current snake_case existente (bootstrap abaixo).
-- ------------------------------------------------------------
COPY (
    WITH checkpoint AS (
        SELECT MAX(_ingestion_timestamp) AS ultima_ingestao
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_abastecimentos.parquet',
            union_by_name = true
        )
    ),
    novos_raw AS (
        SELECT
            nome_alias,
            empresaCodigo AS empresa_codigo,
            abastecimentoCodigo AS abastecimento_codigo,
            dataFiscal AS data_fiscal,
            horaFiscal AS hora_fiscal,
            dataHoraAbastecimento AS data_hora_abastecimento,
            codigoBico AS codigo_bico,
            codigoProduto AS codigo_produto,
            quantidade,
            valorUnitario AS valor_unitario,
            valorTotal AS valor_total,
            precoCadastro AS preco_cadastro,
            tabelaPrecoA AS tabela_preco_a,
            tabelaPrecoB AS tabela_preco_b,
            tabelaPrecoC AS tabela_preco_c,
            encerrante,
            codigoFrentista AS codigo_frentista,
            afericao,
            vendaItemCodigo AS venda_item_codigo,
            placa,
            stringFull AS string_full,
            codigo,
            _source_id,
            _source_name,
            _entity_id,
            _entity,
            _event_table,
            _operation,
            _event_id,
            _batch_id,
            _ingestion_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY nome_alias, empresaCodigo, abastecimentoCodigo
                ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Abastecimentos/**/*.parquet',
            union_by_name = true
        ) b
        CROSS JOIN checkpoint c
        WHERE c.ultima_ingestao IS NULL
           OR b._ingestion_timestamp > c.ultima_ingestao
    ),
    novos AS (
        SELECT
            *,
            CAST(empresa_codigo AS VARCHAR) || '-' || CAST(abastecimento_codigo AS VARCHAR) AS abastecimento_key
        FROM (
            SELECT * EXCLUDE (_rn)
            FROM novos_raw
            WHERE _rn = 1
        )
    ),
    atual_sem_afetados AS (
        SELECT a.*
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_abastecimentos.parquet',
            union_by_name = true
        ) a
        WHERE NOT EXISTS (
            SELECT 1
            FROM novos n
            WHERE n.nome_alias IS NOT DISTINCT FROM a.nome_alias
              AND n.empresa_codigo IS NOT DISTINCT FROM a.empresa_codigo
              AND n.abastecimento_codigo IS NOT DISTINCT FROM a.abastecimento_codigo
        )
    )
    SELECT * FROM atual_sem_afetados
    UNION ALL BY NAME
    SELECT * FROM novos
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/tmp_postos_abastecimentos_incremental.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

UPDATE central.public.configuracao_entidades
SET tabela_hub = 'postos_abastecimentos'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Abastecimentos', '_', ''));

-- ============================================================
-- POSTOS_DUPLICATAS (API Quality, id 10011)
-- Bloco incremental anti-join - padrao POSTOS
-- Bronze: payload cru (harmoniza = FALSE), pasta Bronze/*/Duplicatas/**
-- Chave de negocio: nome_alias + empresaCodigo + duplicataCodigo
-- ============================================================

-- ------------------------------------------------------------
-- postos_duplicatas
-- ------------------------------------------------------------
-- INCREMENTAL POSTOS (anti-join)
-- Rank apenas sobre o delta Bronze novo; o Current e varrido uma
-- vez em hash anti-join (NOT EXISTS). Nao ordena Current U+ delta.
-- Suporta checkpoint NULL: 'c.ultima_ingestao IS NULL OR ...'
-- (permite o bootstrap automatico do Refinador v3).
-- ------------------------------------------------------------
COPY (
    WITH checkpoint AS (
        SELECT MAX(_ingestion_timestamp) AS ultima_ingestao
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_duplicatas.parquet',
            union_by_name = true
        )
    ),
    novos_raw AS (
        SELECT
            nome_alias,
            empresaCodigo AS empresa_codigo,
            duplicataCodigo AS duplicata_codigo,
            dataMovimento AS data_movimento,
            vencimento,
            dataPagamento AS data_pagamento,
            clienteCodigo AS cliente_codigo,
            nomeCliente AS nome_cliente,
            cpfCnpjCliente AS cpf_cnpj_cliente,
            numeroDocumento AS numero_documento,
            nossoNumero AS nosso_numero,
            situacao,
            pendente,
            valorDuplicata AS valor_duplicata,
            valorPago AS valor_pago,
            valorAcrescimo AS valor_acrescimo,
            valorDesconto AS valor_desconto,
            valorLiquido AS valor_liquido,
            remessaBoleto AS remessa_boleto,
            hierarquiaPlanoConta AS hierarquia_plano_conta,
            planoContaGerencialCodigo AS plano_conta_gerencial_codigo,
            codigo,
            _source_id,
            _source_name,
            _entity_id,
            _entity,
            _event_table,
            _operation,
            _event_id,
            _batch_id,
            _ingestion_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY nome_alias, empresaCodigo, duplicataCodigo
                ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/Duplicatas/**/*.parquet',
            union_by_name = true
        ) b
        CROSS JOIN checkpoint c
        WHERE c.ultima_ingestao IS NULL
           OR b._ingestion_timestamp > c.ultima_ingestao
    ),
    novos AS (
        SELECT
            *,
            CAST(empresa_codigo AS VARCHAR) || '-' || CAST(duplicata_codigo AS VARCHAR) AS duplicata_key
        FROM (
            SELECT * EXCLUDE (_rn)
            FROM novos_raw
            WHERE _rn = 1
        )
    ),
    atual_sem_afetados AS (
        SELECT a.*
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_duplicatas.parquet',
            union_by_name = true
        ) a
        WHERE NOT EXISTS (
            SELECT 1
            FROM novos n
            WHERE n.nome_alias IS NOT DISTINCT FROM a.nome_alias
              AND n.empresa_codigo IS NOT DISTINCT FROM a.empresa_codigo
              AND n.duplicata_codigo IS NOT DISTINCT FROM a.duplicata_codigo
        )
    )
    SELECT * FROM atual_sem_afetados
    UNION ALL BY NAME
    SELECT * FROM novos
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/tmp_postos_duplicatas_incremental.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

UPDATE central.public.configuracao_entidades
SET tabela_hub = 'postos_duplicatas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Duplicatas', '_', ''));

-- ============================================================
-- POSTOS_MOVIMENTOS_CONTA (API Quality, id 10012)
-- Bloco incremental anti-join - padrao POSTOS
-- Bronze: payload cru (harmoniza = FALSE), pasta Bronze/*/MovimentosConta/**
-- Chave de negocio: nome_alias + empresaCodigo + movimentoContaCodigo
-- ============================================================

-- ------------------------------------------------------------
-- postos_movimentos_conta
-- ------------------------------------------------------------
-- INCREMENTAL POSTOS (anti-join)
-- Rank apenas sobre o delta Bronze novo; o Current e varrido uma
-- vez em hash anti-join (NOT EXISTS).
-- Suporta checkpoint NULL: 'c.ultima_ingestao IS NULL OR ...'
-- (bootstrap automatico do Refinador v3).
-- ------------------------------------------------------------
COPY (
    WITH checkpoint AS (
        SELECT MAX(_ingestion_timestamp) AS ultima_ingestao
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_movimentos_conta.parquet',
            union_by_name = true
        )
    ),
    novos_raw AS (
        SELECT
            nome_alias,
            empresaCodigo AS empresa_codigo,
            movimentoContaCodigo AS movimento_conta_codigo,
            dataMovimento AS data_movimento,
            evento,
            tipo,
            tipoDocumentoOrigem AS tipo_documento_origem,
            codigoTipoDocumentoOrigem AS codigo_tipo_documento_origem,
            documentoOrigemCodigo AS documento_origem_codigo,
            documento,
            lote,
            descricao,
            valor,
            saldo,
            conciliado,
            daraHoraConciliacao AS data_hora_conciliacao,
            usuarioConciliacao AS usuario_conciliacao,
            contaCodigo AS conta_codigo,
            planoContaGerencialCodigo AS plano_conta_gerencial_codigo,
            centroCustoCodigo AS centro_custo_codigo,
            codigoPessoa AS codigo_pessoa,
            tipoPessoa AS tipo_pessoa,
            codigo,
            _source_id,
            _source_name,
            _entity_id,
            _entity,
            _event_table,
            _operation,
            _event_id,
            _batch_id,
            _ingestion_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY nome_alias, empresaCodigo, movimentoContaCodigo
                ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/MovimentosConta/**/*.parquet',
            union_by_name = true
        ) b
        CROSS JOIN checkpoint c
        WHERE c.ultima_ingestao IS NULL
           OR b._ingestion_timestamp > c.ultima_ingestao
    ),
    novos AS (
        SELECT * EXCLUDE (_rn)
        FROM novos_raw
        WHERE _rn = 1
    ),
    atual_sem_afetados AS (
        SELECT a.*
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/postos_movimentos_conta.parquet',
            union_by_name = true
        ) a
        WHERE NOT EXISTS (
            SELECT 1
            FROM novos n
            WHERE n.nome_alias IS NOT DISTINCT FROM a.nome_alias
              AND n.empresa_codigo IS NOT DISTINCT FROM a.empresa_codigo
              AND n.movimento_conta_codigo IS NOT DISTINCT FROM a.movimento_conta_codigo
        )
    )
    SELECT * FROM atual_sem_afetados
    UNION ALL BY NAME
    SELECT * FROM novos
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/tmp_postos_movimentos_conta_incremental.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

UPDATE central.public.configuracao_entidades
SET tabela_hub = 'postos_movimentos_conta'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('MovimentosConta', '_', ''));


-- ------------------------------------------------------------
-- postos_tanques
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        empresaCodigo,
        tanqueCodigo,
        tanqueCodigoExterno,
        nome,
        produtoCodigo,
        capacidade,
        ultimoUsuarioAlteracao,
        lastro,
        estoqueEscritural,
        produtoLmcCodigo,
        dataHoraMedidor,
        codigo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, empresaCodigo, tanqueCodigo
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/tanques/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        empresaCodigo AS empresa_codigo,
        tanqueCodigo AS tanque_codigo,
        tanqueCodigoExterno AS tanque_codigo_externo,
        nome,
        produtoCodigo AS produto_codigo,
        capacidade,
        ultimoUsuarioAlteracao AS ultimo_usuario_alteracao,
        lastro,
        estoqueEscritural AS estoque_escritural,
        produtoLmcCodigo AS produto_lmc_codigo,
        dataHoraMedidor AS data_hora_medidor,
        codigo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/postos_tanques.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'postos_tanques'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Tanques', '_', ''));

-- ------------------------------------------------------------
-- postos_tanques_entradas
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        tanqueEntradaCodigo,
        tanqueCodigo,
        dataHoraInicio,
        dataHoraFim,
        volumeInicio,
        volumeFim,
        volumeEntrada,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, tanqueCodigo, tanqueEntradaCodigo
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/tanqueentrada/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        tanqueEntradaCodigo AS tanque_entrada_codigo,
        tanqueCodigo AS tanque_codigo,
        dataHoraInicio AS data_hora_inicio,
        dataHoraFim AS data_hora_fim,
        volumeInicio AS volume_inicio,
        volumeFim AS volume_fim,
        volumeEntrada AS volume_entrada,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/postos_tanques_entradas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'postos_tanques_entradas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('TanqueEntrada', '_', ''));

-- ------------------------------------------------------------
-- postos_tanques_medidas
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        tanqueMedidaCodigo,
        tanqueCodigo,
        dataMedida,
        horaMedida,
        volume,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, tanqueCodigo, tanqueMedidaCodigo
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/tanquemedida/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        tanqueMedidaCodigo AS tanque_medida_codigo,
        tanqueCodigo AS tanque_codigo,
        dataMedida AS data_medida,
        horaMedida AS hora_medida,
        volume,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/postos_tanques_medidas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'postos_tanques_medidas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('TanqueMedida', '_', ''));

-- ------------------------------------------------------------
-- postos_caixas
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        empresaCodigo,
        caixaCodigo,
        dataMovimento,
        turnoCodigo,
        turno,
        pdvCodigo,
        funcionarioCodigo,
        centroCusto,
        abertura,
        fechamento,
        fechado,
        consolidado,
        tipoInclusao,
        bloqueado,
        tipoBloqueio,
        apurado,
        diferenca,
        dataHoraAtualizacao,
        codigo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, empresaCodigo, caixaCodigo
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/caixas/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        empresaCodigo AS empresa_codigo,
        caixaCodigo AS caixa_codigo,
        dataMovimento AS data_movimento,
        turnoCodigo AS turno_codigo,
        turno,
        pdvCodigo AS pdv_codigo,
        funcionarioCodigo AS funcionario_codigo,
        centroCusto AS centro_custo,
        abertura,
        fechamento,
        fechado,
        consolidado,
        tipoInclusao AS tipo_inclusao,
        bloqueado,
        tipoBloqueio AS tipo_bloqueio,
        apurado,
        diferenca,
        dataHoraAtualizacao AS data_hora_atualizacao,
        codigo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/postos_caixas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'postos_caixas'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('Caixas', '_', ''));


-- ------------------------------------------------------------
-- postos_sangrias_caixa
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        sangriaCodigo,
        empresaCodigo,
        caixaCodigo,
        contaCodigo,
        usuarioCodigo,
        funcionarioCodigo,
        dinheiro,
        cheque,
        cartao,
        nota,
        cartaFrete,
        emprestimo,
        despesa,
        chequePre,
        vale,
        transferencia,
        dataSangria,
        horaSangria,
        dataHoraColeta,
        dataHoraAtualizacao,
        alterada,
        coo,
        numeroDocumento,
        observacao,
        codigo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, empresaCodigo, sangriaCodigo
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/sangriascaixa/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        sangriaCodigo AS sangria_codigo,
        empresaCodigo AS empresa_codigo,
        caixaCodigo AS caixa_codigo,
        contaCodigo AS conta_codigo,
        usuarioCodigo AS usuario_codigo,
        funcionarioCodigo AS funcionario_codigo,
        dinheiro,
        cheque,
        cartao,
        nota,
        cartaFrete AS carta_frete,
        emprestimo,
        despesa,
        chequePre AS cheque_pre,
        vale,
        transferencia,
        dataSangria AS data_sangria,
        horaSangria AS hora_sangria,
        dataHoraColeta AS data_hora_coleta,
        dataHoraAtualizacao AS data_hora_atualizacao,
        alterada,
        coo,
        numeroDocumento AS numero_documento,
        observacao,
        codigo,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/postos_sangrias_caixa.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

-- Registra automaticamente o nome padronizado da entidade no Hub.
UPDATE central.public.configuracao_entidades
SET tabela_hub = 'postos_sangrias_caixa'
WHERE tabela_hub IS NULL
  AND LOWER(REPLACE(nome_tabela, '_', '')) =
      LOWER(REPLACE('SangriasCaixa', '_', ''));

-- ------------------------------------------------------------
-- postos_cartoes
-- ------------------------------------------------------------
COPY (
    WITH origem_rankeada AS (
        SELECT
        nome_alias,
        empresaCodigo,
        cartaoCodigo,
        vendaCodigo,
        vencimento,
        valor,
        parcela,
        taxaPercentual,
        administradoraCodigo,
        adiministradoraDescricao,
        clienteReferencia,
        clienteRazao,
        clienteCpfCnpj,
        centroCustoCodigo,
        centroCustoDescricao,
        dataPagamento,
        tipoInclusao,
        dataMovimento,
        horaMovimento,
        dataFiscal,
        pendente,
        nsu,
        autorizacao,
        codigo,
        codigoBandeira,
        nsuTef,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp,
            ROW_NUMBER() OVER (
            PARTITION BY nome_alias, empresaCodigo, cartaoCodigo
            ORDER BY _ingestion_timestamp DESC, _event_id DESC
            ) AS _rn
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Bronze/*/cartoes/**/*.parquet',
            union_by_name = true
        )
    )
    SELECT
        nome_alias,
        empresaCodigo AS empresa_codigo,
        cartaoCodigo AS cartao_codigo,
        vendaCodigo AS venda_codigo,
        vencimento,
        valor,
        parcela,
        taxaPercentual AS taxa_percentual,
        administradoraCodigo AS administradora_codigo,
        adiministradoraDescricao AS administradora_descricao,
        clienteReferencia AS cliente_referencia,
        clienteRazao AS cliente_razao,
        clienteCpfCnpj AS cliente_cpf_cnpj,
        centroCustoCodigo AS centro_custo_codigo,
        centroCustoDescricao AS centro_custo_descricao,
        dataPagamento AS data_pagamento,
        tipoInclusao AS tipo_inclusao,
        dataMovimento AS data_movimento,
        horaMovimento AS hora_movimento,
        dataFiscal AS data_fiscal,
        pendente,
        nsu,
        autorizacao,
        codigo,
        codigoBandeira AS codigo_bandeira,
        nsuTef AS nsu_tef,
        _source_id,
        _source_name,
        _entity_id,
        _entity,
        _event_table,
        _operation,
        _event_id,
        _batch_id,
        _ingestion_timestamp
    FROM origem_rankeada
    WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/postos_cartoes.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);