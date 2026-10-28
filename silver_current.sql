-- ============================================================
-- POMBO DATA LAKE - SILVER CURRENT (COMPATÍVEL)
-- Deduplicação com ROW_NUMBER em CTE, sem usar QUALIFY.
-- Mantém o evento mais recente por chave de negócio.
-- ============================================================

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/contaspagarreceber.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (
SELECT
    nome_alias,
    CodEmpresa,
    NumLancamento,
    ValorBrutoMoeda,
    ValorLiquidoMoeda,
    SaldoMoeda,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/contaspagarreceberMoeda.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/notafiscal.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,
    CodEmpresa,
    Sequencial,
    CodProd,
    Descricao,
	Item, 
	CodProd,
	NumeroOC,
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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/itensnf.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,

    CodEmpresa,
    NumLote,
    CodConta,
    ValorDebito,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/MovimentoPlanoContas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,

    CodEmpresa,
    NumLote,
    SeqCC,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/CentroCustoRateio.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,

    CodEmpresa,
    NumLancamento,
    SeqCC,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/CentroCustoPagar.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,

    CodEmpresa,
    SequencialNF,
    ID_ClassificacaoDespesas,
    DetalhamentoDespesa,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Z_NotaFiscal_ClassificacaoDespesas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (


SELECT
    nome_alias,

    CodEmpresa,
    NomeUsual,
    CGC,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/empresas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (


SELECT
    nome_alias,
    CodEnt,
	RzSocial,
	Endereco,
	CEP,
	Bairro,
	Municipio,
	UF,
	CGC,
	Fone,
	Fax,
	IE,
	Vendedor,
	DataAlteracaoCad,
	ConsumidorFinal,
	Status,
	Tipo,
	Comprador,
	CNPJPagamento,
	CNAE,
	InscricaoMunicipal,
	Suframa,
	Numero,
	Complemento,
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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Entidade.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (


SELECT
    nome_alias,
    CodVend,
    Descricao,
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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Vendedor.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,

    SeqDocto,
    GNRE,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/TipoDocumento.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,

    CodNatOp,
    Descricao,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/NaturezaOperacao.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,

    ID,
    Descricao,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/z_classificacaodespesas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (

SELECT
    nome_alias,

    CodConta,
    Descricao,
    CodCompl,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/planocontas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (


SELECT
    nome_alias,

    CodPort,
    BloqueiaBaixa,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Portador.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (


SELECT
    nome_alias,

    SeqCC,
    Descricao,
    MascaraOrdenacao,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/CentroCusto.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Pedido.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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
    CUSTOFINANCSUGERIDO,
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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/ItensPedido.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Cidade.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
WITH origem_rankeada AS (


SELECT
    nome_alias,

    codBandeira,
    descricao,
    BloqueadoVendas,
    BandeiraANP,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Bandeira.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/BaixaAntecipada.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/ItensBaixa.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Baixa.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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

    IDICMS,

    DescricaoReduzida,

    ID,

    ANP_Descricao,

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Produto.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

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
SELECT * EXCLUDE (_rn)
FROM origem_rankeada
WHERE _rn = 1
)
TO '//10.68.152.218/d$/Pipeline/Silver/Current/Deposito.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);