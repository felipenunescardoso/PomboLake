--
-- PostgreSQL database dump
--

\restrict BdbfaCYPEu5SwvY7n6Siz0m1kqzrnPLQYPlbX2pcDWIwq6zDEsaFyswzTkoKlfO

-- Dumped from database version 18.4
-- Dumped by pg_dump version 18.4

-- Started on 2026-10-28 11:49:56

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 139 (class 2615 OID 16390)
-- Name: gold; Type: SCHEMA; Schema: -; Owner: hub_admin
--

CREATE SCHEMA gold;


ALTER SCHEMA gold OWNER TO hub_admin;

--
-- TOC entry 449 (class 1255 OID 21486)
-- Name: fn_ajustar_nomeusual_empresas(); Type: FUNCTION; Schema: gold; Owner: postgres
--

CREATE FUNCTION gold.fn_ajustar_nomeusual_empresas() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_cnpj_limpo text;
BEGIN
    v_cnpj_limpo := regexp_replace(
        COALESCE(NEW.cnpj::text, ''),
        '[^0-9]',
        '',
        'g'
    );

    NEW.nome_usual :=
        CASE v_cnpj_limpo
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
                    WHEN TRIM(COALESCE(NEW.nome_usual, '')) LIKE '76%'
                        THEN TRIM(COALESCE(NEW.nome_usual, ''))
                    ELSE TRIM(
                        regexp_replace(
                            TRIM(COALESCE(NEW.nome_usual, '')),
                            '^[0-9]+[[:space:]-]*',
                            ''
                        )
                    )
                END
        END;

    RETURN NEW;
END;
$$;


ALTER FUNCTION gold.fn_ajustar_nomeusual_empresas() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 432 (class 1259 OID 17593)
-- Name: baixa; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.baixa (
    nome_alias text,
    cod_empresa integer,
    num_baixa integer,
    cc_baixa text,
    num_docto text,
    observacao text,
    valor_movimentado numeric(18,4),
    valor_desconto numeric(18,4),
    valor_acrescimo numeric(18,4),
    valor_original numeric(18,4),
    cc_acrescimo text,
    cc_desconto text,
    num_cheque text,
    itens_baixa integer,
    num_credito integer,
    num_lote_bx integer,
    cancelado integer,
    em_cheque integer,
    tipo text,
    moeda text,
    usuario text,
    val_credito numeric(18,4),
    seq_caucao integer,
    bloqueado integer,
    usuario_desbloqueio text,
    val_credito_gerado numeric(18,4),
    seq_lote_real integer,
    seq_docto integer,
    info_adic text,
    usuario_cancto text,
    observacoes text,
    outros_descontos_valor numeric(18,4),
    seq_banco text,
    num_agencia text,
    id_pix_recebido integer,
    id integer,
    dt_baixa date,
    data_cheque date,
    data_fluxo date,
    data_prog_desbloqueio date,
    data_desbloqueio date,
    data_inclusao_baixa timestamp without time zone,
    data_cancelamento date,
    data_cancto_usu date,
    dt_baixa_key integer,
    data_cheque_key integer,
    data_fluxo_key integer,
    data_prog_desbloqueio_key integer,
    data_desbloqueio_key integer,
    data_inclusao_baixa_key integer,
    data_cancelamento_key integer,
    data_cancto_usu_key integer,
    baixa_key text CONSTRAINT "baixa_BaixaKey_not_null" NOT NULL
);


ALTER TABLE gold.baixa OWNER TO postgres;

--
-- TOC entry 430 (class 1259 OID 17581)
-- Name: baixa_antecipada; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.baixa_antecipada (
    nome_alias text,
    cod_empresa integer,
    seq_credito integer,
    num_docto text,
    valor_orig numeric(18,4),
    moeda text,
    historico text,
    saldo numeric(18,4),
    num_cheque text,
    em_cheque integer,
    num_lote integer,
    cod_ent integer,
    cc_banco text,
    cc_entidade text,
    tipo text,
    usuario text,
    status text,
    credito_destino integer,
    credito_origem integer,
    cod_ent_origem integer,
    comprador integer,
    nao_identificado integer,
    bloqueado integer,
    desbloqueado_por text,
    cod_empresa_origem_credito_ee integer,
    seq_credito_origem_credito_ee integer,
    cli_for text,
    info_adic text,
    num_ped_compra integer,
    pmm_pd_ajuste_cod_emp integer,
    pmm_pd_ajuste_num_ped integer,
    pmm_cod_dep text,
    permite_retira_entrega integer,
    usuario_cancelamento text,
    pmm_despreza_lib_qtde_pend integer,
    pagamento_eletronico integer,
    status_proc_pag_elet integer,
    bloqueado_baixa integer,
    id_deposito_identif_itens integer,
    id_pix_recebido integer,
    id integer,
    cod_tipo_antecipacao integer,
    id_classificacao_despesas text,
    detalhamento_despesa text,
    data date,
    data_cheque date,
    data_fluxo date,
    data_desbloqueio date,
    desbloqueado_em date,
    pmm_pd_ajuste_data date,
    data_cancelamento date,
    data_simples date,
    data_inclusao timestamp without time zone,
    data_key integer,
    data_cheque_key integer,
    data_fluxo_key integer,
    data_desbloqueio_key integer,
    desbloqueado_em_key integer,
    pmm_pd_ajuste_data_key integer,
    data_cancelamento_key integer,
    data_simples_key integer,
    data_inclusao_key integer,
    rz_social text,
    rz_social_comprador text,
    baixa_antecipada_key text CONSTRAINT "baixaantecipada_BaixaAntecipadaKey_not_null" NOT NULL
);


ALTER TABLE gold.baixa_antecipada OWNER TO postgres;

--
-- TOC entry 429 (class 1259 OID 17576)
-- Name: bandeira; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.bandeira (
    nome_alias text,
    cod_bandeira text,
    bloqueado_vendas integer,
    bandeira_anp text,
    descricao text
);


ALTER TABLE gold.bandeira OWNER TO postgres;

--
-- TOC entry 425 (class 1259 OID 16750)
-- Name: centro_custo; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.centro_custo (
    nome_alias text CONSTRAINT centrocusto_nome_alias_not_null NOT NULL,
    seq_cc bigint CONSTRAINT "centrocusto_SeqCC_not_null" NOT NULL,
    descricao text,
    mascara_ordenacao text,
    centro_custo_key text CONSTRAINT "centrocusto_CentroCustoKey_not_null" NOT NULL
);


ALTER TABLE gold.centro_custo OWNER TO postgres;

--
-- TOC entry 415 (class 1259 OID 16642)
-- Name: centro_custo_pagar; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.centro_custo_pagar (
    nome_alias text CONSTRAINT centrocustopagar_nome_alias_not_null NOT NULL,
    cod_empresa integer CONSTRAINT "centrocustopagar_CodEmpresa_not_null" NOT NULL,
    num_lancamento bigint CONSTRAINT "centrocustopagar_NumLancamento_not_null" NOT NULL,
    seq_cc bigint CONSTRAINT "centrocustopagar_SeqCC_not_null" NOT NULL,
    cpr_key text CONSTRAINT "centrocustopagar_CPRKey_not_null" NOT NULL,
    centro_custo_key text CONSTRAINT "centrocustopagar_CentroCustoKey_not_null" NOT NULL,
    empresa_key text CONSTRAINT "centrocustopagar_EmpresaKey_not_null" NOT NULL
);


ALTER TABLE gold.centro_custo_pagar OWNER TO postgres;

--
-- TOC entry 414 (class 1259 OID 16628)
-- Name: centro_custo_rateio; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.centro_custo_rateio (
    nome_alias text CONSTRAINT centrocustorateio_nome_alias_not_null NOT NULL,
    cod_empresa integer CONSTRAINT "centrocustorateio_CodEmpresa_not_null" NOT NULL,
    num_lote bigint CONSTRAINT "centrocustorateio_NumLote_not_null" NOT NULL,
    seq_cc bigint CONSTRAINT "centrocustorateio_SeqCC_not_null" NOT NULL,
    lote_key text CONSTRAINT "centrocustorateio_LoteKey_not_null" NOT NULL,
    centro_custo_key text CONSTRAINT "centrocustorateio_CentroCustoKey_not_null" NOT NULL,
    empresa_key text CONSTRAINT "centrocustorateio_EmpresaKey_not_null" NOT NULL
);


ALTER TABLE gold.centro_custo_rateio OWNER TO postgres;

--
-- TOC entry 428 (class 1259 OID 17571)
-- Name: cidade; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.cidade (
    nome_alias text,
    codigo_dnc text,
    uf text,
    preco_max_diesel numeric(18,4),
    regiao integer,
    codigo_municipio text,
    codigo_dnc_nfe text,
    urbano integer,
    descricao text
);


ALTER TABLE gold.cidade OWNER TO postgres;

--
-- TOC entry 409 (class 1259 OID 16564)
-- Name: contas_pagar_receber; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.contas_pagar_receber (
    nome_alias text CONSTRAINT contaspagarreceber_nome_alias_not_null NOT NULL,
    cod_empresa integer CONSTRAINT "contaspagarreceber_CodEmpresa_not_null" NOT NULL,
    tipo text,
    num_lancamento bigint CONSTRAINT "contaspagarreceber_NumLancamento_not_null" NOT NULL,
    cod_ent bigint,
    num_docto text,
    num_lote bigint,
    moeda text,
    valor_orig numeric(18,4),
    saldo numeric(18,4),
    historico text,
    seq_nf bigint,
    parcela integer,
    cod_tp_cobr text,
    cod_rec text,
    rz_social text,
    observ text,
    valor_pago numeric(18,4),
    numero_nf text,
    num_pedido text,
    cc_entidade text,
    cc_custo_venda text,
    cc_estoque text,
    custo numeric(18,4),
    cc_dsp_com text,
    cc_com_a_pg text,
    val_comissao numeric(18,4),
    usuario text,
    comprador text,
    rz_social_comprador text,
    sinistro text,
    tx_juros_dia numeric(18,6),
    nosso_numero text,
    baixa_eletronica boolean,
    baixa_confirmada boolean,
    postergacao_confirmada boolean,
    seq_cc bigint,
    processado_pela_tabela_preco boolean,
    competencia text,
    convenio text,
    cartorio text,
    previsao text,
    seq_contabilizacao bigint,
    pode_contabilizar boolean,
    seq_lote_real bigint,
    seq_padrao bigint,
    seq_docto bigint,
    banco text,
    agencia text,
    conta text,
    contador_seq_docto bigint,
    valor_bruto numeric(18,4),
    sequencial_reneg bigint,
    renegociado boolean,
    info_adic text,
    comando_auxiliar text,
    cheque_devolvido boolean,
    gera_adiantamento boolean,
    contabiliza_real boolean,
    fat_d_fin text,
    val_fat numeric(18,4),
    autorizado boolean,
    autorizado_por text,
    autorizado_em timestamp without time zone,
    dig_agencia text,
    dig_conta text,
    cr_nosso_numero text,
    seq_banco bigint,
    cod_empresa_adto integer,
    seq_credito_adto bigint,
    com_adto_quantificado boolean,
    usuario_cancelamento text,
    seq_contrato bigint,
    data_inclusao timestamp without time zone,
    carteira text,
    valor_acrescimo_tipo_cobr numeric(18,4),
    data_cancelamento_lote timestamp without time zone,
    seq_cpr_recorrente bigint,
    utiliza_conversao boolean,
    total_parcelas integer,
    sequencial_processo_pmm bigint,
    sequencial_etapa_pmm bigint,
    valor_contas numeric(18,4),
    cnpj text,
    data_emissao date,
    data_vencimento_original date,
    data_vencimento date,
    data_cancelamento date,
    data_emissao_key integer,
    data_vencimento_original_key integer,
    data_vencimento_key integer,
    data_cancelamento_key integer,
    cpr_key text CONSTRAINT "contaspagarreceber_CPRKey_not_null" NOT NULL,
    empresa_key text CONSTRAINT "contaspagarreceber_EmpresaKey_not_null" NOT NULL,
    entidade_key text,
    nota_fiscal_key text,
    tipo_documento_key text,
    centro_custo_key text,
    lote_key text,
    contabiliza_na_nf text,
    cod_tp_cobr_key character varying(150),
    venda_codigo bigint,
    duplicata_codigo bigint,
    tipo_original character varying(100),
    data_pagamento date,
    plano_conta_gerencial_codigo bigint,
    convertido boolean,
    titulo_numero bigint,
    data_hora_atualizacao timestamp with time zone,
    codigo bigint
);


ALTER TABLE gold.contas_pagar_receber OWNER TO postgres;

--
-- TOC entry 410 (class 1259 OID 16576)
-- Name: contas_pagar_receber_moeda; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.contas_pagar_receber_moeda (
    nome_alias text CONSTRAINT contaspagarrecebermoeda_nome_alias_not_null NOT NULL,
    cod_empresa integer CONSTRAINT "contaspagarrecebermoeda_CodEmpresa_not_null" NOT NULL,
    num_lancamento bigint CONSTRAINT "contaspagarrecebermoeda_NumLancamento_not_null" NOT NULL,
    valor_bruto_moeda numeric(18,4),
    valor_liquido_moeda numeric(18,4),
    saldo_moeda numeric(18,4),
    cpr_key text CONSTRAINT "contaspagarrecebermoeda_CPRKey_not_null" NOT NULL,
    empresa_key text CONSTRAINT "contaspagarrecebermoeda_EmpresaKey_not_null" NOT NULL
);


ALTER TABLE gold.contas_pagar_receber_moeda OWNER TO postgres;

--
-- TOC entry 434 (class 1259 OID 17604)
-- Name: deposito; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.deposito (
    nome_alias text,
    cod_dep text,
    tipo text,
    numero text,
    municipio text,
    uf text,
    cep text,
    cgc text,
    ie text,
    deposito_externo integer,
    base_propria integer,
    codigo_dnc text,
    codigo_dcp text,
    codigo_dcp_refinaria text,
    ativo integer,
    participa_dcp integer,
    permite_venda integer,
    cod_dep_virtual text,
    cod_ent integer,
    cod_pais integer,
    latitude numeric(18,6),
    longitude numeric(18,6),
    id_perfil integer,
    perfil_utilizacao integer,
    descricao text,
    rz_social text,
    endereco text,
    complemento text,
    bairro text,
    nome_deposito text,
    telefone character varying(30)
);


ALTER TABLE gold.deposito OWNER TO postgres;

--
-- TOC entry 417 (class 1259 OID 16670)
-- Name: empresas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.empresas (
    nome_alias text NOT NULL,
    cod_empresa integer NOT NULL,
    nome_usual text,
    cnpj text,
    empresa_key text CONSTRAINT "empresas_EmpresaKey_not_null" NOT NULL,
    endereco text,
    bairro text,
    numero text,
    cep text,
    cidade text,
    uf text,
    empresa_codigo_externo text,
    sigla text,
    tipo_imposto text,
    razao_social text,
    ie text,
    inscricao_municipal text,
    cnae text
);


ALTER TABLE gold.empresas OWNER TO postgres;

--
-- TOC entry 418 (class 1259 OID 16680)
-- Name: entidade; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.entidade (
    nome_alias text NOT NULL,
    cod_ent bigint CONSTRAINT "entidade_CodEnt_not_null" NOT NULL,
    rz_social text,
    endereco text,
    cep text,
    bairro text,
    municipio text,
    uf text,
    fone text,
    fax text,
    ie text,
    vendedor text,
    consumidor_final boolean,
    status text,
    tipo text,
    comprador text,
    cnpj_pagamento text,
    cnae text,
    inscricao_municipal text,
    suframa text,
    numero text,
    complemento text,
    cnpj text,
    data_alteracao_cadastro date,
    data_alteracao_cadastro_key integer,
    entidade_key text CONSTRAINT "entidade_EntidadeKey_not_null" NOT NULL,
    vendedor_key text,
    cod_tp_cobr_key character varying(150),
    cod_tp_cobr character varying(50),
    nome_usual text,
    data_cadastro date,
    cod_cidade bigint,
    usa_limite_litros boolean,
    limite_litros numeric(18,2),
    usa_limite_reais boolean,
    limite_reais numeric(18,2),
    bloqueado boolean,
    ultimo_usuario_alteracao text,
    data_hora_atualizacao timestamp with time zone,
    cliente_grupo_codigo bigint,
    cliente_codigo_externo text,
    celular text,
    outro_telefone text,
    observacoes text,
    website text,
    pais text,
    rg text,
    cliente_referencia text
);


ALTER TABLE gold.entidade OWNER TO postgres;

--
-- TOC entry 431 (class 1259 OID 17587)
-- Name: itens_baixa; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.itens_baixa (
    nome_alias text,
    cod_empresa integer,
    num_baixa integer,
    item integer,
    valor_original numeric(18,4),
    valor_desc numeric(18,4),
    valor_acrescimo numeric(18,4),
    valor_movimento numeric(18,4),
    num_lancamento integer,
    cancelado integer,
    cod_ent integer,
    seq_credito integer,
    val_credito numeric(18,4),
    val_credito_gerado numeric(18,4),
    seq_credito_gerado integer,
    bloqueado integer,
    nao_identificado integer,
    valor_devolvido numeric(18,4),
    historico text,
    info_adic text,
    outros_descontos_valor numeric(18,4),
    id integer,
    dt_baixa date,
    dt_baixa_key integer,
    baixa_key text CONSTRAINT "itensbaixa_BaixaKey_not_null" NOT NULL
);


ALTER TABLE gold.itens_baixa OWNER TO postgres;

--
-- TOC entry 412 (class 1259 OID 16600)
-- Name: itens_nf; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.itens_nf (
    nome_alias text CONSTRAINT itensnf_nome_alias_not_null NOT NULL,
    cod_empresa integer CONSTRAINT "itensnf_CodEmpresa_not_null" NOT NULL,
    sequencial bigint CONSTRAINT "itensnf_Sequencial_not_null" NOT NULL,
    cod_prod text,
    descricao text,
    item integer,
    numero_oc text,
    nota_fiscal_key text CONSTRAINT "itensnf_NotaFiscalKey_not_null" NOT NULL,
    item_nota_fiscal_key text CONSTRAINT "itensnf_ItemNotaFiscalKey_not_null" NOT NULL,
    empresa_key text CONSTRAINT "itensnf_EmpresaKey_not_null" NOT NULL,
    produto_key text
);


ALTER TABLE gold.itens_nf OWNER TO postgres;

--
-- TOC entry 427 (class 1259 OID 17565)
-- Name: itens_pedido; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.itens_pedido (
    nome_alias text,
    cod_empresa integer,
    num_ped integer,
    seq integer,
    cod_prod text,
    qtde numeric(18,4),
    qtde_pendente numeric(18,4),
    qtde_separada numeric(18,4),
    preco_fob numeric(18,4),
    preco_cif numeric(18,4),
    custo_cifsi numeric(18,4),
    tipo text,
    tabela text,
    qtde_faturada numeric(18,4),
    deposito_entrada text,
    deposito_saida text,
    qtde_nao_atendida numeric(18,4),
    cod_ent integer,
    perc_desconto numeric(18,6),
    tipo_prod text,
    cancelamento_parcial integer,
    valor_ipi numeric(18,4),
    cod_unid text,
    qtde_devolv numeric(18,4),
    valor_pendente numeric(18,4),
    docto_cliente_faturado text,
    ie_cliente text,
    consumidor_final integer,
    frete_unit_padrao numeric(18,4),
    frete_unit_real numeric(18,4),
    fator_conversao numeric(18,4),
    confirmado integer,
    numero_oc integer,
    percentual_desconto numeric(18,4),
    seq_carga integer,
    preco_tabela numeric(18,4),
    custo_medio numeric(18,4),
    pmm_qtde_paga numeric(18,4),
    preco_minimo_a_vista numeric(18,4),
    valor_desconto_condicional numeric(18,4),
    custo numeric(18,4),
    custo_alocado numeric(18,4),
    pmm_saldo_excedente numeric(18,4),
    pmm_saldo_excedente_nao_utilizado numeric(18,4),
    id integer,
    pedido_id integer,
    uf_fat text,
    item_ref_ped text,
    qtde_transferida_entrada numeric(18,4),
    qtde_transferida_saida numeric(18,4),
    item_oc integer,
    qtde_convertida numeric(18,4),
    dt_prev_entrega date,
    dt_prev_entrega_key integer,
    pedido_key text CONSTRAINT "itenspedido_PedidoKey_not_null" NOT NULL
);


ALTER TABLE gold.itens_pedido OWNER TO postgres;

--
-- TOC entry 413 (class 1259 OID 16614)
-- Name: movimento_plano_contas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.movimento_plano_contas (
    nome_alias text CONSTRAINT movimentoplanocontas_nome_alias_not_null NOT NULL,
    cod_empresa integer CONSTRAINT "movimentoplanocontas_CodEmpresa_not_null" NOT NULL,
    num_lote bigint CONSTRAINT "movimentoplanocontas_NumLote_not_null" NOT NULL,
    cod_conta text CONSTRAINT "movimentoplanocontas_CodConta_not_null" NOT NULL,
    valor_debito numeric(18,4),
    lote_key text CONSTRAINT "movimentoplanocontas_LoteKey_not_null" NOT NULL,
    plano_conta_key text CONSTRAINT "movimentoplanocontas_PlanoContaKey_not_null" NOT NULL,
    empresa_key text CONSTRAINT "movimentoplanocontas_EmpresaKey_not_null" NOT NULL
);


ALTER TABLE gold.movimento_plano_contas OWNER TO postgres;

--
-- TOC entry 422 (class 1259 OID 16720)
-- Name: natureza_operacao; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.natureza_operacao (
    nome_alias text CONSTRAINT naturezaoperacao_nome_alias_not_null NOT NULL,
    cod_nat_op text CONSTRAINT "naturezaoperacao_CodNatOp_not_null" NOT NULL,
    descricao text,
    natureza_operacao_key text CONSTRAINT "naturezaoperacao_NaturezaOperacaoKey_not_null" NOT NULL
);


ALTER TABLE gold.natureza_operacao OWNER TO postgres;

--
-- TOC entry 411 (class 1259 OID 16588)
-- Name: nota_fiscal; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.nota_fiscal (
    nome_alias text CONSTRAINT notafiscal_nome_alias_not_null NOT NULL,
    sequencial bigint CONSTRAINT "notafiscal_Sequencial_not_null" NOT NULL,
    cod_empresa integer CONSTRAINT "notafiscal_CodEmpresa_not_null" NOT NULL,
    numero_nf text,
    cod_nat_op text,
    val_fin numeric(18,4),
    num_lote bigint,
    cod_ent bigint,
    chave_acesso text,
    cod_transp bigint,
    seq_frota bigint,
    documento text,
    data_nota_fiscal date,
    data_nota_fiscal_key integer,
    nota_fiscal_key text CONSTRAINT "notafiscal_NotaFiscalKey_not_null" NOT NULL,
    empresa_key text CONSTRAINT "notafiscal_EmpresaKey_not_null" NOT NULL,
    entidade_key text,
    natureza_operacao_key text,
    lote_key text,
    num_conhecimento character varying(50),
    placa_vei_transp character varying(20),
    carreta character varying(20),
    val_prod numeric(18,2),
    base_icms numeric(18,2),
    val_icms numeric(18,2),
    base_icmssubst numeric(18,2),
    icmssubst numeric(18,2),
    val_ipi numeric(18,2),
    val_frete numeric(18,2),
    val_desconto numeric(18,2),
    val_seguro numeric(18,2),
    val_desp_acess numeric(18,2),
    data_movto date,
    es character varying(1),
    tipo character varying(10)
);


ALTER TABLE gold.nota_fiscal OWNER TO postgres;

--
-- TOC entry 426 (class 1259 OID 17559)
-- Name: pedido; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.pedido (
    nome_alias text,
    cod_empresa integer,
    num_ped integer,
    tipo text,
    cod_ent integer,
    ref_ped text,
    usuario text,
    desconto numeric(18,4),
    observ text,
    status_pedido text,
    valor_merc numeric(18,4),
    cod_nat_op text,
    frete_inc integer,
    val_frete numeric(18,4),
    cod_tp_cobr text,
    tab_preco text,
    val_pedido numeric(18,4),
    cod_transp text,
    cod_entrega integer,
    vendedor text,
    cta_contab text,
    lib_financ integer,
    lib_venda integer,
    cod_cobranca integer,
    status_fat text,
    lib_financ_por text,
    lib_vendas_por text,
    gera_numero_serie integer,
    qtde_pedido numeric(18,4),
    qtde_pendente numeric(18,4),
    qtde_nao_atendida numeric(18,4),
    qtde_separada numeric(18,4),
    qtde_faturada numeric(18,4),
    cep_cobranca text,
    municipio_cobranca text,
    uf_cobranca text,
    cep_entrega text,
    municipio_entrega text,
    uf_entrega text,
    entrega_parcial integer,
    serie_nf text,
    cep_fat text,
    municipio_fat text,
    uf_fat text,
    pronto_faturar integer,
    cod_pg text,
    prazo_entrega text,
    perc_icms integer,
    ref_ped_venda text,
    obs_longo text,
    retira_entrega text,
    aprovado integer,
    deposito_padrao text,
    tem_pendencia integer,
    prazo_pagto integer,
    fora_estado integer,
    segurado integer,
    usuario_cancelamento text,
    valor_pendente numeric(18,4),
    valor_credito numeric(18,4),
    valor_bloqueado numeric(18,4),
    valor_liberado numeric(18,4),
    documento text,
    placa_vei text,
    carreta text,
    capacidade integer,
    documento_transp text,
    treminhao text,
    tipo_transp integer,
    aprov_transp integer,
    aprov_transp_por text,
    cod_cod_transp text,
    seq_frota integer,
    seq_frota_carreta integer,
    seq_frota_trem integer,
    valor_credito_bloqueado numeric(18,4),
    cod_vend_assist text,
    seq_motivo_cancto integer,
    cod_transp_anterior text,
    seq_frota_anterior integer,
    seq_carreta_anterior integer,
    seq_treminhao_anterior integer,
    carga_fechada integer,
    cr_usuario_pre_credito text,
    pmm_cod_empresa_origem_oc integer,
    valor_tabela numeric(18,4),
    pmm_controla_qtde_adto integer,
    cod_setor integer,
    numero_entrega text,
    numero_cobranca text,
    numero_fat text,
    codigo_dnc_entrega text,
    codigo_dnc_fat text,
    obs_aprovacao text,
    seq_carga integer,
    dpmp integer,
    forca_dep_destino integer,
    deposito_destino text,
    pmm_tipo integer,
    contrato integer,
    suspenso integer,
    peso_total integer,
    cod_ent_corretora integer,
    cod_ent_forne_carregto integer,
    id_consulta_on_line integer,
    id integer,
    web_origem_pedido integer,
    tipo_operacao integer,
    tipo_operacao_calculado integer,
    aprovacao_automatica_vendas integer,
    es text,
    ajuste_estoque integer,
    inclui_pre_adiantamento integer,
    finalizado integer,
    qtde_transferida_entrada numeric(18,4),
    qtde_transferida_saida numeric(18,4),
    aprovado_cliente_gac_nao_ok integer,
    servico_analise_aprovacao_vd integer,
    id_tabela_preco integer,
    id_tabela_preco_aux_usu integer,
    valor_ipi numeric(18,4),
    id_pix integer,
    deposito_alterado integer,
    id_tabela_preco_novo_dep integer,
    cnpj_ent text,
    desconto_condicional_tipo integer,
    cmv_custo_medio_saida integer,
    cod_tipo_antecipacao integer,
    id_classificacao_despesas text,
    detalhamento_despesa text,
    data_ped date,
    dt_prev_fat date,
    lib_financ_em date,
    lib_vendas_em date,
    data_cancelamento date,
    aprov_transp_em date,
    cr_data_pre_credito date,
    data_movto date,
    servico_analise_aprovacao_vd_data date,
    data_inicio_execucao date,
    data_fim_execucao date,
    data_ped_key integer,
    dt_prev_fat_key integer,
    lib_financ_em_key integer,
    lib_vendas_em_key integer,
    data_cancelamento_key integer,
    aprov_transp_em_key integer,
    cr_data_pre_credito_key integer,
    data_movto_key integer,
    servico_analise_aprovacao_vd_data_key integer,
    data_inicio_execucao_key integer,
    data_fim_execucao_key integer,
    razao_social text,
    descr_transp text,
    rz_social_ent text,
    endereco_cobranca text,
    endereco_entrega text,
    endereco_fat text,
    complemento_entrega text,
    complemento_cobranca text,
    complemento_fat text,
    bairro_cobranca text,
    bairro_entrega text,
    bairro_fat text,
    pedido_key text CONSTRAINT "pedido_PedidoKey_not_null" NOT NULL
);


ALTER TABLE gold.pedido OWNER TO postgres;

--
-- TOC entry 424 (class 1259 OID 16740)
-- Name: plano_contas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.plano_contas (
    nome_alias text CONSTRAINT planocontas_nome_alias_not_null NOT NULL,
    cod_conta text CONSTRAINT "planocontas_CodConta_not_null" NOT NULL,
    descricao text,
    cod_compl integer,
    plano_conta_key text CONSTRAINT "planocontas_PlanoContaKey_not_null" NOT NULL
);


ALTER TABLE gold.plano_contas OWNER TO postgres;

--
-- TOC entry 420 (class 1259 OID 16700)
-- Name: portador; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.portador (
    nome_alias text NOT NULL,
    cod_port text CONSTRAINT "portador_CodPort_not_null" NOT NULL,
    bloqueia_baixa boolean,
    portador_key text CONSTRAINT "portador_PortadorKey_not_null" NOT NULL
);


ALTER TABLE gold.portador OWNER TO postgres;

--
-- TOC entry 440 (class 1259 OID 94320)
-- Name: postos_abastecimentos; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_abastecimentos (
    nome_alias text,
    empresa_codigo bigint NOT NULL,
    abastecimento_codigo bigint NOT NULL,
    data_fiscal date,
    hora_fiscal character varying(20),
    data_hora_abastecimento timestamp with time zone,
    codigo_bico bigint,
    codigo_produto text,
    quantidade numeric(18,4),
    valor_unitario numeric(18,4),
    valor_total numeric(18,2),
    preco_cadastro numeric(18,4),
    tabela_preco_a numeric(18,4),
    tabela_preco_b numeric(18,4),
    tabela_preco_c numeric(18,4),
    encerrante numeric(18,4),
    codigo_frentista bigint,
    afericao boolean,
    venda_item_codigo bigint,
    placa character varying(10),
    string_full text,
    codigo bigint,
    abastecimento_key character varying(255),
    data_fiscal_key integer
);


ALTER TABLE gold.postos_abastecimentos OWNER TO postgres;

--
-- TOC entry 446 (class 1259 OID 173556)
-- Name: postos_caixas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_caixas (
    nome_alias character varying(100) NOT NULL,
    empresa_codigo bigint NOT NULL,
    caixa_codigo bigint NOT NULL,
    data_movimento date,
    turno_codigo bigint,
    turno character varying(100),
    pdv_codigo bigint,
    funcionario_codigo bigint,
    centro_custo bigint,
    abertura timestamp with time zone,
    fechamento timestamp with time zone,
    fechado boolean,
    consolidado boolean,
    tipo_inclusao character varying(50),
    bloqueado boolean,
    tipo_bloqueio character varying(100),
    apurado numeric(18,2),
    diferenca numeric(18,2),
    data_hora_atualizacao timestamp with time zone,
    codigo bigint
);


ALTER TABLE gold.postos_caixas OWNER TO postgres;

--
-- TOC entry 448 (class 1259 OID 201652)
-- Name: postos_cartoes; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_cartoes (
    nome_alias character varying(100) NOT NULL,
    empresa_codigo bigint NOT NULL,
    cartao_codigo bigint NOT NULL,
    venda_codigo bigint,
    vencimento date,
    valor numeric(18,2),
    parcela integer,
    taxa_percentual numeric(18,4),
    administradora_codigo bigint,
    administradora_descricao character varying(150),
    cliente_referencia character varying(100),
    cliente_razao character varying(255),
    cliente_cpf_cnpj character varying(30),
    centro_custo_codigo bigint,
    centro_custo_descricao character varying(150),
    data_pagamento date,
    tipo_inclusao character varying(50),
    data_movimento date,
    hora_movimento time without time zone,
    data_fiscal date,
    pendente boolean,
    nsu character varying(50),
    autorizacao character varying(100),
    codigo bigint,
    codigo_bandeira character varying(20),
    nsu_tef character varying(100)
);


ALTER TABLE gold.postos_cartoes OWNER TO postgres;

--
-- TOC entry 441 (class 1259 OID 100071)
-- Name: postos_duplicatas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_duplicatas (
    nome_alias text,
    empresa_codigo bigint NOT NULL,
    duplicata_codigo bigint NOT NULL,
    data_movimento date,
    vencimento date,
    data_pagamento date,
    cliente_codigo bigint,
    nome_cliente text,
    cpf_cnpj_cliente text,
    numero_documento text,
    nosso_numero text,
    situacao text,
    pendente boolean,
    valor_duplicata numeric(18,2),
    valor_pago numeric(18,2),
    valor_acrescimo numeric(18,2),
    valor_desconto numeric(18,2),
    valor_liquido numeric(18,2),
    remessa_boleto text,
    hierarquia_plano_conta text,
    plano_conta_gerencial_codigo bigint,
    codigo bigint,
    duplicata_key character varying(255),
    data_movimento_key integer
);


ALTER TABLE gold.postos_duplicatas OWNER TO postgres;

--
-- TOC entry 442 (class 1259 OID 100490)
-- Name: postos_movimentos_conta; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_movimentos_conta (
    nome_alias text,
    empresa_codigo bigint NOT NULL,
    movimento_conta_codigo bigint NOT NULL,
    data_movimento date,
    evento timestamp with time zone,
    tipo text,
    tipo_documento_origem text,
    codigo_tipo_documento_origem bigint,
    documento_origem_codigo bigint,
    documento text,
    lote text,
    descricao text,
    valor numeric(18,2),
    saldo numeric(18,2),
    conciliado boolean,
    data_hora_conciliacao timestamp with time zone,
    usuario_conciliacao text,
    conta_codigo bigint,
    plano_conta_gerencial_codigo bigint,
    centro_custo_codigo bigint,
    codigo_pessoa bigint,
    tipo_pessoa text,
    codigo bigint,
    data_movimento_key integer
);


ALTER TABLE gold.postos_movimentos_conta OWNER TO postgres;

--
-- TOC entry 447 (class 1259 OID 174011)
-- Name: postos_sangrias_caixa; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_sangrias_caixa (
    nome_alias character varying(100) NOT NULL,
    sangria_codigo bigint NOT NULL,
    empresa_codigo bigint NOT NULL,
    caixa_codigo bigint NOT NULL,
    conta_codigo bigint,
    usuario_codigo bigint,
    funcionario_codigo bigint,
    dinheiro numeric(18,2),
    cheque numeric(18,2),
    cartao numeric(18,2),
    nota numeric(18,2),
    carta_frete numeric(18,2),
    emprestimo numeric(18,2),
    despesa numeric(18,2),
    cheque_pre numeric(18,2),
    vale numeric(18,2),
    transferencia numeric(18,2),
    data_sangria date,
    hora_sangria time without time zone,
    data_hora_coleta timestamp with time zone,
    data_hora_atualizacao timestamp with time zone,
    alterada boolean,
    coo character varying(20),
    numero_documento character varying(50),
    observacao text,
    codigo bigint
);


ALTER TABLE gold.postos_sangrias_caixa OWNER TO postgres;

--
-- TOC entry 443 (class 1259 OID 159047)
-- Name: postos_tanques; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_tanques (
    nome_alias character varying(100) NOT NULL,
    empresa_codigo bigint NOT NULL,
    tanque_codigo bigint NOT NULL,
    tanque_codigo_externo character varying(100),
    nome character varying(255),
    produto_codigo bigint,
    capacidade numeric(18,4),
    ultimo_usuario_alteracao character varying(100),
    lastro numeric(18,4),
    estoque_escritural numeric(18,4),
    produto_lmc_codigo bigint,
    data_hora_medidor timestamp with time zone,
    codigo bigint
);


ALTER TABLE gold.postos_tanques OWNER TO postgres;

--
-- TOC entry 444 (class 1259 OID 159341)
-- Name: postos_tanques_entradas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_tanques_entradas (
    nome_alias character varying(100) NOT NULL,
    tanque_codigo bigint NOT NULL,
    tanque_entrada_codigo bigint NOT NULL,
    data_hora_inicio timestamp with time zone,
    data_hora_fim timestamp with time zone,
    volume_inicio numeric(18,4),
    volume_fim numeric(18,4),
    volume_entrada numeric(18,4)
);


ALTER TABLE gold.postos_tanques_entradas OWNER TO postgres;

--
-- TOC entry 445 (class 1259 OID 166570)
-- Name: postos_tanques_medidas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.postos_tanques_medidas (
    nome_alias character varying(100) NOT NULL,
    tanque_codigo bigint NOT NULL,
    tanque_medida_codigo bigint NOT NULL,
    data_medida date,
    hora_medida time without time zone,
    volume numeric(18,4)
);


ALTER TABLE gold.postos_tanques_medidas OWNER TO postgres;

--
-- TOC entry 433 (class 1259 OID 17599)
-- Name: produto; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.produto (
    nome_alias text,
    cod_prod text,
    classific text,
    ucc numeric(18,4),
    cmc numeric(18,4),
    tipo_prod text,
    cod_ipi text,
    peso_bruto numeric(18,4),
    peso_liq numeric(18,4),
    cod_unid text,
    preco_base numeric(18,4),
    cmf numeric(18,4),
    ucf numeric(18,4),
    moeda text,
    cod_icms text,
    menor_fob numeric(18,4),
    menor_cif numeric(18,4),
    maior_fob numeric(18,4),
    maior_cif numeric(18,4),
    cmcsi numeric(18,4),
    especie text,
    marca text,
    cd_org_merc text,
    alic_icms numeric(18,4),
    localiza text,
    ativo integer,
    controla_numero_serie integer,
    unidade_por_serie numeric(18,4),
    nome_usual text,
    cst text,
    perc_vda_pis numeric(18,4),
    perc_vda_cof numeric(18,4),
    cod_nat_op_venda_estado text,
    cod_nat_op_venda_f_estado text,
    codigo_barra text,
    custo_base_nf numeric(18,4),
    codigo_dcp text,
    codigo_sicopi text,
    codigo_nbm text,
    codigo_ncm text,
    aplicacao text,
    cod_prod_tanque text,
    proporcao_dcp numeric(18,4),
    dpmp_codigo integer,
    tipo_item integer,
    observacoes text,
    codigo_dnf text,
    anp_comerc_codigo_produto text,
    nat_rec integer,
    cest text,
    id integer,
    descricao_completa text,
    descricao_nf text,
    descricao_reduzida text,
    anp_descricao text,
    referencia_codigo text,
    grupo_codigo bigint,
    combustivel boolean,
    produto_lmc_codigo bigint,
    tipo_combustivel text,
    unidade_compra text,
    sub_grupo1_codigo bigint,
    sub_grupo2_codigo bigint,
    sub_grupo3_codigo bigint,
    produto_codigo_externo text,
    tributacao_ad_rem numeric(18,4),
    descricao_fabricante text,
    registra_inventario text,
    mistura_biocombustivel numeric(18,4),
    perc_uf_origem_scanc numeric(18,4)
);


ALTER TABLE gold.produto OWNER TO postgres;

--
-- TOC entry 435 (class 1259 OID 21501)
-- Name: tipo_cobranca; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.tipo_cobranca (
    nome_alias character varying(100) NOT NULL,
    cod_tp_cobr character varying(20) NOT NULL,
    descricao character varying(255),
    cod_tp_cobr_key character varying(150) NOT NULL
);


ALTER TABLE gold.tipo_cobranca OWNER TO postgres;

--
-- TOC entry 421 (class 1259 OID 16710)
-- Name: tipo_documento; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.tipo_documento (
    nome_alias text CONSTRAINT tipodocumento_nome_alias_not_null NOT NULL,
    seq_docto bigint CONSTRAINT "tipodocumento_SeqDocto_not_null" NOT NULL,
    gnre text,
    tipo_documento_key text CONSTRAINT "tipodocumento_TipoDocumentoKey_not_null" NOT NULL
);


ALTER TABLE gold.tipo_documento OWNER TO postgres;

--
-- TOC entry 437 (class 1259 OID 62597)
-- Name: vendas_postos; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.vendas_postos (
    id bigint NOT NULL,
    nome_alias character varying(100) NOT NULL,
    empresa_codigo bigint NOT NULL,
    venda_codigo bigint NOT NULL,
    nota_codigo bigint,
    funcionario_codigo bigint,
    cliente_codigo bigint,
    destaca_acrescimo_desconto integer,
    cliente_cpf_cnpj character varying(30),
    data_hora timestamp with time zone,
    nota_numero character varying(50),
    nota_serie character varying(20),
    total_venda numeric(18,2),
    caixa_codigo bigint,
    nota_chave character varying(100),
    modelo_documento character varying(10),
    cancelada character varying(10),
    placa_veiculo character varying(20),
    cliente_codigo_externo character varying(100),
    centro_custo_codigo bigint,
    centro_custo_veiculo character varying(100),
    identificacao_fidelidade character varying(100),
    venda_uuid character varying(100),
    motorista_codigo bigint,
    codigo bigint,
    venda_key character varying(255) NOT NULL
);


ALTER TABLE gold.vendas_postos OWNER TO postgres;

--
-- TOC entry 438 (class 1259 OID 62614)
-- Name: vendas_postos_forma_pagamento; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.vendas_postos_forma_pagamento (
    empresa_codigo bigint,
    venda_codigo bigint,
    venda_prazo_codigo bigint,
    financeiro_codigo bigint,
    data_movimento date,
    vencimento date,
    valor_pagamento numeric(18,2),
    taxa_percentual numeric(18,4),
    taxa_valor numeric(18,2),
    forma_pagamento_codigo bigint,
    administradora_codigo bigint,
    turno_codigo bigint,
    tipo_forma_pagamento text,
    nome_forma_pagamento text,
    codigo bigint,
    venda_key text,
    venda_forma_pagamento_key text,
    nome_alias text
);


ALTER TABLE gold.vendas_postos_forma_pagamento OWNER TO postgres;

--
-- TOC entry 436 (class 1259 OID 62596)
-- Name: vendas_postos_id_seq; Type: SEQUENCE; Schema: gold; Owner: postgres
--

CREATE SEQUENCE gold.vendas_postos_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE gold.vendas_postos_id_seq OWNER TO postgres;

--
-- TOC entry 5370 (class 0 OID 0)
-- Dependencies: 436
-- Name: vendas_postos_id_seq; Type: SEQUENCE OWNED BY; Schema: gold; Owner: postgres
--

ALTER SEQUENCE gold.vendas_postos_id_seq OWNED BY gold.vendas_postos.id;


--
-- TOC entry 439 (class 1259 OID 62619)
-- Name: vendas_postos_itens; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.vendas_postos_itens (
    empresa_codigo bigint,
    venda_codigo bigint,
    venda_item_codigo bigint,
    data_movimento date,
    produto_codigo bigint,
    quantidade numeric(18,4),
    preco_custo numeric(18,4),
    total_custo numeric(18,2),
    preco_venda numeric(18,4),
    total_venda numeric(18,2),
    total_desconto numeric(18,2),
    total_acrescimo numeric(18,2),
    bico_codigo bigint,
    tanque_codigo bigint,
    produto_lmc_codigo bigint,
    funcionario_codigo bigint,
    produto_kit_codigo bigint,
    controle_item bigint,
    icms_valor numeric(18,2),
    icms_base numeric(18,2),
    icms_aliquota numeric(18,4),
    cfop text,
    cst text,
    produto_codigo_externo text,
    cst_pis text,
    aliquota_pis numeric(18,4),
    base_pis numeric(18,2),
    valor_pis numeric(18,2),
    cst_cofins text,
    aliquota_cofins numeric(18,4),
    base_cofins numeric(18,2),
    valor_cofins numeric(18,2),
    tributacao_ad_rem numeric(18,4),
    valor_tributacao_ad_rem numeric(18,2),
    total_venda_bruto numeric(18,2),
    total_desconto_bruto numeric(18,2),
    total_acrescimo_bruto numeric(18,2),
    preco_venda_bruto numeric(18,4),
    cst_cbs text,
    class_tributaria_cbs text,
    bc_cbs numeric(18,2),
    aliquota_efetiva_cbs numeric(18,4),
    valor_cbs numeric(18,2),
    cst_ibs text,
    class_tributaria_ibs text,
    bc_ibs numeric(18,2),
    aliquota_efetiva_ibs numeric(18,4),
    valor_ibs numeric(18,2),
    aliquota_ad_rem_cbs numeric(18,4),
    valor_ad_rem_cbs numeric(18,2),
    aliquota_ad_rem_ibs numeric(18,4),
    valor_ad_rem_ibs numeric(18,2),
    aliquota_cbs numeric(18,4),
    aliquota_ibs numeric(18,4),
    codigo bigint,
    venda_key text,
    venda_item_key text,
    nome_alias text
);


ALTER TABLE gold.vendas_postos_itens OWNER TO postgres;

--
-- TOC entry 419 (class 1259 OID 16690)
-- Name: vendedor; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.vendedor (
    nome_alias text NOT NULL,
    cod_vend character varying(50) CONSTRAINT "vendedor_CodVend_not_null" NOT NULL,
    descricao text,
    vendedor_key text CONSTRAINT "vendedor_VendedorKey_not_null" NOT NULL
);


ALTER TABLE gold.vendedor OWNER TO postgres;

--
-- TOC entry 423 (class 1259 OID 16730)
-- Name: z_classificacao_despesas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.z_classificacao_despesas (
    nome_alias text CONSTRAINT z_classificacaodespesas_nome_alias_not_null NOT NULL,
    id bigint CONSTRAINT "z_classificacaodespesas_ID_not_null" NOT NULL,
    descricao text,
    classificacao_despesa_key text CONSTRAINT "z_classificacaodespesas_ClassificacaoDespesaKey_not_null" NOT NULL
);


ALTER TABLE gold.z_classificacao_despesas OWNER TO postgres;

--
-- TOC entry 416 (class 1259 OID 16656)
-- Name: z_nota_fiscal_classificacao_despesas; Type: TABLE; Schema: gold; Owner: postgres
--

CREATE TABLE gold.z_nota_fiscal_classificacao_despesas (
    nome_alias text CONSTRAINT z_notafiscal_classificacaodespesas_nome_alias_not_null NOT NULL,
    cod_empresa integer CONSTRAINT "z_notafiscal_classificacaodespesas_CodEmpresa_not_null" NOT NULL,
    sequencial_nf bigint CONSTRAINT "z_notafiscal_classificacaodespesas_SequencialNF_not_null" NOT NULL,
    id_classificacao_despesas bigint CONSTRAINT z_nota_fiscal_classificacao_despesas_id_classificacao_despesas_ NOT NULL,
    detalhamento_despesa text,
    nota_fiscal_key text CONSTRAINT "z_notafiscal_classificacaodespesas_NotaFiscalKey_not_null" NOT NULL,
    classificacao_despesa_key text CONSTRAINT z_nota_fiscal_classificacao_despesas_classificacao_despesa_key_ NOT NULL,
    empresa_key text CONSTRAINT "z_notafiscal_classificacaodespesas_EmpresaKey_not_null" NOT NULL
);


ALTER TABLE gold.z_nota_fiscal_classificacao_despesas OWNER TO postgres;

--
-- TOC entry 5072 (class 2604 OID 62600)
-- Name: vendas_postos id; Type: DEFAULT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.vendas_postos ALTER COLUMN id SET DEFAULT nextval('gold.vendas_postos_id_seq'::regclass);


--
-- TOC entry 5140 (class 2606 OID 16759)
-- Name: centro_custo pk_gold_centro_custo; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.centro_custo
    ADD CONSTRAINT pk_gold_centro_custo PRIMARY KEY (centro_custo_key);


--
-- TOC entry 5109 (class 2606 OID 16655)
-- Name: centro_custo_pagar pk_gold_centro_custo_pagar; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.centro_custo_pagar
    ADD CONSTRAINT pk_gold_centro_custo_pagar PRIMARY KEY (cpr_key, centro_custo_key);


--
-- TOC entry 5105 (class 2606 OID 16641)
-- Name: centro_custo_rateio pk_gold_centro_custo_rateio; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.centro_custo_rateio
    ADD CONSTRAINT pk_gold_centro_custo_rateio PRIMARY KEY (lote_key, centro_custo_key);


--
-- TOC entry 5082 (class 2606 OID 16575)
-- Name: contas_pagar_receber pk_gold_contas_pagar_receber; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.contas_pagar_receber
    ADD CONSTRAINT pk_gold_contas_pagar_receber PRIMARY KEY (cpr_key);


--
-- TOC entry 5085 (class 2606 OID 16587)
-- Name: contas_pagar_receber_moeda pk_gold_contas_pagar_receber_moeda; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.contas_pagar_receber_moeda
    ADD CONSTRAINT pk_gold_contas_pagar_receber_moeda PRIMARY KEY (cpr_key);


--
-- TOC entry 5116 (class 2606 OID 16679)
-- Name: empresas pk_gold_empresas; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.empresas
    ADD CONSTRAINT pk_gold_empresas PRIMARY KEY (empresa_key);


--
-- TOC entry 5119 (class 2606 OID 16689)
-- Name: entidade pk_gold_entidade; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.entidade
    ADD CONSTRAINT pk_gold_entidade PRIMARY KEY (entidade_key);


--
-- TOC entry 5097 (class 2606 OID 16613)
-- Name: itens_nf pk_gold_itens_nf; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.itens_nf
    ADD CONSTRAINT pk_gold_itens_nf PRIMARY KEY (item_nota_fiscal_key);


--
-- TOC entry 5101 (class 2606 OID 16627)
-- Name: movimento_plano_contas pk_gold_movimento_plano_contas; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.movimento_plano_contas
    ADD CONSTRAINT pk_gold_movimento_plano_contas PRIMARY KEY (lote_key, plano_conta_key);


--
-- TOC entry 5131 (class 2606 OID 16729)
-- Name: natureza_operacao pk_gold_natureza_operacao; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.natureza_operacao
    ADD CONSTRAINT pk_gold_natureza_operacao PRIMARY KEY (natureza_operacao_key);


--
-- TOC entry 5093 (class 2606 OID 16599)
-- Name: nota_fiscal pk_gold_nota_fiscal; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.nota_fiscal
    ADD CONSTRAINT pk_gold_nota_fiscal PRIMARY KEY (nota_fiscal_key);


--
-- TOC entry 5137 (class 2606 OID 16749)
-- Name: plano_contas pk_gold_plano_contas; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.plano_contas
    ADD CONSTRAINT pk_gold_plano_contas PRIMARY KEY (plano_conta_key);


--
-- TOC entry 5125 (class 2606 OID 16709)
-- Name: portador pk_gold_portador; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.portador
    ADD CONSTRAINT pk_gold_portador PRIMARY KEY (portador_key);


--
-- TOC entry 5128 (class 2606 OID 16719)
-- Name: tipo_documento pk_gold_tipo_documento; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.tipo_documento
    ADD CONSTRAINT pk_gold_tipo_documento PRIMARY KEY (tipo_documento_key);


--
-- TOC entry 5122 (class 2606 OID 16699)
-- Name: vendedor pk_gold_vendedor; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.vendedor
    ADD CONSTRAINT pk_gold_vendedor PRIMARY KEY (vendedor_key);


--
-- TOC entry 5134 (class 2606 OID 16739)
-- Name: z_classificacao_despesas pk_gold_z_classificacao_despesas; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.z_classificacao_despesas
    ADD CONSTRAINT pk_gold_z_classificacao_despesas PRIMARY KEY (classificacao_despesa_key);


--
-- TOC entry 5113 (class 2606 OID 16669)
-- Name: z_nota_fiscal_classificacao_despesas pk_gold_z_nota_fiscal_classificacao_despesas; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.z_nota_fiscal_classificacao_despesas
    ADD CONSTRAINT pk_gold_z_nota_fiscal_classificacao_despesas PRIMARY KEY (nota_fiscal_key, classificacao_despesa_key);


--
-- TOC entry 5152 (class 2606 OID 21513)
-- Name: tipo_cobranca uq_gold_tipo_cobranca; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.tipo_cobranca
    ADD CONSTRAINT uq_gold_tipo_cobranca UNIQUE (cod_tp_cobr_key);


--
-- TOC entry 5155 (class 2606 OID 62611)
-- Name: vendas_postos uq_vendas_postos_venda_key; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.vendas_postos
    ADD CONSTRAINT uq_vendas_postos_venda_key UNIQUE (venda_key);


--
-- TOC entry 5158 (class 2606 OID 62609)
-- Name: vendas_postos vendas_postos_pkey; Type: CONSTRAINT; Schema: gold; Owner: postgres
--

ALTER TABLE ONLY gold.vendas_postos
    ADD CONSTRAINT vendas_postos_pkey PRIMARY KEY (id);


--
-- TOC entry 5161 (class 1259 OID 94328)
-- Name: ix_abastecimento_data_fiscal; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_abastecimento_data_fiscal ON gold.postos_abastecimentos USING btree (data_fiscal);


--
-- TOC entry 5162 (class 1259 OID 94327)
-- Name: ix_abastecimento_empresa_codigo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_abastecimento_empresa_codigo ON gold.postos_abastecimentos USING btree (empresa_codigo, abastecimento_codigo);


--
-- TOC entry 5107 (class 1259 OID 16776)
-- Name: ix_ccpagar_cpr; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_ccpagar_cpr ON gold.centro_custo_pagar USING btree (cpr_key);


--
-- TOC entry 5103 (class 1259 OID 16775)
-- Name: ix_ccrateio_lote; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_ccrateio_lote ON gold.centro_custo_rateio USING btree (lote_key);


--
-- TOC entry 5111 (class 1259 OID 16777)
-- Name: ix_classificacao_nf; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_classificacao_nf ON gold.z_nota_fiscal_classificacao_despesas USING btree (nota_fiscal_key);


--
-- TOC entry 5073 (class 1259 OID 16764)
-- Name: ix_cpr_centro_custo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_cpr_centro_custo ON gold.contas_pagar_receber USING btree (centro_custo_key);


--
-- TOC entry 5074 (class 1259 OID 16766)
-- Name: ix_cpr_dataemissao; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_cpr_dataemissao ON gold.contas_pagar_receber USING btree (data_emissao_key);


--
-- TOC entry 5075 (class 1259 OID 16767)
-- Name: ix_cpr_datavencimento; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_cpr_datavencimento ON gold.contas_pagar_receber USING btree (data_vencimento_key);


--
-- TOC entry 5076 (class 1259 OID 16760)
-- Name: ix_cpr_empresa; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_cpr_empresa ON gold.contas_pagar_receber USING btree (empresa_key);


--
-- TOC entry 5077 (class 1259 OID 16761)
-- Name: ix_cpr_entidade; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_cpr_entidade ON gold.contas_pagar_receber USING btree (entidade_key);


--
-- TOC entry 5078 (class 1259 OID 16765)
-- Name: ix_cpr_lote; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_cpr_lote ON gold.contas_pagar_receber USING btree (lote_key);


--
-- TOC entry 5079 (class 1259 OID 16762)
-- Name: ix_cpr_nota_fiscal; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_cpr_nota_fiscal ON gold.contas_pagar_receber USING btree (nota_fiscal_key);


--
-- TOC entry 5080 (class 1259 OID 16763)
-- Name: ix_cpr_tipo_documento; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_cpr_tipo_documento ON gold.contas_pagar_receber USING btree (tipo_documento_key);


--
-- TOC entry 5164 (class 1259 OID 100079)
-- Name: ix_duplicatas_data_movimento; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_duplicatas_data_movimento ON gold.postos_duplicatas USING btree (data_movimento);


--
-- TOC entry 5165 (class 1259 OID 100078)
-- Name: ix_duplicatas_empresa_codigo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_duplicatas_empresa_codigo ON gold.postos_duplicatas USING btree (empresa_codigo, duplicata_codigo);


--
-- TOC entry 5095 (class 1259 OID 16773)
-- Name: ix_itens_nf_nota_fiscal; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_itens_nf_nota_fiscal ON gold.itens_nf USING btree (nota_fiscal_key);


--
-- TOC entry 5167 (class 1259 OID 100498)
-- Name: ix_movconta_data_movimento; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_movconta_data_movimento ON gold.postos_movimentos_conta USING btree (data_movimento);


--
-- TOC entry 5168 (class 1259 OID 100497)
-- Name: ix_movconta_empresa_movimento; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_movconta_empresa_movimento ON gold.postos_movimentos_conta USING btree (empresa_codigo, movimento_conta_codigo);


--
-- TOC entry 5099 (class 1259 OID 16774)
-- Name: ix_mpc_lote; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_mpc_lote ON gold.movimento_plano_contas USING btree (lote_key);


--
-- TOC entry 5087 (class 1259 OID 16772)
-- Name: ix_nf_data; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_nf_data ON gold.nota_fiscal USING btree (data_nota_fiscal_key);


--
-- TOC entry 5088 (class 1259 OID 16768)
-- Name: ix_nf_empresa; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_nf_empresa ON gold.nota_fiscal USING btree (empresa_key);


--
-- TOC entry 5089 (class 1259 OID 16769)
-- Name: ix_nf_entidade; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_nf_entidade ON gold.nota_fiscal USING btree (entidade_key);


--
-- TOC entry 5090 (class 1259 OID 16771)
-- Name: ix_nf_lote; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_nf_lote ON gold.nota_fiscal USING btree (lote_key);


--
-- TOC entry 5091 (class 1259 OID 16770)
-- Name: ix_nf_natureza_operacao; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_nf_natureza_operacao ON gold.nota_fiscal USING btree (natureza_operacao_key);


--
-- TOC entry 5173 (class 1259 OID 173563)
-- Name: ix_postos_caixas_data_movimento; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_postos_caixas_data_movimento ON gold.postos_caixas USING btree (data_movimento);


--
-- TOC entry 5174 (class 1259 OID 173564)
-- Name: ix_postos_caixas_empresa; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_postos_caixas_empresa ON gold.postos_caixas USING btree (empresa_codigo);


--
-- TOC entry 5179 (class 1259 OID 201661)
-- Name: ix_postos_cartoes_data_movimento; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_postos_cartoes_data_movimento ON gold.postos_cartoes USING btree (data_movimento);


--
-- TOC entry 5176 (class 1259 OID 174021)
-- Name: ix_postos_sangrias_caixa_data; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_postos_sangrias_caixa_data ON gold.postos_sangrias_caixa USING btree (data_sangria);


--
-- TOC entry 5177 (class 1259 OID 174022)
-- Name: ix_postos_sangrias_caixa_empresa; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE INDEX ix_postos_sangrias_caixa_empresa ON gold.postos_sangrias_caixa USING btree (empresa_codigo);


--
-- TOC entry 5146 (class 1259 OID 18880)
-- Name: ux_baixa_antecipada_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_baixa_antecipada_pombo ON gold.baixa_antecipada USING btree (baixa_antecipada_key);


--
-- TOC entry 5148 (class 1259 OID 18907)
-- Name: ux_baixa_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_baixa_pombo ON gold.baixa USING btree (baixa_key);


--
-- TOC entry 5145 (class 1259 OID 18874)
-- Name: ux_bandeira_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_bandeira_pombo ON gold.bandeira USING btree (nome_alias, cod_bandeira);


--
-- TOC entry 5110 (class 1259 OID 17694)
-- Name: ux_centro_custo_pagar_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_centro_custo_pagar_pombo ON gold.centro_custo_pagar USING btree (nome_alias, cod_empresa, num_lancamento, seq_cc);


--
-- TOC entry 5141 (class 1259 OID 17969)
-- Name: ux_centro_custo_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_centro_custo_pombo ON gold.centro_custo USING btree (centro_custo_key);


--
-- TOC entry 5106 (class 1259 OID 17681)
-- Name: ux_centro_custo_rateio_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_centro_custo_rateio_pombo ON gold.centro_custo_rateio USING btree (nome_alias, cod_empresa, num_lote, seq_cc);


--
-- TOC entry 5144 (class 1259 OID 18868)
-- Name: ux_cidade_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_cidade_pombo ON gold.cidade USING btree (nome_alias, codigo_dnc);


--
-- TOC entry 5086 (class 1259 OID 17623)
-- Name: ux_contas_pagar_receber_moeda_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_contas_pagar_receber_moeda_pombo ON gold.contas_pagar_receber_moeda USING btree (cpr_key);


--
-- TOC entry 5083 (class 1259 OID 17610)
-- Name: ux_contas_pagar_receber_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_contas_pagar_receber_pombo ON gold.contas_pagar_receber USING btree (cpr_key);


--
-- TOC entry 5150 (class 1259 OID 19430)
-- Name: ux_deposito_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_deposito_pombo ON gold.deposito USING btree (nome_alias, cod_dep);


--
-- TOC entry 5117 (class 1259 OID 17720)
-- Name: ux_empresas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_empresas_pombo ON gold.empresas USING btree (empresa_key);


--
-- TOC entry 5120 (class 1259 OID 17729)
-- Name: ux_entidade_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_entidade_pombo ON gold.entidade USING btree (entidade_key);


--
-- TOC entry 5147 (class 1259 OID 18887)
-- Name: ux_itens_baixa_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_itens_baixa_pombo ON gold.itens_baixa USING btree (nome_alias, cod_empresa, num_baixa, item);


--
-- TOC entry 5098 (class 1259 OID 17653)
-- Name: ux_itens_nf_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_itens_nf_pombo ON gold.itens_nf USING btree (item_nota_fiscal_key);


--
-- TOC entry 5143 (class 1259 OID 18830)
-- Name: ux_itens_pedido_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_itens_pedido_pombo ON gold.itens_pedido USING btree (nome_alias, cod_empresa, num_ped, seq);


--
-- TOC entry 5102 (class 1259 OID 17667)
-- Name: ux_movimento_plano_contas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_movimento_plano_contas_pombo ON gold.movimento_plano_contas USING btree (nome_alias, cod_empresa, num_lote, cod_conta);


--
-- TOC entry 5132 (class 1259 OID 17942)
-- Name: ux_natureza_operacao_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_natureza_operacao_pombo ON gold.natureza_operacao USING btree (natureza_operacao_key);


--
-- TOC entry 5094 (class 1259 OID 17634)
-- Name: ux_nota_fiscal_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_nota_fiscal_pombo ON gold.nota_fiscal USING btree (nota_fiscal_key);


--
-- TOC entry 5142 (class 1259 OID 18235)
-- Name: ux_pedido_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_pedido_pombo ON gold.pedido USING btree (pedido_key);


--
-- TOC entry 5138 (class 1259 OID 17960)
-- Name: ux_plano_contas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_plano_contas_pombo ON gold.plano_contas USING btree (plano_conta_key);


--
-- TOC entry 5126 (class 1259 OID 17924)
-- Name: ux_portador_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_portador_pombo ON gold.portador USING btree (portador_key);


--
-- TOC entry 5163 (class 1259 OID 94496)
-- Name: ux_postos_abastecimentos_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_abastecimentos_pombo ON gold.postos_abastecimentos USING btree (empresa_codigo, abastecimento_codigo);


--
-- TOC entry 5175 (class 1259 OID 173562)
-- Name: ux_postos_caixas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_caixas_pombo ON gold.postos_caixas USING btree (nome_alias, empresa_codigo, caixa_codigo);


--
-- TOC entry 5180 (class 1259 OID 201660)
-- Name: ux_postos_cartoes_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_cartoes_pombo ON gold.postos_cartoes USING btree (nome_alias, empresa_codigo, cartao_codigo);


--
-- TOC entry 5166 (class 1259 OID 100227)
-- Name: ux_postos_duplicatas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_duplicatas_pombo ON gold.postos_duplicatas USING btree (empresa_codigo, duplicata_codigo);


--
-- TOC entry 5169 (class 1259 OID 100570)
-- Name: ux_postos_movimentos_conta_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_movimentos_conta_pombo ON gold.postos_movimentos_conta USING btree (empresa_codigo, movimento_conta_codigo);


--
-- TOC entry 5178 (class 1259 OID 174020)
-- Name: ux_postos_sangrias_caixa_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_sangrias_caixa_pombo ON gold.postos_sangrias_caixa USING btree (nome_alias, empresa_codigo, sangria_codigo);


--
-- TOC entry 5171 (class 1259 OID 159347)
-- Name: ux_postos_tanques_entradas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_tanques_entradas_pombo ON gold.postos_tanques_entradas USING btree (nome_alias, tanque_codigo, tanque_entrada_codigo);


--
-- TOC entry 5172 (class 1259 OID 166576)
-- Name: ux_postos_tanques_medidas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_tanques_medidas_pombo ON gold.postos_tanques_medidas USING btree (nome_alias, tanque_codigo, tanque_medida_codigo);


--
-- TOC entry 5170 (class 1259 OID 159055)
-- Name: ux_postos_tanques_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_postos_tanques_pombo ON gold.postos_tanques USING btree (nome_alias, empresa_codigo, tanque_codigo);


--
-- TOC entry 5149 (class 1259 OID 18914)
-- Name: ux_produto_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_produto_pombo ON gold.produto USING btree (nome_alias, cod_prod);


--
-- TOC entry 5153 (class 1259 OID 22118)
-- Name: ux_tipo_cobranca_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_tipo_cobranca_pombo ON gold.tipo_cobranca USING btree (cod_tp_cobr_key);


--
-- TOC entry 5129 (class 1259 OID 17933)
-- Name: ux_tipo_documento_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_tipo_documento_pombo ON gold.tipo_documento USING btree (tipo_documento_key);


--
-- TOC entry 5159 (class 1259 OID 86059)
-- Name: ux_vendas_postos_forma_pagamento_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_vendas_postos_forma_pagamento_pombo ON gold.vendas_postos_forma_pagamento USING btree (venda_forma_pagamento_key);


--
-- TOC entry 5160 (class 1259 OID 86065)
-- Name: ux_vendas_postos_itens_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_vendas_postos_itens_pombo ON gold.vendas_postos_itens USING btree (venda_item_key);


--
-- TOC entry 5156 (class 1259 OID 77750)
-- Name: ux_vendas_postos_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_vendas_postos_pombo ON gold.vendas_postos USING btree (venda_key);


--
-- TOC entry 5123 (class 1259 OID 17915)
-- Name: ux_vendedor_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_vendedor_pombo ON gold.vendedor USING btree (vendedor_key);


--
-- TOC entry 5135 (class 1259 OID 17951)
-- Name: ux_z_classificacao_despesas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_z_classificacao_despesas_pombo ON gold.z_classificacao_despesas USING btree (classificacao_despesa_key);


--
-- TOC entry 5114 (class 1259 OID 17707)
-- Name: ux_z_nota_fiscal_classificacao_despesas_pombo; Type: INDEX; Schema: gold; Owner: postgres
--

CREATE UNIQUE INDEX ux_z_nota_fiscal_classificacao_despesas_pombo ON gold.z_nota_fiscal_classificacao_despesas USING btree (nome_alias, cod_empresa, sequencial_nf, id_classificacao_despesas);


--
-- TOC entry 5181 (class 2620 OID 21489)
-- Name: empresas trg_ajustar_nomeusual_empresas; Type: TRIGGER; Schema: gold; Owner: postgres
--

CREATE TRIGGER trg_ajustar_nomeusual_empresas BEFORE INSERT OR UPDATE OF cnpj, nome_usual ON gold.empresas FOR EACH ROW EXECUTE FUNCTION gold.fn_ajustar_nomeusual_empresas();


--
-- TOC entry 5334 (class 0 OID 0)
-- Dependencies: 139
-- Name: SCHEMA gold; Type: ACL; Schema: -; Owner: hub_admin
--

GRANT ALL ON SCHEMA gold TO etl_pombo;
GRANT USAGE ON SCHEMA gold TO bi_reader;
GRANT USAGE ON SCHEMA gold TO ti_hub;


--
-- TOC entry 5335 (class 0 OID 0)
-- Dependencies: 432
-- Name: TABLE baixa; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.baixa TO etl_pombo;
GRANT SELECT ON TABLE gold.baixa TO bi_reader;
GRANT ALL ON TABLE gold.baixa TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.baixa TO ti_hub;


--
-- TOC entry 5336 (class 0 OID 0)
-- Dependencies: 430
-- Name: TABLE baixa_antecipada; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.baixa_antecipada TO etl_pombo;
GRANT SELECT ON TABLE gold.baixa_antecipada TO bi_reader;
GRANT ALL ON TABLE gold.baixa_antecipada TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.baixa_antecipada TO ti_hub;


--
-- TOC entry 5337 (class 0 OID 0)
-- Dependencies: 429
-- Name: TABLE bandeira; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.bandeira TO etl_pombo;
GRANT SELECT ON TABLE gold.bandeira TO bi_reader;
GRANT ALL ON TABLE gold.bandeira TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.bandeira TO ti_hub;


--
-- TOC entry 5338 (class 0 OID 0)
-- Dependencies: 425
-- Name: TABLE centro_custo; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.centro_custo TO etl_pombo;
GRANT SELECT ON TABLE gold.centro_custo TO bi_reader;
GRANT ALL ON TABLE gold.centro_custo TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.centro_custo TO ti_hub;


--
-- TOC entry 5339 (class 0 OID 0)
-- Dependencies: 415
-- Name: TABLE centro_custo_pagar; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.centro_custo_pagar TO etl_pombo;
GRANT SELECT ON TABLE gold.centro_custo_pagar TO bi_reader;
GRANT ALL ON TABLE gold.centro_custo_pagar TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.centro_custo_pagar TO ti_hub;


--
-- TOC entry 5340 (class 0 OID 0)
-- Dependencies: 414
-- Name: TABLE centro_custo_rateio; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.centro_custo_rateio TO etl_pombo;
GRANT SELECT ON TABLE gold.centro_custo_rateio TO bi_reader;
GRANT ALL ON TABLE gold.centro_custo_rateio TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.centro_custo_rateio TO ti_hub;


--
-- TOC entry 5341 (class 0 OID 0)
-- Dependencies: 428
-- Name: TABLE cidade; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.cidade TO etl_pombo;
GRANT SELECT ON TABLE gold.cidade TO bi_reader;
GRANT ALL ON TABLE gold.cidade TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.cidade TO ti_hub;


--
-- TOC entry 5342 (class 0 OID 0)
-- Dependencies: 409
-- Name: TABLE contas_pagar_receber; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.contas_pagar_receber TO etl_pombo;
GRANT SELECT ON TABLE gold.contas_pagar_receber TO bi_reader;
GRANT ALL ON TABLE gold.contas_pagar_receber TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.contas_pagar_receber TO ti_hub;


--
-- TOC entry 5343 (class 0 OID 0)
-- Dependencies: 410
-- Name: TABLE contas_pagar_receber_moeda; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.contas_pagar_receber_moeda TO etl_pombo;
GRANT SELECT ON TABLE gold.contas_pagar_receber_moeda TO bi_reader;
GRANT ALL ON TABLE gold.contas_pagar_receber_moeda TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.contas_pagar_receber_moeda TO ti_hub;


--
-- TOC entry 5344 (class 0 OID 0)
-- Dependencies: 434
-- Name: TABLE deposito; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.deposito TO etl_pombo;
GRANT SELECT ON TABLE gold.deposito TO bi_reader;
GRANT ALL ON TABLE gold.deposito TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.deposito TO ti_hub;


--
-- TOC entry 5345 (class 0 OID 0)
-- Dependencies: 417
-- Name: TABLE empresas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.empresas TO etl_pombo;
GRANT SELECT ON TABLE gold.empresas TO bi_reader;
GRANT ALL ON TABLE gold.empresas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.empresas TO ti_hub;


--
-- TOC entry 5346 (class 0 OID 0)
-- Dependencies: 418
-- Name: TABLE entidade; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.entidade TO etl_pombo;
GRANT SELECT ON TABLE gold.entidade TO bi_reader;
GRANT ALL ON TABLE gold.entidade TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.entidade TO ti_hub;


--
-- TOC entry 5347 (class 0 OID 0)
-- Dependencies: 431
-- Name: TABLE itens_baixa; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.itens_baixa TO etl_pombo;
GRANT SELECT ON TABLE gold.itens_baixa TO bi_reader;
GRANT ALL ON TABLE gold.itens_baixa TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.itens_baixa TO ti_hub;


--
-- TOC entry 5348 (class 0 OID 0)
-- Dependencies: 412
-- Name: TABLE itens_nf; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.itens_nf TO etl_pombo;
GRANT SELECT ON TABLE gold.itens_nf TO bi_reader;
GRANT ALL ON TABLE gold.itens_nf TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.itens_nf TO ti_hub;


--
-- TOC entry 5349 (class 0 OID 0)
-- Dependencies: 427
-- Name: TABLE itens_pedido; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.itens_pedido TO etl_pombo;
GRANT SELECT ON TABLE gold.itens_pedido TO bi_reader;
GRANT ALL ON TABLE gold.itens_pedido TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.itens_pedido TO ti_hub;


--
-- TOC entry 5350 (class 0 OID 0)
-- Dependencies: 413
-- Name: TABLE movimento_plano_contas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.movimento_plano_contas TO etl_pombo;
GRANT SELECT ON TABLE gold.movimento_plano_contas TO bi_reader;
GRANT ALL ON TABLE gold.movimento_plano_contas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.movimento_plano_contas TO ti_hub;


--
-- TOC entry 5351 (class 0 OID 0)
-- Dependencies: 422
-- Name: TABLE natureza_operacao; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.natureza_operacao TO etl_pombo;
GRANT SELECT ON TABLE gold.natureza_operacao TO bi_reader;
GRANT ALL ON TABLE gold.natureza_operacao TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.natureza_operacao TO ti_hub;


--
-- TOC entry 5352 (class 0 OID 0)
-- Dependencies: 411
-- Name: TABLE nota_fiscal; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.nota_fiscal TO etl_pombo;
GRANT SELECT ON TABLE gold.nota_fiscal TO bi_reader;
GRANT ALL ON TABLE gold.nota_fiscal TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.nota_fiscal TO ti_hub;


--
-- TOC entry 5353 (class 0 OID 0)
-- Dependencies: 426
-- Name: TABLE pedido; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.pedido TO etl_pombo;
GRANT SELECT ON TABLE gold.pedido TO bi_reader;
GRANT ALL ON TABLE gold.pedido TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.pedido TO ti_hub;


--
-- TOC entry 5354 (class 0 OID 0)
-- Dependencies: 424
-- Name: TABLE plano_contas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.plano_contas TO etl_pombo;
GRANT SELECT ON TABLE gold.plano_contas TO bi_reader;
GRANT ALL ON TABLE gold.plano_contas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.plano_contas TO ti_hub;


--
-- TOC entry 5355 (class 0 OID 0)
-- Dependencies: 420
-- Name: TABLE portador; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.portador TO etl_pombo;
GRANT SELECT ON TABLE gold.portador TO bi_reader;
GRANT ALL ON TABLE gold.portador TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.portador TO ti_hub;


--
-- TOC entry 5356 (class 0 OID 0)
-- Dependencies: 440
-- Name: TABLE postos_abastecimentos; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_abastecimentos TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_abastecimentos TO bi_reader;
GRANT ALL ON TABLE gold.postos_abastecimentos TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_abastecimentos TO ti_hub;


--
-- TOC entry 5357 (class 0 OID 0)
-- Dependencies: 446
-- Name: TABLE postos_caixas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_caixas TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_caixas TO bi_reader;
GRANT ALL ON TABLE gold.postos_caixas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_caixas TO ti_hub;


--
-- TOC entry 5358 (class 0 OID 0)
-- Dependencies: 448
-- Name: TABLE postos_cartoes; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_cartoes TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_cartoes TO bi_reader;
GRANT ALL ON TABLE gold.postos_cartoes TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_cartoes TO ti_hub;


--
-- TOC entry 5359 (class 0 OID 0)
-- Dependencies: 441
-- Name: TABLE postos_duplicatas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_duplicatas TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_duplicatas TO bi_reader;
GRANT ALL ON TABLE gold.postos_duplicatas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_duplicatas TO ti_hub;


--
-- TOC entry 5360 (class 0 OID 0)
-- Dependencies: 442
-- Name: TABLE postos_movimentos_conta; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_movimentos_conta TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_movimentos_conta TO bi_reader;
GRANT ALL ON TABLE gold.postos_movimentos_conta TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_movimentos_conta TO ti_hub;


--
-- TOC entry 5361 (class 0 OID 0)
-- Dependencies: 447
-- Name: TABLE postos_sangrias_caixa; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_sangrias_caixa TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_sangrias_caixa TO bi_reader;
GRANT ALL ON TABLE gold.postos_sangrias_caixa TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_sangrias_caixa TO ti_hub;


--
-- TOC entry 5362 (class 0 OID 0)
-- Dependencies: 443
-- Name: TABLE postos_tanques; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_tanques TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_tanques TO bi_reader;
GRANT ALL ON TABLE gold.postos_tanques TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_tanques TO ti_hub;


--
-- TOC entry 5363 (class 0 OID 0)
-- Dependencies: 444
-- Name: TABLE postos_tanques_entradas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_tanques_entradas TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_tanques_entradas TO bi_reader;
GRANT ALL ON TABLE gold.postos_tanques_entradas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_tanques_entradas TO ti_hub;


--
-- TOC entry 5364 (class 0 OID 0)
-- Dependencies: 445
-- Name: TABLE postos_tanques_medidas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_tanques_medidas TO etl_pombo;
GRANT SELECT ON TABLE gold.postos_tanques_medidas TO bi_reader;
GRANT ALL ON TABLE gold.postos_tanques_medidas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.postos_tanques_medidas TO ti_hub;


--
-- TOC entry 5365 (class 0 OID 0)
-- Dependencies: 433
-- Name: TABLE produto; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.produto TO etl_pombo;
GRANT SELECT ON TABLE gold.produto TO bi_reader;
GRANT ALL ON TABLE gold.produto TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.produto TO ti_hub;


--
-- TOC entry 5366 (class 0 OID 0)
-- Dependencies: 435
-- Name: TABLE tipo_cobranca; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.tipo_cobranca TO etl_pombo;
GRANT SELECT ON TABLE gold.tipo_cobranca TO bi_reader;
GRANT ALL ON TABLE gold.tipo_cobranca TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.tipo_cobranca TO ti_hub;


--
-- TOC entry 5367 (class 0 OID 0)
-- Dependencies: 421
-- Name: TABLE tipo_documento; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.tipo_documento TO etl_pombo;
GRANT SELECT ON TABLE gold.tipo_documento TO bi_reader;
GRANT ALL ON TABLE gold.tipo_documento TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.tipo_documento TO ti_hub;


--
-- TOC entry 5368 (class 0 OID 0)
-- Dependencies: 437
-- Name: TABLE vendas_postos; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.vendas_postos TO etl_pombo;
GRANT SELECT ON TABLE gold.vendas_postos TO bi_reader;
GRANT ALL ON TABLE gold.vendas_postos TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.vendas_postos TO ti_hub;


--
-- TOC entry 5369 (class 0 OID 0)
-- Dependencies: 438
-- Name: TABLE vendas_postos_forma_pagamento; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.vendas_postos_forma_pagamento TO etl_pombo;
GRANT SELECT ON TABLE gold.vendas_postos_forma_pagamento TO bi_reader;
GRANT ALL ON TABLE gold.vendas_postos_forma_pagamento TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.vendas_postos_forma_pagamento TO ti_hub;


--
-- TOC entry 5371 (class 0 OID 0)
-- Dependencies: 436
-- Name: SEQUENCE vendas_postos_id_seq; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,USAGE ON SEQUENCE gold.vendas_postos_id_seq TO etl_pombo;
GRANT SELECT,USAGE ON SEQUENCE gold.vendas_postos_id_seq TO ti_hub;


--
-- TOC entry 5372 (class 0 OID 0)
-- Dependencies: 439
-- Name: TABLE vendas_postos_itens; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.vendas_postos_itens TO etl_pombo;
GRANT SELECT ON TABLE gold.vendas_postos_itens TO bi_reader;
GRANT ALL ON TABLE gold.vendas_postos_itens TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.vendas_postos_itens TO ti_hub;


--
-- TOC entry 5373 (class 0 OID 0)
-- Dependencies: 419
-- Name: TABLE vendedor; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.vendedor TO etl_pombo;
GRANT SELECT ON TABLE gold.vendedor TO bi_reader;
GRANT ALL ON TABLE gold.vendedor TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.vendedor TO ti_hub;


--
-- TOC entry 5374 (class 0 OID 0)
-- Dependencies: 423
-- Name: TABLE z_classificacao_despesas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.z_classificacao_despesas TO etl_pombo;
GRANT SELECT ON TABLE gold.z_classificacao_despesas TO bi_reader;
GRANT ALL ON TABLE gold.z_classificacao_despesas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.z_classificacao_despesas TO ti_hub;


--
-- TOC entry 5375 (class 0 OID 0)
-- Dependencies: 416
-- Name: TABLE z_nota_fiscal_classificacao_despesas; Type: ACL; Schema: gold; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.z_nota_fiscal_classificacao_despesas TO etl_pombo;
GRANT SELECT ON TABLE gold.z_nota_fiscal_classificacao_despesas TO bi_reader;
GRANT ALL ON TABLE gold.z_nota_fiscal_classificacao_despesas TO hub_admin;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE gold.z_nota_fiscal_classificacao_despesas TO ti_hub;


--
-- TOC entry 2398 (class 826 OID 16395)
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: gold; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA gold GRANT SELECT,USAGE ON SEQUENCES TO etl_pombo;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA gold GRANT SELECT,USAGE ON SEQUENCES TO ti_hub;


--
-- TOC entry 2397 (class 826 OID 16393)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: gold; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA gold GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO etl_pombo;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA gold GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA gold GRANT ALL ON TABLES TO hub_admin;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA gold GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO ti_hub;


-- Completed on 2026-10-28 11:49:56

--
-- PostgreSQL database dump complete
--

\unrestrict BdbfaCYPEu5SwvY7n6Siz0m1kqzrnPLQYPlbX2pcDWIwq6zDEsaFyswzTkoKlfO

