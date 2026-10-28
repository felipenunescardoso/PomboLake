--
-- PostgreSQL database dump
--

\restrict 0zs9k8bBm82Sk40dQHzhhoEQjVehJHl2JxPbRMblXfS6pwVPtseoYTqd7nNkfWY

-- Dumped from database version 18.4
-- Dumped by pg_dump version 18.4

-- Started on 2026-10-28 10:55:39

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
-- TOC entry 4 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: pg_database_owner
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO pg_database_owner;

--
-- TOC entry 5236 (class 0 OID 0)
-- Dependencies: 4
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: pg_database_owner
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- TOC entry 456 (class 1255 OID 21488)
-- Name: sp_cadastrar_entidade(text, text, boolean); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.sp_cadastrar_entidade(IN p_nome_entidade text, IN p_tipo_carga text, IN p_usar_trigger boolean)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_nome_entidade TEXT;
    v_tipo_carga    TEXT;
    v_qtd_inserida  INTEGER;
BEGIN
    ------------------------------------------------------------------------
    -- NORMALIZAÇÃO DOS PARÂMETROS
    ------------------------------------------------------------------------
    v_nome_entidade := TRIM(p_nome_entidade);
    v_tipo_carga    := UPPER(TRIM(p_tipo_carga));

    ------------------------------------------------------------------------
    -- VALIDAÇÕES
    ------------------------------------------------------------------------
    IF COALESCE(v_nome_entidade, '') = '' THEN
        RAISE EXCEPTION
            'O nome da entidade não pode ser vazio.';
    END IF;

    IF v_tipo_carga NOT IN ('SNAPSHOT', 'INCREMENTAL') THEN
        RAISE EXCEPTION
            'Tipo de carga inválido: %. Informe SNAPSHOT ou INCREMENTAL.',
            p_tipo_carga;
    END IF;

    IF p_usar_trigger IS NULL THEN
        RAISE EXCEPTION
            'O parâmetro p_usar_trigger deve ser TRUE ou FALSE.';
    END IF;

    ------------------------------------------------------------------------
    -- CADASTRA A ENTIDADE EM TODAS AS BASES ODBC ATIVAS
    ------------------------------------------------------------------------
    INSERT INTO public.configuracao_entidades
    (
        id_fonte,
        nome_tabela,
        tipo_carga,
        intervalo_minutos,
        prioridade,
        tamanho_lote,
        usar_trigger,
        ativo,
        carga_inicial_concluida
    )
    SELECT
        f.id,
        v_nome_entidade,
        v_tipo_carga,
        f.intervalo_minutos,
        1,
        1000,
        p_usar_trigger,
        TRUE,
        FALSE
    FROM public.fontes AS f
    WHERE f.tipo_fonte = 'ODBC'
      AND f.ativo = TRUE

      -- Impede cadastrar a mesma entidade duas vezes na mesma base
      AND NOT EXISTS
      (
          SELECT 1
          FROM public.configuracao_entidades AS ce
          WHERE ce.id_fonte = f.id
            AND LOWER(TRIM(ce.nome_tabela)) =
                LOWER(v_nome_entidade)
      );

    GET DIAGNOSTICS v_qtd_inserida = ROW_COUNT;

    RAISE NOTICE
        'Entidade "%" cadastrada em % base(s). Tipo: %. Trigger: %.',
        v_nome_entidade,
        v_qtd_inserida,
        v_tipo_carga,
        p_usar_trigger;
END;
$$;


ALTER PROCEDURE public.sp_cadastrar_entidade(IN p_nome_entidade text, IN p_tipo_carga text, IN p_usar_trigger boolean) OWNER TO postgres;

--
-- TOC entry 444 (class 1255 OID 19719)
-- Name: sp_cadastrar_fonte(text, text, text, text, text, text, integer, text, text, text); Type: PROCEDURE; Schema: public; Owner: postgres
--

CREATE PROCEDURE public.sp_cadastrar_fonte(IN p_ip_servidor text, IN p_driver_odbc text, IN p_nome_base text, IN p_nome_server text, IN p_usuario_db text, IN p_senha_db text, IN p_porta integer, IN p_cod_empresa text, IN p_engine text, IN p_nome_alias text)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_id_configuracao INTEGER;
    v_id_fonte        INTEGER;
BEGIN

    ----------------------------------------------------------------------------
    -- CONFIGURACAO
    ----------------------------------------------------------------------------
    INSERT INTO configuracao
    (
        ip_servidor,
        driver_odbc,
        nome_base,
        nome_server,
        usuario_db,
        senha_db,
        porta,
        cod_empresa,
        ativo,
        engine,
        nome_alias, id_configuracao_inicial
    )
    VALUES
    (
        p_ip_servidor,
        p_driver_odbc,
        p_nome_base,
        p_nome_server,
        p_usuario_db,
        p_senha_db,
        p_porta,
        p_cod_empresa,
        TRUE,
        p_engine,
        p_nome_alias, 1
    )
    RETURNING id INTO v_id_configuracao;

    ----------------------------------------------------------------------------
    -- FONTES
    ----------------------------------------------------------------------------
    INSERT INTO fontes
    (
        nome_fonte,
        tipo_fonte,
        id_referencia,
        ordem_execucao,
        grupo_execucao,
        modo_carga,
        intervalo_minutos,
        carga_inicial,
        ativo
    )
    VALUES
    (
        p_nome_server,
        'ODBC',
        v_id_configuracao,
        1,
        1,
        'INCREMENTAL',
        5,
        TRUE,
        TRUE
    )
    RETURNING id INTO v_id_fonte;

    ----------------------------------------------------------------------------
    -- CONFIGURACAO_ENTIDADES
    ----------------------------------------------------------------------------
    INSERT INTO configuracao_entidades
    (
        id_fonte,
        nome_tabela,
        tipo_carga,
        intervalo_minutos,
        prioridade,
        tamanho_lote,
        usar_trigger
    )
    SELECT
        v_id_fonte,
        nome_tabela,
        tipo_carga,
        intervalo_minutos,
        prioridade,
        tamanho_lote,
        usar_trigger
    FROM configuracao_entidades
    WHERE id_fonte = 1;

END;
$$;


ALTER PROCEDURE public.sp_cadastrar_fonte(IN p_ip_servidor text, IN p_driver_odbc text, IN p_nome_base text, IN p_nome_server text, IN p_usuario_db text, IN p_senha_db text, IN p_porta integer, IN p_cod_empresa text, IN p_engine text, IN p_nome_alias text) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 420 (class 1259 OID 16401)
-- Name: configuracao; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.configuracao (
    id integer NOT NULL,
    ip_servidor character varying(20),
    driver_odbc character varying(64),
    nome_base character varying(64),
    nome_server character varying(64),
    usuario_db character varying(64),
    senha_db character varying(256),
    porta integer,
    cod_empresa character varying(256),
    ativo boolean DEFAULT true NOT NULL,
    engine character varying(32),
    nome_alias character varying(20),
    id_configuracao_inicial integer
);


ALTER TABLE public.configuracao OWNER TO postgres;

--
-- TOC entry 422 (class 1259 OID 16412)
-- Name: configuracao_api; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.configuracao_api (
    id integer NOT NULL,
    nome_api character varying(100) NOT NULL,
    descricao character varying(255),
    metodo_http character varying(10) DEFAULT 'GET'::character varying NOT NULL,
    endpoint_base character varying(500) NOT NULL,
    rota_recurso character varying(500),
    tipo_autenticacao character varying(30) DEFAULT 'NONE'::character varying NOT NULL,
    usuario_api character varying(100),
    senha_api character varying(256),
    token_api text,
    api_key text,
    api_key_header character varying(100),
    headers_json jsonb,
    parametros_json jsonb,
    timeout_segundos integer DEFAULT 30 NOT NULL,
    qtd_tentativas integer DEFAULT 3 NOT NULL,
    intervalo_retry_seg integer DEFAULT 10 NOT NULL,
    paginacao_ativa boolean DEFAULT false NOT NULL,
    tipo_paginacao character varying(30),
    parametro_pagina character varying(50),
    parametro_limite character varying(50),
    tamanho_pagina integer,
    campo_dados character varying(255),
    campo_proxima_pagina character varying(255),
    ativo boolean DEFAULT true NOT NULL,
    data_criacao timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    data_alteracao timestamp without time zone,
    id_configuracao_inicial integer,
    nome_alias character varying(32),
    harmoniza boolean DEFAULT false,
    gravadata boolean
);


ALTER TABLE public.configuracao_api OWNER TO postgres;

--
-- TOC entry 421 (class 1259 OID 16411)
-- Name: configuracao_api_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.configuracao_api ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.configuracao_api_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 428 (class 1259 OID 16520)
-- Name: configuracao_entidades; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.configuracao_entidades (
    id integer NOT NULL,
    id_fonte integer NOT NULL,
    nome_tabela character varying(128) NOT NULL,
    tipo_carga character varying(20) DEFAULT 'INCREMENTAL'::character varying NOT NULL,
    intervalo_minutos integer NOT NULL,
    prioridade integer DEFAULT 1 NOT NULL,
    tamanho_lote integer DEFAULT 1000 NOT NULL,
    usar_trigger boolean DEFAULT false NOT NULL,
    ativo boolean DEFAULT true NOT NULL,
    ultima_execucao timestamp without time zone,
    proxima_execucao timestamp without time zone,
    data_criacao timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    carga_inicial_concluida boolean DEFAULT false NOT NULL,
    data_carga_inicial timestamp without time zone,
    ultima_execucao_status character varying(20),
    tabela_hub character varying(64),
    historico_erro integer DEFAULT 0 NOT NULL,
    ultimo_parquet character varying(500),
    CONSTRAINT ck_entidade_tipo_carga CHECK (((tipo_carga)::text = ANY ((ARRAY['INCREMENTAL'::character varying, 'SNAPSHOT'::character varying, 'FULL'::character varying])::text[])))
);


ALTER TABLE public.configuracao_entidades OWNER TO postgres;

--
-- TOC entry 427 (class 1259 OID 16519)
-- Name: configuracao_entidades_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.configuracao_entidades ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.configuracao_entidades_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 419 (class 1259 OID 16400)
-- Name: configuracao_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.configuracao ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.configuracao_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 424 (class 1259 OID 16439)
-- Name: configuracao_planilhas; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.configuracao_planilhas (
    id integer NOT NULL,
    nome_configuracao character varying(100) NOT NULL,
    descricao character varying(255),
    caminho_arquivo character varying(1000) NOT NULL,
    nome_arquivo character varying(255) NOT NULL,
    abas character varying(1000) NOT NULL,
    usuario_acesso character varying(150),
    senha_acesso character varying(256),
    tipo_arquivo character varying(20) DEFAULT 'XLSX'::character varying NOT NULL,
    possui_cabecalho boolean DEFAULT true NOT NULL,
    linha_cabecalho integer DEFAULT 1 NOT NULL,
    intervalo_leitura character varying(100),
    coluna_data_referencia character varying(100),
    destino_entidade character varying(100) NOT NULL,
    modo_carga character varying(30) DEFAULT 'INCREMENTAL'::character varying NOT NULL,
    ativo boolean DEFAULT true NOT NULL,
    data_criacao timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    data_alteracao timestamp without time zone,
    id_configuracao_inicial integer
);


ALTER TABLE public.configuracao_planilhas OWNER TO postgres;

--
-- TOC entry 423 (class 1259 OID 16438)
-- Name: configuracao_planilhas_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.configuracao_planilhas ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.configuracao_planilhas_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 430 (class 1259 OID 16547)
-- Name: configuracao_pombo; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.configuracao_pombo (
    id integer NOT NULL,
    descricao character varying(100) NOT NULL,
    tipo_destino character varying(30) DEFAULT 'SMB'::character varying NOT NULL,
    caminho character varying(512) NOT NULL,
    usuario character varying(100),
    senha character varying(256),
    ativo boolean DEFAULT true NOT NULL,
    data_criacao timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    data_alteracao timestamp without time zone
);


ALTER TABLE public.configuracao_pombo OWNER TO postgres;

--
-- TOC entry 429 (class 1259 OID 16546)
-- Name: configuracao_pombo_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.configuracao_pombo ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.configuracao_pombo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 439 (class 1259 OID 77998)
-- Name: controle_api_dw; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.controle_api_dw (
    id_entidade bigint NOT NULL,
    nome_alias character varying(100) NOT NULL,
    ultimo_ingestion_ts timestamp with time zone NOT NULL,
    data_ultima_confirmacao timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    status character varying(20) DEFAULT 'SUCESSO'::character varying NOT NULL
);


ALTER TABLE public.controle_api_dw OWNER TO postgres;

--
-- TOC entry 435 (class 1259 OID 21543)
-- Name: controle_carga_dw; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.controle_carga_dw (
    id_entidade integer NOT NULL,
    id_fonte integer NOT NULL,
    nome_alias character varying(100) NOT NULL,
    nome_entidade character varying(150) NOT NULL,
    carga_inicial_concluida boolean DEFAULT false NOT NULL,
    data_carga_inicial timestamp without time zone,
    data_ultima_confirmacao timestamp without time zone,
    status character varying(20)
);


ALTER TABLE public.controle_carga_dw OWNER TO postgres;

--
-- TOC entry 434 (class 1259 OID 21530)
-- Name: controle_delta_dw; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.controle_delta_dw (
    id_fonte integer NOT NULL,
    nome_alias character varying(100) NOT NULL,
    nome_entidade character varying(100) NOT NULL,
    ultimo_batch_enviado bigint,
    data_ultima_carga timestamp without time zone,
    status_ultima_carga character varying(20)
);


ALTER TABLE public.controle_delta_dw OWNER TO postgres;

--
-- TOC entry 436 (class 1259 OID 21565)
-- Name: controle_evento_dw; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.controle_evento_dw (
    id_entidade integer NOT NULL,
    id_fonte integer NOT NULL,
    nome_alias character varying(100) NOT NULL,
    nome_entidade character varying(150) NOT NULL,
    tabela_evento character varying(50) NOT NULL,
    ultimo_event_id_confirmado bigint,
    ultimo_batch_id character varying(100),
    data_ultima_confirmacao timestamp without time zone,
    status character varying(20)
);


ALTER TABLE public.controle_evento_dw OWNER TO postgres;

--
-- TOC entry 441 (class 1259 OID 93374)
-- Name: de_para_entidade; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.de_para_entidade (
    id_de_para integer NOT NULL,
    id_entidade integer NOT NULL,
    nome_tabela character varying(100) NOT NULL,
    campo_origem character varying(150) NOT NULL,
    campo_destino character varying(150) NOT NULL,
    ativo boolean DEFAULT true NOT NULL,
    valor_condicional jsonb
);


ALTER TABLE public.de_para_entidade OWNER TO postgres;

--
-- TOC entry 440 (class 1259 OID 93373)
-- Name: de_para_entidade_id_de_para_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.de_para_entidade ALTER COLUMN id_de_para ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.de_para_entidade_id_de_para_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 426 (class 1259 OID 16494)
-- Name: fontes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fontes (
    id integer NOT NULL,
    nome_fonte character varying(100) NOT NULL,
    tipo_fonte character varying(20) NOT NULL,
    id_referencia integer NOT NULL,
    ordem_execucao integer DEFAULT 1 NOT NULL,
    grupo_execucao integer DEFAULT 1 NOT NULL,
    modo_carga character varying(20) DEFAULT 'INCREMENTAL'::character varying NOT NULL,
    intervalo_minutos integer DEFAULT 5 NOT NULL,
    carga_inicial boolean DEFAULT true NOT NULL,
    ativo boolean DEFAULT true NOT NULL,
    data_criacao timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    data_alteracao timestamp without time zone,
    CONSTRAINT ck_fontes_modo_carga CHECK (((modo_carga)::text = ANY ((ARRAY['FULL'::character varying, 'INCREMENTAL'::character varying])::text[]))),
    CONSTRAINT ck_fontes_tipo CHECK (((tipo_fonte)::text = ANY ((ARRAY['ODBC'::character varying, 'API'::character varying, 'PLANILHA'::character varying])::text[])))
);


ALTER TABLE public.fontes OWNER TO postgres;

--
-- TOC entry 425 (class 1259 OID 16493)
-- Name: fontes_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.fontes ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.fontes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 432 (class 1259 OID 19912)
-- Name: logs_delta; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.logs_delta (
    id bigint NOT NULL,
    pombo_id bigint NOT NULL,
    nome_acao character varying(30) NOT NULL,
    data_inicio timestamp without time zone NOT NULL,
    data_fim timestamp without time zone,
    log text
);


ALTER TABLE public.logs_delta OWNER TO postgres;

--
-- TOC entry 431 (class 1259 OID 19911)
-- Name: logs_delta_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.logs_delta_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.logs_delta_id_seq OWNER TO postgres;

--
-- TOC entry 5256 (class 0 OID 0)
-- Dependencies: 431
-- Name: logs_delta_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.logs_delta_id_seq OWNED BY public.logs_delta.id;


--
-- TOC entry 438 (class 1259 OID 25429)
-- Name: pombo_usuarios; Type: TABLE; Schema: public; Owner: central_pombo
--

CREATE TABLE public.pombo_usuarios (
    id bigint NOT NULL,
    nome character varying(150) NOT NULL,
    login character varying(100) NOT NULL,
    email character varying(254),
    senha_hash text NOT NULL,
    perfil character varying(30) NOT NULL,
    ativo boolean DEFAULT true NOT NULL,
    data_criacao timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    data_alteracao timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    ultimo_login timestamp with time zone,
    senha_alterada_em timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_pombo_usuarios_perfil CHECK (((perfil)::text = ANY ((ARRAY['ADMIN'::character varying, 'OPERADOR'::character varying, 'LEITURA'::character varying])::text[])))
);


ALTER TABLE public.pombo_usuarios OWNER TO central_pombo;

--
-- TOC entry 437 (class 1259 OID 25428)
-- Name: pombo_usuarios_id_seq; Type: SEQUENCE; Schema: public; Owner: central_pombo
--

ALTER TABLE public.pombo_usuarios ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.pombo_usuarios_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 443 (class 1259 OID 99740)
-- Name: range_datas_apis; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.range_datas_apis (
    id integer NOT NULL,
    id_api integer NOT NULL,
    configuracao integer NOT NULL,
    ativo boolean DEFAULT true NOT NULL,
    data_criacao timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    data_alteracao timestamp without time zone
);


ALTER TABLE public.range_datas_apis OWNER TO postgres;

--
-- TOC entry 5260 (class 0 OID 0)
-- Dependencies: 443
-- Name: TABLE range_datas_apis; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.range_datas_apis IS 'Granularidade da janela da carga inicial por API (extrator API).';


--
-- TOC entry 5261 (class 0 OID 0)
-- Dependencies: 443
-- Name: COLUMN range_datas_apis.configuracao; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.range_datas_apis.configuracao IS '1=Diaria, 2=Semanal, 3=Mensal, 4=Semestral, 5=Anual (padrao quando sem linha).';


--
-- TOC entry 442 (class 1259 OID 99739)
-- Name: range_datas_apis_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.range_datas_apis ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.range_datas_apis_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 433 (class 1259 OID 20596)
-- Name: tipogold; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.tipogold (
    tipo boolean DEFAULT false
);


ALTER TABLE public.tipogold OWNER TO postgres;

--
-- TOC entry 5028 (class 2604 OID 19915)
-- Name: logs_delta id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.logs_delta ALTER COLUMN id SET DEFAULT nextval('public.logs_delta_id_seq'::regclass);


--
-- TOC entry 5047 (class 2606 OID 16437)
-- Name: configuracao_api configuracao_api_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.configuracao_api
    ADD CONSTRAINT configuracao_api_pkey PRIMARY KEY (id);


--
-- TOC entry 5053 (class 2606 OID 16541)
-- Name: configuracao_entidades configuracao_entidades_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.configuracao_entidades
    ADD CONSTRAINT configuracao_entidades_pkey PRIMARY KEY (id);


--
-- TOC entry 5045 (class 2606 OID 16410)
-- Name: configuracao configuracao_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.configuracao
    ADD CONSTRAINT configuracao_pkey PRIMARY KEY (id);


--
-- TOC entry 5049 (class 2606 OID 16463)
-- Name: configuracao_planilhas configuracao_planilhas_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.configuracao_planilhas
    ADD CONSTRAINT configuracao_planilhas_pkey PRIMARY KEY (id);


--
-- TOC entry 5057 (class 2606 OID 16562)
-- Name: configuracao_pombo configuracao_pombo_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.configuracao_pombo
    ADD CONSTRAINT configuracao_pombo_pkey PRIMARY KEY (id);


--
-- TOC entry 5074 (class 2606 OID 78009)
-- Name: controle_api_dw controle_api_dw_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_api_dw
    ADD CONSTRAINT controle_api_dw_pkey PRIMARY KEY (id_entidade, nome_alias);


--
-- TOC entry 5063 (class 2606 OID 21553)
-- Name: controle_carga_dw controle_carga_dw_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_carga_dw
    ADD CONSTRAINT controle_carga_dw_pkey PRIMARY KEY (id_entidade);


--
-- TOC entry 5076 (class 2606 OID 93385)
-- Name: de_para_entidade de_para_entidade_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.de_para_entidade
    ADD CONSTRAINT de_para_entidade_pkey PRIMARY KEY (id_de_para);


--
-- TOC entry 5051 (class 2606 OID 16518)
-- Name: fontes fontes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fontes
    ADD CONSTRAINT fontes_pkey PRIMARY KEY (id);


--
-- TOC entry 5059 (class 2606 OID 19923)
-- Name: logs_delta logs_delta_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.logs_delta
    ADD CONSTRAINT logs_delta_pkey PRIMARY KEY (id);


--
-- TOC entry 5061 (class 2606 OID 21537)
-- Name: controle_delta_dw pk_controle_delta_dw; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_delta_dw
    ADD CONSTRAINT pk_controle_delta_dw PRIMARY KEY (id_fonte, nome_entidade);


--
-- TOC entry 5068 (class 2606 OID 21574)
-- Name: controle_evento_dw pk_controle_evento_dw; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_evento_dw
    ADD CONSTRAINT pk_controle_evento_dw PRIMARY KEY (id_entidade, tabela_evento);


--
-- TOC entry 5070 (class 2606 OID 25449)
-- Name: pombo_usuarios pombo_usuarios_pkey; Type: CONSTRAINT; Schema: public; Owner: central_pombo
--

ALTER TABLE ONLY public.pombo_usuarios
    ADD CONSTRAINT pombo_usuarios_pkey PRIMARY KEY (id);


--
-- TOC entry 5078 (class 2606 OID 99751)
-- Name: range_datas_apis range_datas_apis_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.range_datas_apis
    ADD CONSTRAINT range_datas_apis_pkey PRIMARY KEY (id);


--
-- TOC entry 5055 (class 2606 OID 16543)
-- Name: configuracao_entidades uq_entidade_fonte; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.configuracao_entidades
    ADD CONSTRAINT uq_entidade_fonte UNIQUE (id_fonte, nome_tabela);


--
-- TOC entry 5064 (class 1259 OID 21564)
-- Name: idx_controle_carga_fonte; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_controle_carga_fonte ON public.controle_carga_dw USING btree (id_fonte);


--
-- TOC entry 5065 (class 1259 OID 21586)
-- Name: idx_controle_evento_evento; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_controle_evento_evento ON public.controle_evento_dw USING btree (id_entidade, tabela_evento, ultimo_event_id_confirmado);


--
-- TOC entry 5066 (class 1259 OID 21585)
-- Name: idx_controle_evento_fonte; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_controle_evento_fonte ON public.controle_evento_dw USING btree (id_fonte);


--
-- TOC entry 5071 (class 1259 OID 25451)
-- Name: uq_pombo_usuarios_email_lower; Type: INDEX; Schema: public; Owner: central_pombo
--

CREATE UNIQUE INDEX uq_pombo_usuarios_email_lower ON public.pombo_usuarios USING btree (lower((email)::text)) WHERE (email IS NOT NULL);


--
-- TOC entry 5072 (class 1259 OID 25450)
-- Name: uq_pombo_usuarios_login_lower; Type: INDEX; Schema: public; Owner: central_pombo
--

CREATE UNIQUE INDEX uq_pombo_usuarios_login_lower ON public.pombo_usuarios USING btree (lower((login)::text));


--
-- TOC entry 5080 (class 2606 OID 21554)
-- Name: controle_carga_dw fk_controle_carga_entidade; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_carga_dw
    ADD CONSTRAINT fk_controle_carga_entidade FOREIGN KEY (id_entidade) REFERENCES public.configuracao_entidades(id);


--
-- TOC entry 5081 (class 2606 OID 21559)
-- Name: controle_carga_dw fk_controle_carga_fonte; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_carga_dw
    ADD CONSTRAINT fk_controle_carga_fonte FOREIGN KEY (id_fonte) REFERENCES public.fontes(id);


--
-- TOC entry 5079 (class 2606 OID 21538)
-- Name: controle_delta_dw fk_controle_delta_dw_fonte; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_delta_dw
    ADD CONSTRAINT fk_controle_delta_dw_fonte FOREIGN KEY (id_fonte) REFERENCES public.fontes(id);


--
-- TOC entry 5082 (class 2606 OID 21575)
-- Name: controle_evento_dw fk_controle_evento_entidade; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_evento_dw
    ADD CONSTRAINT fk_controle_evento_entidade FOREIGN KEY (id_entidade) REFERENCES public.configuracao_entidades(id);


--
-- TOC entry 5083 (class 2606 OID 21580)
-- Name: controle_evento_dw fk_controle_evento_fonte; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.controle_evento_dw
    ADD CONSTRAINT fk_controle_evento_fonte FOREIGN KEY (id_fonte) REFERENCES public.fontes(id);


--
-- TOC entry 5237 (class 0 OID 0)
-- Dependencies: 420
-- Name: TABLE configuracao; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.configuracao TO central_pombo;
GRANT ALL ON TABLE public.configuracao TO etl_pombo;


--
-- TOC entry 5238 (class 0 OID 0)
-- Dependencies: 422
-- Name: TABLE configuracao_api; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.configuracao_api TO central_pombo;
GRANT ALL ON TABLE public.configuracao_api TO etl_pombo;


--
-- TOC entry 5239 (class 0 OID 0)
-- Dependencies: 421
-- Name: SEQUENCE configuracao_api_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.configuracao_api_id_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.configuracao_api_id_seq TO central_pombo;


--
-- TOC entry 5240 (class 0 OID 0)
-- Dependencies: 428
-- Name: TABLE configuracao_entidades; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.configuracao_entidades TO central_pombo;
GRANT ALL ON TABLE public.configuracao_entidades TO etl_pombo;


--
-- TOC entry 5241 (class 0 OID 0)
-- Dependencies: 427
-- Name: SEQUENCE configuracao_entidades_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.configuracao_entidades_id_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.configuracao_entidades_id_seq TO central_pombo;


--
-- TOC entry 5242 (class 0 OID 0)
-- Dependencies: 419
-- Name: SEQUENCE configuracao_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.configuracao_id_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.configuracao_id_seq TO central_pombo;


--
-- TOC entry 5243 (class 0 OID 0)
-- Dependencies: 424
-- Name: TABLE configuracao_planilhas; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.configuracao_planilhas TO central_pombo;
GRANT ALL ON TABLE public.configuracao_planilhas TO etl_pombo;


--
-- TOC entry 5244 (class 0 OID 0)
-- Dependencies: 423
-- Name: SEQUENCE configuracao_planilhas_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.configuracao_planilhas_id_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.configuracao_planilhas_id_seq TO central_pombo;


--
-- TOC entry 5245 (class 0 OID 0)
-- Dependencies: 430
-- Name: TABLE configuracao_pombo; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.configuracao_pombo TO central_pombo;
GRANT ALL ON TABLE public.configuracao_pombo TO etl_pombo;


--
-- TOC entry 5246 (class 0 OID 0)
-- Dependencies: 429
-- Name: SEQUENCE configuracao_pombo_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.configuracao_pombo_id_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.configuracao_pombo_id_seq TO central_pombo;


--
-- TOC entry 5247 (class 0 OID 0)
-- Dependencies: 439
-- Name: TABLE controle_api_dw; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.controle_api_dw TO etl_pombo;
GRANT ALL ON TABLE public.controle_api_dw TO central_pombo;


--
-- TOC entry 5248 (class 0 OID 0)
-- Dependencies: 435
-- Name: TABLE controle_carga_dw; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.controle_carga_dw TO etl_pombo;
GRANT ALL ON TABLE public.controle_carga_dw TO central_pombo;


--
-- TOC entry 5249 (class 0 OID 0)
-- Dependencies: 434
-- Name: TABLE controle_delta_dw; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.controle_delta_dw TO etl_pombo;
GRANT ALL ON TABLE public.controle_delta_dw TO central_pombo;


--
-- TOC entry 5250 (class 0 OID 0)
-- Dependencies: 436
-- Name: TABLE controle_evento_dw; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.controle_evento_dw TO etl_pombo;
GRANT ALL ON TABLE public.controle_evento_dw TO central_pombo;


--
-- TOC entry 5251 (class 0 OID 0)
-- Dependencies: 441
-- Name: TABLE de_para_entidade; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.de_para_entidade TO etl_pombo;
GRANT ALL ON TABLE public.de_para_entidade TO central_pombo;


--
-- TOC entry 5252 (class 0 OID 0)
-- Dependencies: 440
-- Name: SEQUENCE de_para_entidade_id_de_para_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.de_para_entidade_id_de_para_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.de_para_entidade_id_de_para_seq TO central_pombo;


--
-- TOC entry 5253 (class 0 OID 0)
-- Dependencies: 426
-- Name: TABLE fontes; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.fontes TO central_pombo;
GRANT ALL ON TABLE public.fontes TO etl_pombo;


--
-- TOC entry 5254 (class 0 OID 0)
-- Dependencies: 425
-- Name: SEQUENCE fontes_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.fontes_id_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.fontes_id_seq TO central_pombo;


--
-- TOC entry 5255 (class 0 OID 0)
-- Dependencies: 432
-- Name: TABLE logs_delta; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.logs_delta TO etl_pombo;
GRANT ALL ON TABLE public.logs_delta TO central_pombo;


--
-- TOC entry 5257 (class 0 OID 0)
-- Dependencies: 431
-- Name: SEQUENCE logs_delta_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.logs_delta_id_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.logs_delta_id_seq TO central_pombo;


--
-- TOC entry 5258 (class 0 OID 0)
-- Dependencies: 438
-- Name: TABLE pombo_usuarios; Type: ACL; Schema: public; Owner: central_pombo
--

GRANT ALL ON TABLE public.pombo_usuarios TO etl_pombo;


--
-- TOC entry 5259 (class 0 OID 0)
-- Dependencies: 437
-- Name: SEQUENCE pombo_usuarios_id_seq; Type: ACL; Schema: public; Owner: central_pombo
--

GRANT ALL ON SEQUENCE public.pombo_usuarios_id_seq TO etl_pombo;


--
-- TOC entry 5262 (class 0 OID 0)
-- Dependencies: 443
-- Name: TABLE range_datas_apis; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.range_datas_apis TO etl_pombo;
GRANT ALL ON TABLE public.range_datas_apis TO central_pombo;


--
-- TOC entry 5263 (class 0 OID 0)
-- Dependencies: 442
-- Name: SEQUENCE range_datas_apis_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.range_datas_apis_id_seq TO etl_pombo;
GRANT ALL ON SEQUENCE public.range_datas_apis_id_seq TO central_pombo;


--
-- TOC entry 5264 (class 0 OID 0)
-- Dependencies: 433
-- Name: TABLE tipogold; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.tipogold TO etl_pombo;
GRANT ALL ON TABLE public.tipogold TO central_pombo;


--
-- TOC entry 2320 (class 826 OID 16563)
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO etl_pombo;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO central_pombo;


--
-- TOC entry 2319 (class 826 OID 16467)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO etl_pombo;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO central_pombo;


-- Completed on 2026-10-28 10:55:39

--
-- PostgreSQL database dump complete
--

\unrestrict 0zs9k8bBm82Sk40dQHzhhoEQjVehJHl2JxPbRMblXfS6pwVPtseoYTqd7nNkfWY

