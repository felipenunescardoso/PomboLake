

CREATE TABLE if not exists "DBA"."z_datalake" (
	"ID" INTEGER NOT NULL DEFAULT AUTOINCREMENT,
	"NomeTabela" VARCHAR(50) NOT NULL,
	"Chave" VARCHAR(12) NOT NULL,
	"CodEmpresa" INTEGER NOT NULL,
	"Sincronizado" BIT NOT NULL DEFAULT 0,
	"Acao" VARCHAR(12) NULL,
	"ColunaChave1" VARCHAR(12) NULL,
	"ColunaChave2" VARCHAR(12) NULL,
	"DataLog" "datetime" NOT NULL DEFAULT CURRENT TIMESTAMP,
	PRIMARY KEY ( "ID" ASC )
) IN "system";

CREATE TABLE if not exists "DBA"."z_datalake_itens" (
	"ID" INTEGER NOT NULL DEFAULT AUTOINCREMENT,
	"NomeTabela" VARCHAR(50) NOT NULL,
	"Chave" VARCHAR(12) NOT NULL,
	"Item" INTEGER NOT NULL,
	"CodEmpresa" INTEGER NOT NULL,
	"Sincronizado" BIT NOT NULL DEFAULT 0,
	"Acao" VARCHAR(12) NULL,
	"ColunaChave1" VARCHAR(12) NULL,
	"ColunaChave2" VARCHAR(12) NULL,
	"ColunaChave3" VARCHAR(12) NULL,
	"DataLog" "datetime" NOT NULL DEFAULT CURRENT TIMESTAMP,
	PRIMARY KEY ( "ID" ASC )
) IN "system";

-- 1. CONTAS PAGAR RECEBER


CREATE OR REPLACE TRIGGER z_lake_cpr_insert
AFTER INSERT
ORDER 1000
ON ContasPagarReceber
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('ContasPagarReceber', STRING(Novo.NumLancamento), Novo.CodEmpresa, 'INSERT', 'CodEmpresa', 'NumLancamento');
END;

CREATE OR REPLACE TRIGGER z_lake_cpr_update
AFTER UPDATE
ORDER 1000
ON ContasPagarReceber
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('ContasPagarReceber', STRING(Novo.NumLancamento), Novo.CodEmpresa, 'UPDATE', 'CodEmpresa', 'NumLancamento');
END;

CREATE OR REPLACE TRIGGER z_lake_cpr_delete
BEFORE DELETE
ORDER 1000
ON ContasPagarReceber
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('ContasPagarReceber', STRING(Antigo.NumLancamento), Antigo.CodEmpresa, 'DELETE', 'CodEmpresa', 'NumLancamento');
END;

-- 2. CONTAS PAGAR RECEBER MOEDA
CREATE OR REPLACE TRIGGER z_lake_cpr_moeda_insert
AFTER INSERT
ORDER 1000
ON ContasPagarReceberMoeda
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('ContasPagarReceberMoeda', STRING(Novo.NumLancamento), Novo.CodEmpresa, 'INSERT', 'CodEmpresa', 'NumLancamento');
END;

CREATE OR REPLACE TRIGGER z_lake_cpr_moeda_update
AFTER UPDATE
ORDER 1000
ON ContasPagarReceberMoeda
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('ContasPagarReceberMoeda', STRING(Novo.NumLancamento), Novo.CodEmpresa, 'UPDATE', 'CodEmpresa', 'NumLancamento');
END;

CREATE OR REPLACE TRIGGER z_lake_cpr_moeda_delete
BEFORE DELETE
ORDER 1000
ON ContasPagarReceberMoeda
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('ContasPagarReceberMoeda', STRING(Antigo.NumLancamento), Antigo.CodEmpresa, 'DELETE', 'CodEmpresa', 'NumLancamento');
END;

-- 3. NOTA FISCAL
CREATE OR REPLACE TRIGGER z_lake_nf_insert
AFTER INSERT
ORDER 1000
ON NotaFiscal
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('NotaFiscal', STRING(Novo.Sequencial), Novo.CodEmpresa, 'INSERT', 'CodEmpresa', 'Sequencial');
END;

CREATE OR REPLACE TRIGGER z_lake_nf_update
AFTER UPDATE
ORDER 1000
ON NotaFiscal
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('NotaFiscal', STRING(Novo.Sequencial), Novo.CodEmpresa, 'UPDATE', 'CodEmpresa', 'Sequencial');
END;

CREATE OR REPLACE TRIGGER z_lake_nf_delete
BEFORE DELETE
ORDER 1000
ON NotaFiscal
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('NotaFiscal', STRING(Antigo.Sequencial), Antigo.CodEmpresa, 'DELETE', 'CodEmpresa', 'Sequencial');
END;

-- 4. ITENS NOTA FISCAL
CREATE OR REPLACE TRIGGER z_lake_itens_nf_insert
AFTER INSERT
ORDER 1000
ON ItensNF
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens (NomeTabela, Chave, Item, CodEmpresa, Acao, ColunaChave1, ColunaChave2, ColunaChave3)
    VALUES ('ItensNF', STRING(Novo.Sequencial), Novo.NumItem, Novo.CodEmpresa, 'INSERT', 'CodEmpresa', 'Sequencial', 'NumItem');
END;

CREATE OR REPLACE TRIGGER z_lake_itens_nf_update
AFTER UPDATE
ORDER 1000
ON ItensNF
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens (NomeTabela, Chave, Item, CodEmpresa, Acao, ColunaChave1, ColunaChave2, ColunaChave3)
    VALUES ('ItensNF', STRING(Novo.Sequencial), Novo.NumItem, Novo.CodEmpresa, 'UPDATE', 'CodEmpresa', 'Sequencial', 'NumItem');
END;

CREATE OR REPLACE TRIGGER z_lake_itens_nf_delete
BEFORE DELETE
ORDER 1000
ON ItensNF
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens (NomeTabela, Chave, Item, CodEmpresa, Acao, ColunaChave1, ColunaChave2, ColunaChave3)
    VALUES ('ItensNF', STRING(Antigo.Sequencial), Antigo.NumItem, Antigo.CodEmpresa, 'DELETE', 'CodEmpresa', 'Sequencial', 'NumItem');
END;

-- 5. MOVIMENTO PLANO CONTAS
CREATE OR REPLACE TRIGGER z_lake_mov_plano_insert
AFTER INSERT
ORDER 1000
ON MovimentoPlanoContas
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('MovimentoPlanoContas', STRING(Novo.NumLote), Novo.CodEmpresa, 'INSERT', 'CodEmpresa', 'NumLote');
END;

CREATE OR REPLACE TRIGGER z_lake_mov_plano_update
AFTER UPDATE
ORDER 1000
ON MovimentoPlanoContas
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('MovimentoPlanoContas', STRING(Novo.NumLote), Novo.CodEmpresa, 'UPDATE', 'CodEmpresa', 'NumLote');
END;

CREATE OR REPLACE TRIGGER z_lake_mov_plano_delete
BEFORE DELETE
ORDER 1000
ON MovimentoPlanoContas
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('MovimentoPlanoContas', STRING(Antigo.NumLote), Antigo.CodEmpresa, 'DELETE', 'CodEmpresa', 'NumLote');
END;

-- 6. CENTRO CUSTO RATEIO
CREATE OR REPLACE TRIGGER z_lake_cc_rateio_insert
AFTER INSERT
ORDER 1000
ON CentroCustoRateio
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('CentroCustoRateio', STRING(Novo.NumLote), Novo.CodEmpresa, 'INSERT', 'CodEmpresa', 'NumLote');
END;

CREATE OR REPLACE TRIGGER z_lake_cc_rateio_update
AFTER UPDATE
ORDER 1000
ON CentroCustoRateio
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('CentroCustoRateio', STRING(Novo.NumLote), Novo.CodEmpresa, 'UPDATE', 'CodEmpresa', 'NumLote');
END;

CREATE OR REPLACE TRIGGER z_lake_cc_rateio_delete
BEFORE DELETE
ORDER 1000
ON CentroCustoRateio
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('CentroCustoRateio', STRING(Antigo.NumLote), Antigo.CodEmpresa, 'DELETE', 'CodEmpresa', 'NumLote');
END;

-- 7. CENTRO CUSTO PAGAR
CREATE OR REPLACE TRIGGER z_lake_cc_pagar_insert
AFTER INSERT
ORDER 1000
ON CentroCustoPagar
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens (NomeTabela, Chave, Item, CodEmpresa, Acao, ColunaChave1, ColunaChave2, ColunaChave3)
    VALUES ('CentroCustoPagar', STRING(Novo.NumLancamento), Novo.SeqCC, Novo.CodEmpresa, 'INSERT', 'CodEmpresa', 'NumLancamento', 'SeqCC');
END;

CREATE OR REPLACE TRIGGER z_lake_cc_pagar_update
AFTER UPDATE
ORDER 1000
ON CentroCustoPagar
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens (NomeTabela, Chave, Item, CodEmpresa, Acao, ColunaChave1, ColunaChave2, ColunaChave3)
    VALUES ('CentroCustoPagar', STRING(Novo.NumLancamento), Novo.SeqCC, Novo.CodEmpresa, 'UPDATE', 'CodEmpresa', 'NumLancamento', 'SeqCC');
END;

CREATE OR REPLACE TRIGGER z_lake_cc_pagar_delete
BEFORE DELETE
ORDER 1000
ON CentroCustoPagar
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens (NomeTabela, Chave, Item, CodEmpresa, Acao, ColunaChave1, ColunaChave2, ColunaChave3)
    VALUES ('CentroCustoPagar', STRING(Antigo.NumLancamento), Antigo.SeqCC, Antigo.CodEmpresa, 'DELETE', 'CodEmpresa', 'NumLancamento', 'SeqCC');
END;

-- 8. CLASSIFICACAO DE DESPESAS DA NOTA FISCAL
CREATE OR REPLACE TRIGGER z_lake_nf_class_desp_insert
AFTER INSERT
ORDER 1000
ON Z_NotaFiscal_ClassificacaoDespesas
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('Z_NotaFiscal_ClassificacaoDespesas', STRING(Novo.SequencialNF), Novo.CodEmpresa, 'INSERT', 'CodEmpresa', 'SequencialNF');
END;

CREATE OR REPLACE TRIGGER z_lake_nf_class_desp_update
AFTER UPDATE
ORDER 1000
ON Z_NotaFiscal_ClassificacaoDespesas
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('Z_NotaFiscal_ClassificacaoDespesas', STRING(Novo.SequencialNF), Novo.CodEmpresa, 'UPDATE', 'CodEmpresa', 'SequencialNF');
END;

CREATE OR REPLACE TRIGGER z_lake_nf_class_desp_delete
BEFORE DELETE
ORDER 1000
ON Z_NotaFiscal_ClassificacaoDespesas
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake (NomeTabela, Chave, CodEmpresa, Acao, ColunaChave1, ColunaChave2)
    VALUES ('Z_NotaFiscal_ClassificacaoDespesas', STRING(Antigo.SequencialNF), Antigo.CodEmpresa, 'DELETE', 'CodEmpresa', 'SequencialNF');
END;

COMMIT;

-- ============================================================
-- 9. PEDIDO
-- Chave: CodEmpresa + NumPed
-- ============================================================

CREATE OR REPLACE TRIGGER z_lake_pedido_insert
AFTER INSERT
ORDER 1000
ON Pedido
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'Pedido',
        STRING(Novo.NumPed),
        Novo.CodEmpresa,
        'INSERT',
        'CodEmpresa',
        'NumPed'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_pedido_update
AFTER UPDATE
ORDER 1000
ON Pedido
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'Pedido',
        STRING(Novo.NumPed),
        Novo.CodEmpresa,
        'UPDATE',
        'CodEmpresa',
        'NumPed'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_pedido_delete
BEFORE DELETE
ORDER 1000
ON Pedido
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'Pedido',
        STRING(Antigo.NumPed),
        Antigo.CodEmpresa,
        'DELETE',
        'CodEmpresa',
        'NumPed'
    );
END;


-- ============================================================
-- 10. ITENS PEDIDO
-- Chave: CodEmpresa + NumPed + Seq
-- ============================================================

CREATE OR REPLACE TRIGGER z_lake_itens_pedido_insert
AFTER INSERT
ORDER 1000
ON ItensPedido
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens
    (
        NomeTabela,
        Chave,
        Item,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2,
        ColunaChave3
    )
    VALUES
    (
        'ItensPedido',
        STRING(Novo.NumPed),
        Novo.Seq,
        Novo.CodEmpresa,
        'INSERT',
        'CodEmpresa',
        'NumPed',
        'Seq'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_itens_pedido_update
AFTER UPDATE
ORDER 1000
ON ItensPedido
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens
    (
        NomeTabela,
        Chave,
        Item,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2,
        ColunaChave3
    )
    VALUES
    (
        'ItensPedido',
        STRING(Novo.NumPed),
        Novo.Seq,
        Novo.CodEmpresa,
        'UPDATE',
        'CodEmpresa',
        'NumPed',
        'Seq'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_itens_pedido_delete
BEFORE DELETE
ORDER 1000
ON ItensPedido
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens
    (
        NomeTabela,
        Chave,
        Item,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2,
        ColunaChave3
    )
    VALUES
    (
        'ItensPedido',
        STRING(Antigo.NumPed),
        Antigo.Seq,
        Antigo.CodEmpresa,
        'DELETE',
        'CodEmpresa',
        'NumPed',
        'Seq'
    );
END;


-- ============================================================
-- 11. BAIXA
-- Chave: CodEmpresa + NumBaixa
-- ============================================================

CREATE OR REPLACE TRIGGER z_lake_baixa_insert
AFTER INSERT
ORDER 1000
ON Baixa
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'Baixa',
        STRING(Novo.NumBaixa),
        Novo.CodEmpresa,
        'INSERT',
        'CodEmpresa',
        'NumBaixa'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_baixa_update
AFTER UPDATE
ORDER 1000
ON Baixa
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'Baixa',
        STRING(Novo.NumBaixa),
        Novo.CodEmpresa,
        'UPDATE',
        'CodEmpresa',
        'NumBaixa'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_baixa_delete
BEFORE DELETE
ORDER 1000
ON Baixa
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'Baixa',
        STRING(Antigo.NumBaixa),
        Antigo.CodEmpresa,
        'DELETE',
        'CodEmpresa',
        'NumBaixa'
    );
END;


-- ============================================================
-- 12. ITENS BAIXA
-- Chave: CodEmpresa + NumBaixa + Item
-- ============================================================

CREATE OR REPLACE TRIGGER z_lake_itens_baixa_insert
AFTER INSERT
ORDER 1000
ON ItensBaixa
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens
    (
        NomeTabela,
        Chave,
        Item,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2,
        ColunaChave3
    )
    VALUES
    (
        'ItensBaixa',
        STRING(Novo.NumBaixa),
        Novo.Item,
        Novo.CodEmpresa,
        'INSERT',
        'CodEmpresa',
        'NumBaixa',
        'Item'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_itens_baixa_update
AFTER UPDATE
ORDER 1000
ON ItensBaixa
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens
    (
        NomeTabela,
        Chave,
        Item,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2,
        ColunaChave3
    )
    VALUES
    (
        'ItensBaixa',
        STRING(Novo.NumBaixa),
        Novo.Item,
        Novo.CodEmpresa,
        'UPDATE',
        'CodEmpresa',
        'NumBaixa',
        'Item'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_itens_baixa_delete
BEFORE DELETE
ORDER 1000
ON ItensBaixa
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake_itens
    (
        NomeTabela,
        Chave,
        Item,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2,
        ColunaChave3
    )
    VALUES
    (
        'ItensBaixa',
        STRING(Antigo.NumBaixa),
        Antigo.Item,
        Antigo.CodEmpresa,
        'DELETE',
        'CodEmpresa',
        'NumBaixa',
        'Item'
    );
END;


-- ============================================================
-- 13. BAIXA ANTECIPADA
-- Chave: CodEmpresa + SeqCredito
-- ============================================================

CREATE OR REPLACE TRIGGER z_lake_baixa_antecipada_insert
AFTER INSERT
ORDER 1000
ON BaixaAntecipada
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'BaixaAntecipada',
        STRING(Novo.SeqCredito),
        Novo.CodEmpresa,
        'INSERT',
        'CodEmpresa',
        'SeqCredito'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_baixa_antecipada_update
AFTER UPDATE
ORDER 1000
ON BaixaAntecipada
REFERENCING NEW AS Novo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'BaixaAntecipada',
        STRING(Novo.SeqCredito),
        Novo.CodEmpresa,
        'UPDATE',
        'CodEmpresa',
        'SeqCredito'
    );
END;


CREATE OR REPLACE TRIGGER z_lake_baixa_antecipada_delete
BEFORE DELETE
ORDER 1000
ON BaixaAntecipada
REFERENCING OLD AS Antigo
FOR EACH ROW
BEGIN
    INSERT INTO z_datalake
    (
        NomeTabela,
        Chave,
        CodEmpresa,
        Acao,
        ColunaChave1,
        ColunaChave2
    )
    VALUES
    (
        'BaixaAntecipada',
        STRING(Antigo.SeqCredito),
        Antigo.CodEmpresa,
        'DELETE',
        'CodEmpresa',
        'SeqCredito'
    );
END;


