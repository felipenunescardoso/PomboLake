-- ============================================================
-- POMBO DATA LAKE - SILVER DELTA
-- Lê os arquivos de Silver/Current e grava em Silver/Delta
-- somente os registros pertencentes ao maior _batch_id
-- encontrado em cada entidade.
--
-- A coluna _operation é preservada para INSERT, UPDATE e DELETE.
-- Na primeira carga, se todos os registros pertencerem ao mesmo
-- batch, todos serão gravados no Delta.
-- ============================================================

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/contaspagarreceber.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/contaspagarreceber.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/contaspagarreceberMoeda.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/contaspagarreceberMoeda.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/notafiscal.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/notafiscal.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/itensnf.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/itensnf.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/MovimentoPlanoContas.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/MovimentoPlanoContas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/CentroCustoRateio.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/CentroCustoRateio.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/CentroCustoPagar.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/CentroCustoPagar.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Z_NotaFiscal_ClassificacaoDespesas.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Z_NotaFiscal_ClassificacaoDespesas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/empresas.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/empresas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Entidade.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Entidade.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Vendedor.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Vendedor.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/TipoDocumento.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/TipoDocumento.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/NaturezaOperacao.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/NaturezaOperacao.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/z_classificacaodespesas.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/z_classificacaodespesas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/planocontas.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/planocontas.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Portador.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Portador.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/CentroCusto.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/CentroCusto.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Pedido.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Pedido.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/ItensPedido.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/ItensPedido.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Cidade.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Cidade.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Bandeira.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Bandeira.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/BaixaAntecipada.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/BaixaAntecipada.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/ItensBaixa.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/ItensBaixa.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Baixa.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Baixa.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Produto.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Produto.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);

COPY (
    WITH current_entidade AS (
        SELECT *
        FROM read_parquet(
            '//10.68.152.218/d$/Pipeline/Silver/Current/Deposito.parquet',
            union_by_name = true
        )
    ),
    ultimo_batch AS (
        SELECT MAX(_batch_id) AS _batch_id
        FROM current_entidade
    )
    SELECT current_entidade.*
    FROM current_entidade
    INNER JOIN ultimo_batch
        ON current_entidade._batch_id = ultimo_batch._batch_id
)
TO '//10.68.152.218/d$/Pipeline/Silver/Delta/Deposito.parquet'
(
    FORMAT PARQUET,
    COMPRESSION ZSTD
);