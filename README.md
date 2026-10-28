# PomboLake

**Pipeline de Engenharia de Dados | Python • Parquet • DuckDB • PostgreSQL**

O **PomboLake** é um projeto de integração, processamento e disponibilização de dados estruturado em camadas **Bronze, Silver e Gold**. O fluxo extrai informações de diferentes fontes, armazena dados brutos em arquivos Parquet, realiza transformações SQL com DuckDB e disponibiliza os resultados em um Data Warehouse PostgreSQL (`hub_dw`).

> **Sobre este repositório:** os arquivos SQL identificados como exemplos ilustram a estrutura e as transformações utilizadas no projeto. Não devem ser interpretados como configurações prontas para produção.

## Arquitetura

```text
Fontes de dados (bancos / APIs)
             |
             v
  Pombo_Extrator.py / extrator_pombo_api.py
             |
             v
      BRONZE (Parquet)
             |
             v
      Pombo_Refinador.py
          (DuckDB)
             |
       +-----+-----+
       |           |
       v           v
     SILVER       GOLD
   (Parquet)    (Parquet)
                   |
                   v
        Pombo_Orquestrador.py
                   |
                   v
        PostgreSQL — hub_dw
                   |
                   v
      Metabase / Consultas e BI
```

## Etapas do processamento

### 1. Extração — Bronze

- **`Pombo_Extrator.py`**: componente de extração de dados de fontes de banco de dados; gera arquivos **Parquet** na camada **Bronze**.
- **`extrator_pombo_api.py`**: componente de extração de dados de **APIs**; também gera arquivos **Parquet** na camada **Bronze**.

A camada Bronze preserva os dados recebidos das fontes para processamento posterior.

### 2. Refinamento — Silver e Gold

- **`Pombo_Refinador.py`**: processa os arquivos Parquet da Bronze utilizando **DuckDB** e executa as transformações SQL para produzir as camadas **Silver** e **Gold**.
- **Silver**: camada de tratamento, padronização e preparação dos dados.
- **Gold**: camada de dados refinados, preparada para consumo analítico e carga no Data Warehouse.

### 3. Orquestração — Data Warehouse

- **`Pombo_Orquestrador.py`**: conecta-se ao banco PostgreSQL **`hub_dw`** e realiza a etapa de carga dos dados processados no Data Warehouse.

## Arquivos SQL de exemplo

Os três arquivos abaixo são **exemplos de referência** disponibilizados para demonstrar a lógica e a estrutura do projeto. **Não são scripts de implantação automática nem devem ser executados sem adaptação e validação no ambiente de destino.**

| Arquivo | Finalidade | Classificação |
|---|---|---|
| `Current_gold.sql` | Exemplo das transformações SQL da camada Gold. | **EXEMPLO** |
| `current_silver.sql` | Exemplo das transformações SQL da camada Silver. | **EXEMPLO** |
| `executar_hub_exemplo.sql` | Exemplo de estrutura SQL do Data Warehouse PostgreSQL (`hub_dw`). | **EXEMPLO** |

> **Observação:** os nomes acima devem corresponder exatamente aos arquivos publicados. O GitHub diferencia maiúsculas e minúsculas nos caminhos exibidos.

## Metabase e execução como serviços Windows

O projeto também inclui arquivos de apoio para configurar o **Metabase**, utilizar **PostgreSQL como banco de aplicação do Metabase** e executar componentes automaticamente como **serviços do Windows**.

| Arquivo | Finalidade |
|---|---|
| `mig.bat` | Rotina de apoio à migração/configuração do banco de aplicação do Metabase para PostgreSQL. |
| `Iniciar_Metabase.bat` | Script de inicialização do Metabase no Windows. |
| `metabase_executar.sql` | Script SQL de referência para a configuração do ambiente Metabase no PostgreSQL. |
| `metabase.py` | Componente Python de execução do Metabase como serviço Windows, com apoio do **NSSM**. |
| `pombo_service.py` | Componente Python para execução dos serviços do PomboLake de forma automatizada. |
| `main_servico.py` | Componente adicional relacionado à execução de serviços do projeto. |

**NSSM (Non-Sucking Service Manager):** utilitário externo utilizado para registrar e gerenciar aplicações como serviços do Windows. É necessário disponibilizar o NSSM no ambiente para os fluxos que dependem dele. A intenção da configuração de serviços é permitir a **inicialização automática**, sem precisar abrir manualmente cada programa após o início do Windows.

> **Importante:** a migração do banco de aplicação do Metabase para PostgreSQL é distinta da carga analítica do PomboLake para o banco `hub_dw`. Os scripts de migração, inicialização e registro de serviços dependem do ambiente, de permissões administrativas e de parâmetros locais. Revise-os antes de executar; a função de cada arquivo nesta seção foi descrita com base no fluxo informado pelo autor.

## Outros componentes do repositório

Além dos componentes centrais, o projeto reúne scripts auxiliares de configuração, serviços, testes e rotinas SQL. Entre eles:

| Arquivo | Papel no projeto |
|---|---|
| `Configurador_Pombo_API.py` / `pombo_configurador.py` | Componentes relacionados à configuração do ambiente e das fontes. |
| `Pombo.py` | Script relacionado ao processamento/execução do PomboLake. |
| `Orquestrador_Pombo.py` | Script adicional de orquestração. |
| `PomboService.spec` / `MetabaseService.spec` | Arquivos de especificação para empacotamento dos componentes de serviço. |
| `executar_central.sql` | Definições SQL do banco central de configurações e controle no PostgreSQL. |
| `scripts_sybase.sql` | Scripts SQL associados à integração com Sybase SQL Anywhere. |

## Tecnologias

- **Python:** extração, automação e integração entre componentes.
- **Parquet:** armazenamento colunar dos dados entre as etapas.
- **DuckDB:** execução das transformações SQL das camadas Silver e Gold.
- **PostgreSQL:** persistência e disponibilização do Data Warehouse `hub_dw`.
- **Sybase SQL Anywhere e APIs:** exemplos de fontes de dados integradas ao pipeline.
- **Metabase:** interface de consultas, exploração e visualização dos dados.
- **NSSM e serviços Windows:** execução automatizada de componentes do ambiente.

## Execução e configuração

O projeto depende de configurações específicas de conexão, permissões de acesso, caminhos de armazenamento e bibliotecas Python. Antes de executar em outro ambiente, é necessário revisar essas dependências e os parâmetros de cada componente.

**Fluxo lógico de execução:**

1. Configurar as conexões e os diretórios de trabalho.
2. Executar o extrator correspondente à fonte (banco de dados ou API), gerando a Bronze.
3. Executar o refinador para produzir Silver e Gold com DuckDB.
4. Executar o orquestrador para carregar os dados no PostgreSQL `hub_dw`.
5. Quando necessário, configurar o Metabase com banco de aplicação PostgreSQL e registrar os componentes como serviços Windows com NSSM.

Este README descreve o fluxo arquitetural; os comandos de instalação e execução devem ser ajustados ao ambiente antes de uso.

## Segurança e publicação

Não publique credenciais, tokens de API, senhas, dados operacionais, dumps com registros reais ou arquivos de configuração sensíveis. Utilize variáveis de ambiente ou outro mecanismo apropriado de gerenciamento de segredos. Os arquivos de exemplo servem apenas para fins de documentação técnica.

## Objetivo

Demonstrar uma arquitetura de integração e transformação de dados que separa a ingestão, o refinamento e a carga analítica, permitindo organizar o fluxo de informações de múltiplas fontes até o Data Warehouse.
