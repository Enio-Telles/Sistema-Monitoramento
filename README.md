# Fiscal Parquet Analyzer

Aplicação desktop em Python para consulta de dados fiscais extraídos do Oracle em Parquet, com foco em desempenho, rastreabilidade e edição controlada da tabela `tabelas_descricoes_unificadas_desagregada`.

## 1. Arquitetura do projeto

A aplicação foi separada em quatro camadas:

- **UI (`fiscal_app/ui`)**: janelas, widgets, navegação e mensagens amigáveis.
- **Regras de negócio (`fiscal_app/services`)**: filtros, agregação, exportação, cadastro de CNPJs e integração com o pipeline Oracle.
- **Mapeamento e Consolidação (`tabelas_auditorias`)**: lógica modularizada para normalização, segregação e agrupamento de códigos de produtos.
- **Acesso a dados (`fiscal_app/services/parquet_service.py`)**: leitura lazy com Polars, paginação, seleção de colunas e persistência em Parquet.
- **Modelo de visualização (`fiscal_app/models`)**: adaptação de DataFrame Polars para `QTableView`.

### Decisões de implementação

- **Python 3.12+**: Requisito base para o ambiente `monit`.
- **Polars** é a biblioteca principal para leitura, filtro, paginação e gravação de Parquet.
- **Mapeamento de Códigos**: Geração automática de `mapeamento_codigos_{cnpj}.parquet` para auditoria de agrupamentos e segregações.
- **PySide6** fornece a interface desktop.
- **openpyxl** gera planilhas Excel.
- **python-docx** gera relatórios padronizados em Word.
- **HTML estruturado** é gerado como string e salvo em `.txt` para rastreabilidade.
- O pipeline Oracle agora utiliza o pacote `tabelas_auditorias` para processar as tabelas finais.

---

## 2. Estrutura de pastas

```text
Sistema-Monitoramento/
├── app.py
├── pipeline_oracle_parquet.py
├── merge_pdfs.py                # NOVO: Utilitário para união de PDFs
├── indice_produtos.py           # NOVO: Indexação e consolidação de atributos
├── requirements.txt
├── .gitignore
├── README.md
├── sql/
│   ├── NFe.sql
│   ├── NFCe.sql
│   ├── bloco_h.sql
│   └── ...
├── tabelas_auditorias/            # Lógica modularizada
│   ├── constants.py
│   ├── utils.py                 # Inclui Classificador CO_SEFIN
│   └── processing.py            # Motor de consolidação
├── referencias/                   # Tabelas de referência (Parquet)
│   ├── CO_SEFIN/                # Bases para inferência SEFIN
│   ├── NCM/
│   ├── CEST/
│   └── ...
├── fiscal_app/
│   ├── ...
└── workspace/
    ├── consultas/
    │   └── <cnpj>/
    │       ├── nfe_<cnpj>.parquet
    │       ├── nfce_<cnpj>.parquet
    │       └── produtos/          # NOVO: Subpasta consolidada
    │           ├── tabela_descricoes_unificadas_<cnpj>.parquet
    │           ├── codigos_desagregados_<cnpj>.parquet
    │           ├── tabela_produtos_<cnpj>.parquet
    │           ├── tabela_produtos_editavel_<cnpj>.parquet
    │           ├── tabela_itens_auditados_<cnpj>.parquet
    │           ├── tabela_somas_anuais_<cnpj>.parquet   # NOVO: Somas por ano/operacao
    │           ├── indice_produtos_<cnpj>.parquet        # NOVO: Índice de chaves únicas
    │           └── mapeamento_codigos_<cnpj>.parquet
    └── app_state/
        └── ...
```

---

## 3. Código funcional entregue

### Módulos principais

- `app.py`: inicializa a aplicação.
- `pipeline_oracle_parquet.py`: executa o Oracle → Parquet e utiliza o pacote `tabelas_auditorias` para gerar os resultados.
- `tabelas_auditorias/processing.py`: motor de consolidação que agrupa descrições similares e gera o mapeamento de códigos.
- `fiscal_app/ui/main_window.py`: janela principal, abas, filtros, exportação e agregação.
- `fiscal_app/services/parquet_service.py`: leitura lazy, filtros e paginação.
- `fiscal_app/services/export_service.py`: exporta Excel, Word e TXT com HTML.
- `fiscal_app/services/aggregation_service.py`: cria e atualiza as tabelas editáveis.
- `fiscal_app/services/registry_service.py`: persistência local dos CNPJs consultados.
- `fiscal_app/services/pipeline_service.py`: dispara o pipeline Oracle a partir da interface.
- `merge_pdfs.py`: utilitário de linha de comando para unir múltiplos arquivos PDF em um único documento.
- `indice_produtos.py`: utilitário para criar um índice único de produtos baseado em atributos consolidados.

---

## 4. Novas Funcionalidades: Mapeamento de Códigos

O sistema agora gera automaticamente uma tabela de mapeamento (`mapeamento_codigos_{cnpj}.parquet`) que permite rastrear:
- **AGRUPADOS**: Códigos originais que foram unificados sob um código padrão.
- **SEGREGADOS**: Códigos originais que foram divididos em novos códigos (`_separado_XX`).
- **REPRESENTANTES**: O código escolhido para representar um grupo de descrições similares.

---

## 5. Dicionário de Dados

Abaixo estão as descrições dos campos encontrados nas tabelas geradas pelo sistema.

### 5.1 Tabela de Produtos (`tabela_produtos_<cnpj>.parquet`)
*Tabela consolidada que agrupa descrições similares.*

- **`descrição_normalizada`**: Descrição do produto após limpeza (sem acentos, em maiúsculas, sem caracteres especiais e sem *stopwords*). É a chave de agrupamento.
- **`descricao`**: Descrição original escolhida como representante principal do grupo.
- **`codigo_padrao`**: Código de produto escolhido como representante do grupo (baseado na maior frequência; em caso de empate, o menor valor alfanumérico).
- **`qtd_codigos`**: Quantidade de códigos originais distintos que foram agrupados nesta linha.
- **`lista_codigos`**: Lista formatada dos códigos originais do grupo e suas respectivas frequências `[codigo; frequencia]`.
- **`lista_tipo_item` / `lista_ncm` / `lista_cest` / `lista_gtin` / `lista_unid`**: Listas contendo todos os valores distintos encontrados no grupo para cada atributo.
- **`tipo_item_padrao` / `NCM_padrao` / `CEST_padrao` / `GTIN_padrao`**: Valores sugeridos (moda) para cada atributo dentro do grupo.
- **`lista_fontes`**: Origens dos dados agrupados (ex: NFe, NFCe, C170, Bloco H).
- **`lista_descricoes`**: Todas as variações de descrições originais que compõem este grupo.
- **`lista_descricoes_normalizadas`**: Todas as descrições já normalizadas que foram unificadas.
- **`descricao_padrao`**: Primeira descrição normalizada identificada para o grupo (usada para rastreabilidade de sistema).
- **`lista_chaves_produto`**: Lista de IDs do índice (`chave_produto`) que compõem este grupo.
- **`Valores_Entradas_<ano>` / `Valores_Saidas_<ano>`**: Somatórios anuais dos valores de Entrada e Saída para o grupo.
- **`Estoque_final_<ano>`**: Valores de Inventário Final (Estoque) para o grupo no ano respectivo.
- **`co_sefin_inferido`**: Código SEFIN inferido via hierarquia (NCM+CEST -> CEST -> NCM).
- **`conflito_co_sefin`**: Flag booleana que indica se o grupo possui múltiplos códigos SEFIN inferidos diferentes entre seus membros.
- **`verificado`**: Campo booleano (`true`/`false`) para controle de revisão manual pelo auditor.

### 5.2 Tabela de Itens Auditados (`tabela_itens_auditados_<cnpj>.parquet`)
*Tabela detalhada no nível de item (não agregada) com todas as características originais e a inferência SEFIN.*

- **`fonte`**: Origem (nfe, nfce, c170, bloco_h, fronteira).
- **`chave_produto`**: ID único do índice de produtos (liga o item à `indice_produtos`).
- **`codigo`**: Código original do produto.
- **`descricao` / `descr_compl`**: Descrição e observações complementares.
- **`tipo_item` / `ncm` / `cest` / `gtin` / `unid`**: Atributos técnicos originais.
- **`data_mov`**: Data de emissão ou inventário.
- **`descricao_normalizada`**: Chave de limpeza usada para agrupamento.
- **`co_sefin_inferido`**: Código SEFIN atribuído individualmente a este item.

### 5.3 Tabela de Somas Anuais (`tabela_somas_anuais_<cnpj>.parquet`)
*Tabela de apoio para produtividade, com totais por produto, ano e tipo de operação.*

- **`descricao_normalizada`**: Chave de limpeza para agrupamento.
- **`ano`**: Ano extraído da data de movimentação.
- **`tipo_operacao`**: Categoria da operação (Entrada, Saída, Inventário).
- **`qtd_total`**: Soma das quantidades movimentadas.
- **`valor_total`**: Soma dos valores de produto.

### 5.4 Tabela de Índice de Produtos (`indice_produtos_<cnpj>.parquet`)
*Índice técnico que mapeia a combinação de [codigo, descricao, descr_compl, tipo_item, ncm, cest, gtin] para um ID numérico único.*

- **`chave_produto`**: ID sequencial único para o conjunto de atributos.
- **`codigo` / `descricao` / `descr_compl` / ...**: Atributos que formam a chave.
- **`lista_unidades`**: Unidades únicas encontradas para este produto.

### 5.5 Tabela de Códigos Segregados (`codigos_desagregados_<cnpj>.parquet`)
*Tabela que contém as novas entradas para códigos que foram "separados" por possuírem descrições muito diferentes.*

- **`codigo_desagregado`**: O novo código gerado (ex: `codigo_separado_01`).
- **`descricao`**: Descrição representante deste novo subgrupo.
- **`lista_tipo_item` / `lista_ncm` / `lista_cest` / ...**: Atributos específicos deste código segregado.

### 5.3 Tabela de Mapeamento (`mapeamento_codigos_{cnpj}.parquet`)
*Resumo rápido do fluxo de transformação dos códigos.*

- **`codigo_original`**: Código conforme constava no banco de dados Oracle.
- **`codigo_final`**: O código para o qual ele foi mapeado (pode ser o código padrão do grupo ou um código segregado).
- **`descricao_final`**: Descrição que representa o código final.
- **`situacao`**:
    - `REPRESENTANTE`: O código original foi mantido como o principal do grupo.
    - `AGRUPADO`: O código original foi movido para baixo de um representante diferente.
    - `SEGREGADO`: O código original foi desmembrado em novos códigos.
- **`detalhe`**: Explicação textual concisa da ação realizada.

---

## 6. Telas principais

### Tela 1 — CNPJs e arquivos
... (restante do documento mantido conforme original) ...

Na lateral esquerda:
- campo para digitar novo CNPJ;
- botão **Analisar CNPJ**;
- lista de CNPJs já consultados;
- árvore de arquivos Parquet do CNPJ selecionado.

### Tela 2 — Consulta

Na aba **Consulta**:
- seleção de coluna + operador + valor para filtro;
- lista de filtros ativos;
- seleção de colunas visíveis;
- paginação;
- visualização da tabela em `QTableView`;
- exportação para Excel, Word e TXT/HTML.

### Tela 3 — Agregação

Na aba **Agregação**:
- filtro rápido por trecho de descrição;
- abertura da tabela editável `_2`;
- lote com múltiplas linhas selecionadas;
- campos para descrição resultante e descrição normalizada resultante;
- geração da tabela editável `_2` mantendo a original intacta;
- prévia do resultado antes da gravação.

### Tela 4 — Logs

Na aba **Logs**:
- histórico em JSONL das agregações executadas.

---

## 5. Leitura e exibição de Parquet com Polars

A aplicação usa `pl.scan_parquet()` para montar `LazyFrame` e aplicar filtros antes da materialização. Isso evita carregar o arquivo inteiro sem necessidade.

Fluxo de exibição:

1. abre o schema do Parquet;
2. aplica filtros em LazyFrame;
3. calcula total de linhas filtradas;
4. carrega apenas a página solicitada com `slice(offset, page_size)`;
5. envia a página para o `QTableView`.

Isso mantém a interface mais leve em tabelas grandes.

---

## 6. Filtros

Filtros suportados:
- contém
- igual
- começa com
- termina com
- `>`
- `>=`
- `<`
- `<=`
- é nulo
- não é nulo

Para agregação por trecho de descrição, a aba **Agregação** aplica um filtro rápido na coluna `descricao` ou `descrição_normalizada`.

---

## 7. Seleção de colunas

O botão **Selecionar colunas** abre um diálogo com checkboxes.

A seleção controla:
- o que aparece na grade;
- a exportação “somente colunas visíveis”;
- os relatórios Word e TXT/HTML.

---

## 8. Exportação para Excel

Botões disponíveis:
- **Excel - tabela completa**: exporta o Parquet inteiro, sem filtros.
- **Excel - tabela filtrada**: exporta todas as colunas após filtros.
- **Excel - colunas visíveis**: exporta o recorte filtrado considerando só as colunas visíveis.

A exportação usa `openpyxl`.

---

## 9. Exportação para Word

O botão **Relatório Word** gera um `.docx` padronizado com:
- título;
- CNPJ;
- nome da tabela;
- filtros aplicados;
- colunas visíveis;
- quantidade de linhas;
- tabela com o recorte atual.

Por desempenho, o relatório Word grava até 500 linhas do recorte selecionado. O Excel continua sendo a saída mais adequada para volumes grandes.

---

## 10. Exportação para TXT com HTML

O botão **TXT com HTML** salva o código-fonte HTML do relatório em um arquivo `.txt`.

Esse HTML inclui:
- cabeçalho do relatório;
- metadados de geração;
- filtros aplicados;
- lista de colunas visíveis;
- tabela HTML com os dados do recorte atual.

---

## 11. Módulo de agregação

### Objetivo

Permitir que o usuário selecione múltiplas linhas da tabela `tabelas_descricoes_unificadas_desagregada_<cnpj>.parquet` e produza uma nova tabela editável:

- `tabelas_descricoes_unificadas_desagregada_2_<cnpj>.parquet`

A tabela original permanece intacta para controle e auditoria.

### Regra de agregação implementada

Ao agregar linhas selecionadas:

- `codigo_padrao` = código com maior frequência em `lista_codigos` das linhas selecionadas;
- em empate, vence o **menor código em ordem alfanumérica**;
- `lista_codigos` = união dos códigos selecionados;
- `lista_tipo_item`, `lista_ncm`, `lista_cest`, `lista_gtin`, `lista_unid` = união distinta dos valores;
- `tipo_item_padrao`, `NCM_padrao`, `CEST_padrao`, `GTIN_padrao` = moda entre as linhas, ignorando vazios;
- `co_sefin_padrao` (ou `co_sefin_inferido` na tabela final) = moda dos códigos SEFIN inferidos;
- `verificado` = `false` após a alteração, para indicar que a linha foi recriada.

### Rastreamento

Cada agregação gera um registro em:

- `workspace/app_state/operacoes_agregacao.jsonl`

Campos logados:
- timestamp;
- CNPJ;
- arquivo destino;
- linhas de origem;
- resultado gerado;
- regra usada para `codigo_padrao`.

---

## 12. Persistência local dos CNPJs

Os CNPJs ficam registrados em:

- `workspace/app_state/cnpjs.json`

O registro guarda:
- CNPJ;
- data de cadastro;
- última execução do pipeline.

A lista exibida na interface combina:
- o cadastro persistido;
- as pastas de CNPJ já existentes em `workspace/consultas`.

---

## 13. Instruções de execução

### 13.1 Instalar dependências

```bash
pip install -r requirements.txt
```

### 13.2 Configurar acesso Oracle

Crie um arquivo `.env` no diretório do projeto com:

```env
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
ORACLE_HOST=exa01-scan.sefin.ro.gov.br
ORACLE_PORT=1521
ORACLE_SERVICE=sefindw
```

### 13.3 Executar a interface

```bash
python app.py
```

### 13.4 Fluxo de uso recomendado

1. digite um CNPJ e clique em **Analisar CNPJ**;
2. selecione o CNPJ na lista lateral;
3. abra uma tabela Parquet;
4. aplique filtros e selecione colunas;
5. exporte para Excel, Word ou TXT/HTML;
6. para agregação, abra a tabela desagregada, filtre por descrição, selecione linhas e envie para a aba **Agregação**;
7. gere a tabela `_2`.

### 13.5 Unir PDFs
```bash
python merge_pdfs.py pasta_com_pdfs -o documento_final.pdf
```

---

## 14. Melhorias futuras sugeridas

As próximas melhorias mais úteis seriam:

- filtros compostos com grupos AND/OR;
- edição pontual de campos na tabela `_2` com trilha de auditoria por célula;
- comparação visual lado a lado entre `_desagregada` e `_desagregada_2`;
- busca semântica de descrições parecidas;
- escolha assistida da descrição resultante com sugestões automáticas;
- exportação de logs também em Parquet;
- execução do pipeline em segundo plano com barra de progresso por consulta SQL.

---

## Observações finais

- A aplicação evita `pandas` no fluxo de interface. O uso principal é `Polars`.
- O pipeline Oracle existente ainda usa `pandas` internamente para a etapa de extração e consolidação já construída.
- Para arquivos muito grandes, a grade exibe por paginação. Isso é proposital para manter a aplicação responsiva.
