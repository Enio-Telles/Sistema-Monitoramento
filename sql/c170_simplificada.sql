/*
 * EXTRAÇÃO SIMPLIFICADA E OTIMIZADA: Registro C170 (Itens das NFs)
 * Utiliza CTEs para pré-filtrar C100, C170 e 0200 apenas para arquivos válidos.
 */
WITH
    PARAMETROS AS (
        SELECT
            :CNPJ AS cnpj_filtro,
            NVL (
                TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'),
                TRUNC(SYSDATE)
            ) AS dt_corte
        FROM dual
    ),
    
    ARQUIVOS_RANKING AS (
        SELECT r.id AS reg_0000_id, r.cnpj, r.dt_ini, r.data_entrega, 
               ROW_NUMBER() OVER (
                   PARTITION BY r.cnpj, r.dt_ini
                   ORDER BY r.data_entrega DESC, r.id DESC
               ) AS rn
        FROM sped.reg_0000 r
        JOIN PARAMETROS p ON r.cnpj = p.cnpj_filtro
        WHERE r.data_entrega <= p.dt_corte
    ),
    
    -- Isola apenas os arquivos válidos (última retificação)
    ARQUIVOS_VALIDOS AS (
        SELECT reg_0000_id, cnpj, dt_ini
        FROM ARQUIVOS_RANKING
        WHERE rn = 1
    ),
    
    -- Pré-filtra o C100 com base nos arquivos válidos
    DADOS_C100 AS (
        SELECT c100.id, c100.reg_0000_id, c100.chv_nfe, c100.cod_sit, c100.ind_emit, 
               c100.ind_oper, c100.num_doc, c100.dt_doc
        FROM sped.reg_c100 c100
        INNER JOIN ARQUIVOS_VALIDOS arq ON arq.reg_0000_id = c100.reg_0000_id
        -- Dica: Você pode adicionar "WHERE c100.cod_sit IN ('00', '01')" aqui se desejar
    ),
    
    -- Pré-filtra o C170 com base nos arquivos válidos
    DADOS_C170 AS (
        SELECT c170.reg_c100_id, c170.reg_0000_id, c170.num_item, c170.cod_item, 
               c170.descr_compl, c170.cfop, c170.cst_icms, c170.qtd, c170.unid, 
               c170.vl_item, c170.vl_icms, c170.vl_bc_icms, c170.aliq_icms, 
               c170.vl_bc_icms_st, c170.vl_icms_st, c170.aliq_st
        FROM sped.reg_c170 c170
        INNER JOIN ARQUIVOS_VALIDOS arq ON arq.reg_0000_id = c170.reg_0000_id
    ),
    
    -- Pré-filtra o 0200 (Cadastro de Produtos) com base nos arquivos válidos
    DADOS_0200 AS (
        SELECT r200.reg_0000_id, r200.cod_item, r200.cod_barra, r200.cod_ncm, 
               r200.cest, r200.tipo_item, r200.descr_item
        FROM sped.reg_0200 r200
        INNER JOIN ARQUIVOS_VALIDOS arq ON arq.reg_0000_id = r200.reg_0000_id
    )

-- Seleção Final cruzando apenas os dados já reduzidos
SELECT
    TO_CHAR(arq.dt_ini, 'YYYY/MM') AS periodo_efd,
    TRIM(c100.chv_nfe) AS chv_nfe,
    c100.cod_sit,
    c100.ind_emit,
    c100.ind_oper,
    c100.num_doc,
    CASE
        WHEN c100.dt_doc IS NOT NULL
        AND REGEXP_LIKE (c100.dt_doc, '^\d{8}$') THEN TO_DATE(c100.dt_doc, 'DDMMYYYY')
        ELSE NULL
    END AS dt_doc,
    c170.num_item,
    c170.cod_item,
    r200.cod_barra,
    r200.cod_ncm,
    r200.cest,
    r200.tipo_item,
    r200.descr_item,
    c170.descr_compl,
    c170.cfop,
    c170.cst_icms,
    NVL (c170.qtd, 0) AS qtd,
    c170.unid,
    c170.vl_item,
    NVL (c170.vl_icms, 0) AS vl_icms,
    NVL (c170.vl_bc_icms, 0) AS vl_bc_icms,
    c170.aliq_icms,
    NVL (c170.vl_bc_icms_st, 0) AS vl_bc_icms_st,
    NVL (c170.vl_icms_st, 0) AS vl_icms_st,
    c170.aliq_st
FROM
    DADOS_C170 c170
    INNER JOIN ARQUIVOS_VALIDOS arq 
            ON arq.reg_0000_id = c170.reg_0000_id
    INNER JOIN DADOS_C100 c100 
            ON c100.id = c170.reg_c100_id 
           AND c100.reg_0000_id = c170.reg_0000_id
    LEFT JOIN DADOS_0200 r200 
           ON r200.reg_0000_id = c170.reg_0000_id 
          AND r200.cod_item = c170.cod_item
ORDER BY 
    arq.dt_ini, c100.num_doc, c170.num_item;