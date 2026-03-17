/*
 * EXTRAÇÃO SIMPLIFICADA: Fronteira (Cálculos de ICMS ST e Próprio no SITAFE)
 * Vincula itens da NF-e (XML) aos cálculos realizados pela auditoria de fronteira.
 */
WITH
    PARAMETROS AS (
        SELECT
:CNPJ AS cnpj_filtro,
            NVL (
                TO_DATE(
:data_limite_processamento,
                    'DD/MM/YYYY'
                ),
                TRUNC(SYSDATE)
            ) AS dt_corte
        FROM dual
    )

SELECT
    CASE
    -- CNPJ consultado é o EMITENTE
        WHEN nfe.co_emitente = p.cnpj_filtro
        AND nfe.co_tp_nf = 1 THEN '1 - SAIDA'
        WHEN nfe.co_emitente = p.cnpj_filtro
        AND nfe.co_tp_nf = 0 THEN '0 - ENTRADA'
        -- CNPJ consultado é o DESTINATARIO
        WHEN nfe.co_destinatario = p.cnpj_filtro
        AND nfe.co_tp_nf = 1 THEN '0 - ENTRADA'
        WHEN nfe.co_destinatario = p.cnpj_filtro
        AND nfe.co_tp_nf = 0 THEN '1 - SAIDA'
        ELSE 'INDEFINIDO'
    END AS tipo_operacao,
    nfe.chave_acesso,
    nfe.seq_nitem AS num_item,
    nfe.prod_cprod AS cod_item,
    nfe.prod_xprod AS desc_item,
    nfe.prod_ncm AS ncm,
    nfe.prod_cest AS cest,
    nfe.prod_qcom AS qtd_comercial,
    nfe.prod_vprod AS valor_produto,
    nfe.icms_vbcst AS bc_icms_st_destacado,
    nfe.icms_vicmsst AS icms_st_destacado,

-- DADOS DO SITA FE - FRONTEIRA
calc_front.it_co_sefin AS co_sefin,
calc_front.it_co_rotina_calculo AS cod_rotina_calculo,
calc_front.it_vl_icms AS valor_icms_fronteira
FROM
    bi.fato_nfe_detalhe nfe
    CROSS JOIN PARAMETROS p
    INNER JOIN sitafe.sitafe_nfe_calculo_item calc_front ON calc_front.it_nu_chave_acesso = nfe.chave_acesso
    AND calc_front.it_nu_item = nfe.seq_nitem
WHERE
    -- Verifica se emitente ou destinatário é o CNPJ do filtro
    (
        nfe.co_emitente = p.cnpj_filtro
        OR nfe.co_destinatario = p.cnpj_filtro
    )
    -- Adicionando um possível filtro de data de processamento/emissão para otimização
    AND nfe.dhemi <= p.dt_corte