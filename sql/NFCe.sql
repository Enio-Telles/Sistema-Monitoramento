/*
 * CONSULTA ESPELHO: bi.fato_nfe_detalhe
 * Comentarios baseados no MOC 7.0 Anexo I - Leiaute e Regras de Validacao
 */

WITH parametros AS (
    SELECT 
        :CNPJ AS cnpj_filtro
    FROM DUAL
)
SELECT
    CASE 
        -- CNPJ consultado é o EMITENTE
        WHEN d.co_emitente = p.cnpj_filtro AND d.co_tp_nf = 1 THEN '1 - SAIDA'
        WHEN d.co_emitente = p.cnpj_filtro AND d.co_tp_nf = 0 THEN '0 - ENTRADA'

-- CNPJ consultado é o DESTINATARIO
WHEN d.co_destinatario = p.cnpj_filtro
AND d.co_tp_nf = 1 THEN '0 - ENTRADA' WHEN d.co_destinatario = p.cnpj_filtro
AND d.co_tp_nf = 0 THEN '1 - SAIDA' ELSE 'INDEFINIDO' END AS tipo_operacao,
d.* -- Traz os demais campos da nota em seguida
FROM bi.fato_nfce_detalhe d, parametros p
WHERE
    d.co_destinatario = p.cnpj_filtro
    OR d.co_emitente = p.cnpj_filtro
    --AND INFPROT_CSTAT in (100,150) NFCe com código diferentes de 100 ou 150 não devem ser registradas
    -- AND PROD_CPROD LIKE '%226052'