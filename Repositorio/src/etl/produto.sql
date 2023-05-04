-- Databricks notebook source
WITH tb_join AS (

  SELECT DISTINCT
         t2.idVendedor,
         t3.*

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t2.idProduto = t3.idProduto

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= ADD_MONTHS('2018-01-01', -6)
  AND  t2.idVendedor IS NOT NULL

),

--SELECT * FROM tb_join

tb_summary AS (

  SELECT idVendedor,
        AVG(COALESCE(nrFotos, 0)) AS avgFotos, -- Coalesce pega o primeiro valor não nulo, pega os dados em cadei (mesma linha) diferentemente do ifnull 
        AVG(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS avgVolumeProduto,
        PERCENTILE(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) AS medianVolumeProduto,
        MIN(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS minVolumeProduto,
        MAX(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS maxVolumeProduto,

        count(distinct case when descCategoria='cama_mesa_banho' then idProduto end) / count(distinct idProduto) as pctCategoriacama_mesa_banho,
        count(distinct case when descCategoria='beleza_saude' then idProduto end) / count(distinct idProduto) as pctCategoriabeleza_saude,
        count(distinct case when descCategoria='esporte_lazer' then idProduto end) / count(distinct idProduto) as pctCategoriaesporte_lazer,
        count(distinct case when descCategoria='informatica_acessorios' then idProduto end) / count(distinct idProduto) as pctCategoriainformatica_acessorios,
        count(distinct case when descCategoria='moveis_decoracao' then idProduto end) / count(distinct idProduto) as pctCategoriamoveis_decoracao,
        count(distinct case when descCategoria='utilidades_domesticas' then idProduto end) / count(distinct idProduto) as pctCategoriautilidades_domesticas,
        count(distinct case when descCategoria='relogios_presentes' then idProduto end) / count(distinct idProduto) as pctCategoriarelogios_presentes,
        count(distinct case when descCategoria='telefonia' then idProduto end) / count(distinct idProduto) as pctCategoriatelefonia,
        count(distinct case when descCategoria='automotivo' then idProduto end) / count(distinct idProduto) as pctCategoriaautomotivo,
        count(distinct case when descCategoria='brinquedos' then idProduto end) / count(distinct idProduto) as pctCategoriabrinquedos,
        count(distinct case when descCategoria='cool_stuff' then idProduto end) / count(distinct idProduto) as pctCategoriacool_stuff,
        count(distinct case when descCategoria='ferramentas_jardim' then idProduto end) / count(distinct idProduto) as pctCategoriaferramentas_jardim,
        count(distinct case when descCategoria='perfumaria' then idProduto end) / count(distinct idProduto) as pctCategoriaperfumaria,
        count(distinct case when descCategoria='bebes' then idProduto end) / count(distinct idProduto) as pctCategoriabebes,
        count(distinct case when descCategoria='eletronicos' then idProduto end) / count(distinct idProduto) as pctCategoriaeletronicos

  FROM tb_join

  GROUP BY idVendedor

)

SELECT '2018-01-01' AS dtReference,
       * 

FROM tb_summary


-- COMMAND ----------

-- Descobrindo as categorias que mais vendem
-- Query auxiliar

SELECT descCategoria   

FROM silver.olist.item_pedido AS t2

LEFT JOIN silver.olist.produto AS t3
ON t2.idProduto = t3.idProduto

WHERE t2.idVendedor IS NOT NULL

GROUP BY 1
ORDER BY COUNT(DISTINCT idPedido) DESC

LIMIT 15
