-- Databricks notebook source
-- Selecionando a range de data para pedidos
-- Preparing data to avoid duplication
WITH tb_pedidos AS (
    
  SELECT 
    DISTINCT
    t1.idPedido,
    t2.idVendedor

  FROM silver.olist.pedido as t1
  
  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido
  
  WHERE t1.dtPedido < '2018-01-01'
  AND   t1.dtPedido >= add_months('2018-01-01', -6)
  AND   idVendedor IS NOT NULL
    
),   
    
tb_join AS (

  SELECT t1.idVendedor,
         t2.*

  FROM tb_pedidos AS t1 -- Starting with tb_pedidos

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido

),

tb_group AS (

    SELECT idVendedor,
           descTipoPagamento,
           count(distinct idPedido) AS qtdePedidoMeioPagamento,
           sum(vlPagamento) as vlPedidoMeioPagamento

    FROM tb_join

    GROUP BY idVendedor, descTipoPagamento
    ORDER BY idVendedor, descTipoPagamento

),

tb_summary AS (

--SELECT DISTINCT descTipoPagamento FROM tb_group 
  SELECT idVendedor,

    -- Quantidades de pedidos por forma de pagamento
    sum(CASE WHEN descTipoPagamento= 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_credit_card_pedido,
    sum(CASE WHEN descTipoPagamento= 'boleto'      THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_boleto_pedido,
    sum(CASE WHEN descTipoPagamento= 'debit_card'  THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_debit_card_pedido,
    sum(CASE WHEN descTipoPagamento= 'voucher'     THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_voucher_pedido,

    -- Valor total por forma de pagamento
    sum(CASE WHEN descTipoPagamento= 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card_pedido,
    sum(CASE WHEN descTipoPagamento= 'boleto'      THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto_pedido,
    sum(CASE WHEN descTipoPagamento= 'debit_card'  THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_card_pedido,
    sum(CASE WHEN descTipoPagamento= 'voucher'     THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher_pedido,

    -- Pct da quantidade de pedidos total por forma de pagamento
    sum(CASE WHEN descTipoPagamento= 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / sum(qtdePedidoMeioPagamento) AS pct_qtd_credit_card_pedido,
    sum(CASE WHEN descTipoPagamento= 'boleto'      THEN qtdePedidoMeioPagamento ELSE 0 END) / sum(qtdePedidoMeioPagamento) AS pct_qtd_boleto_pedido,
    sum(CASE WHEN descTipoPagamento= 'debit_card'  THEN qtdePedidoMeioPagamento ELSE 0 END) / sum(qtdePedidoMeioPagamento) AS pct_qtd_debit_card_pedido,
    sum(CASE WHEN descTipoPagamento= 'voucher'     THEN qtdePedidoMeioPagamento ELSE 0 END) / sum(qtdePedidoMeioPagamento) AS pct_qtd_voucher_pedido,

    -- Pct do valor total por forma de pagamento
    sum(CASE WHEN descTipoPagamento= 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
    sum(CASE WHEN descTipoPagamento= 'boleto'      THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
    sum(CASE WHEN descTipoPagamento= 'debit_card'  THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_debit_card_pedido,
    sum(CASE WHEN descTipoPagamento= 'voucher'     THEN vlPedidoMeioPagamento ELSE 0 END) / sum(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido


  FROM tb_group 

  GROUP BY idVendedor

),

-- Cruzando a tabela cartão com a tb_summary
tb_cartao AS (

  SELECT idVendedor,
        round(AVG(nrParcelas)) AS avgQtdeParcelas,
        PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas,
        MAX(nrParcelas) AS maxQtdeParcelas,
        MIN(nrParcelas) AS minQtdeParcelas

  FROM tb_join 

  WHERE descTipoPagamento = 'credit_card'

  GROUP BY idVendedor

)

SELECT 
       '2018-01-01' AS dtReferencia,
       t1.*,
       t2.avgQtdeParcelas,
       t2.medianQtdeParcelas,
       t2.maxQtdeParcelas,
       t2.minQtdeParcelas

FROM tb_summary AS t1

LEFT JOIN tb_cartao AS t2
ON t1.idVendedor = t2.idVendedor

--31:13
