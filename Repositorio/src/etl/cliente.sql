-- Databricks notebook source
SELECT *

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2

ON t1.idPedido = t2.idPedido

-- 47:18 video 3
