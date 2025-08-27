QUERY_ENTRADA_DETALHADO = """
    Select /* Quebra */
	       o.situacao, case o.situacao
	                   when '90' then 'Cancelado'
	                   when '99' then 'Encerrado' 
	                   else s.descricao end desc_situacao,
	        REPLACE(L.descricao, 'REMESSA RETORNO - ', '') AS descricao_linha, --acrescentado por Michele
	       /* Ordem */
	       --:EX_RESUMO RESUMIDO,
	       o.ordem, o.abertura, o.cadastro,  t.nome,
	       /* Equipamento */
	       e.produto RR, e.descricao desc_equipamento,
	       /*Previa conclusao*/
	       cast(o.Ent_Prev as date) Prev_Conclusao,
	       /* Vendedor */
	       o.vendedor, v.nomered nome_vendedor,
	       t.vendedortmk vendedor_interno, v2.nomered nome_vend_interno
	  From osordem o
	  inner join cadastro t on t.codigo = o.cadastro
	  inner join osequipamentos e on e.equipamento = o.equipamento
	  Inner Join Ceprodutos p On p.Produto = e.Produto
	  LEFT OUTER JOIN celinhas L ON P.LINHA = L.LINHA -- inserido
	  left outer join ossituacao s on s.situacao = o.situacao
	  left outer join vendedores v on v.vendedor = o.vendedor
	  left outer join vendedores v2 on v2.vendedor = t.vendedortmk
	 Where o.Abertura Between '2025-08-03' and '2025-08-09' 
	 order by o.situacao, o.ordem
"""