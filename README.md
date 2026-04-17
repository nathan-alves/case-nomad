# Case Analítico — Pizzaria

Diagnóstico de receita e identificação de oportunidades de crescimento com base nos dados reais de operação.

---

## Sobre o projeto

Análise completa de uma pizzaria com 48.620 registros de pedidos ao longo de 2015.  
O objetivo foi entender o comportamento de vendas e traduzir os achados em oportunidades financeiras quantificadas — sem nenhum novo investimento.

O entregável consiste em um relatório executivo com os principais resultados e oportunidades identificadas, acompanhado de um dashboard no Looker Studio para exploração interativa das visões.

- [Relatório Executivo (PDF)](./Relatório%20-%20Case%20Pizzaria.pdf)
- [Dashboard Looker Studio](https://datastudio.google.com/reporting/77835a04-3ca0-4987-878d-0346fa3e9455)

---

## Principais resultados

| Métrica | Valor |
|---|---|
| Receita total 2015 | R$ 817.860 |
| Média mensal | R$ 68.155 |
| Total de pedidos | 21.350 |
| Ticket médio por pedido | R$ 38,31 |
| Concentração (top 4 produtos) | 20,3% da receita |

### Oportunidades identificadas — +R$ 55,9k/ano

| Ação | Potencial estimado |
|---|---|
| Expansão do turno manhã | +R$ 22,4k/ano |
| Otimização do mix de produtos | +R$ 16,4k/ano |
| Ticket no fim de semana (+5%) | +R$ 11,1k/ano |
| Upsell tamanho M → L (10% conversão) | +R$ 6,0k/ano |

> Todas as estimativas são conservadoras e baseadas nos dados reais de 2015,  
> sem projeção de novos clientes ou investimento adicional.

---

## Tecnologias utilizadas

- **Databricks / PySpark** — pipeline ELT e processamento dos dados
- **Delta Lake** — persistência da base consolidada
- **Looker Studio** — visualização e dashboard interativo
- **Python** — transformações, helpers e exportação

---

## Estrutura do projeto

### Pipeline de dados (Databricks)

```plaintext
workspace.case_pizzaria/
├── pizza_sales    # Tabela de pedidos — preço, tamanho e quantidade (fonte 1)
├── pizzas         # Dimensão de produtos — nome, categoria e ingredientes (fonte 2)
└── base_final     # Tabela consolidada — saída do ELT, base para consumo
```


**`notebook case_pizzaria.py`** — job de ELT com 7 etapas:

| Etapa | O que faz |
|---|---|
| 1 | Carga das tabelas `pizza_sales` e `pizzas` via `spark.table()` |
| 2 | Extração de `pizza_type_id` a partir de `pizza_name` em `pizza_sales` — remoção do sufixo de tamanho (`_m`, `_l`, `_s`, `_xl`, `_xxl`) |
| 3 | Join entre `pizza_sales` e `pizzas` pela chave comum `pizza_type_id` (`left join`) |
| 4 | Cálculo da receita (`quantity × unit_price`) |
| 5 | Helpers de formatação |
| 6 | Export CSV para consumo local |
| 7 | Persistência da `base_final` no Spark/Delta |

---

## Autor

**Nathan Alves**
