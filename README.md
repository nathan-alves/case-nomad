# Case Analítico — Pizzaria

Diagnóstico de receita e identificação de oportunidades de crescimento com base nos dados fornecidos.

---

## Sobre o projeto

Análise completa de uma pizzaria com 48.620 registros de pedidos ao longo de 2015.  
O objetivo foi entender o comportamento de vendas e traduzir os achados em oportunidades financeiras quantificadas — sem nenhum novo investimento.

O entregável é um relatório executivo com clareza nos resultados obtidos e melhorias identificadas, e um dashboard desenvolvido no Looker trazendo maior amplitude e domínio das visões para apresentação.

---

## Principais resultados

- **Receita total 2015:** R$ 817.860 (média de R$ 68k/mês)
- **Concentração:** top 4 produtos respondem por 20,3% da receita
- **Gargalo:** período da manhã representa menos de 10% da receita com capacidade ociosa
- **Oportunidades identificadas: +R$ 55,9k/ano**

| Oportunidade            | Potencial     |
|-------------------------|---------------|
| Expansão turno manhã    | +R$ 22,5k/ano |
| Otimização do mix       | +R$ 16,4k/ano |
| Ticket no fim de semana | +R$ 11,1k/ano |
| Upsell tamanho M → L    | +R$  6,0k/ano |

---

## Estrutura do projeto

### Pipeline de dados (Databricks)

```
workspace.case_pizzaria/
├── pizza_sales    # Tabela de pedidos — contém preço, tamanho e quantidade (fonte 1)
├── pizzas         # Dimensão de produtos — nome, categoria e ingredientes (fonte 2)
└── base_final     # Tabela consolidada — saída do ELT, base para consumo
```

**Notebook `case_pizzaria.py`** — job de ELT com 7 etapas:

| Etapa | O que faz |
|-------|-----------|
| 1 | Carga das tabelas `pizza_sales` e `pizzas` via `spark.table()` |
| 2 | Padronização dos campos-chave |
| 3 | Join entre as duas tabelas por `pizza_type_id` |
| 4 | Cálculo da receita (`quantity × unit_price`) |
| 5 | Helpers de formatação |
| 6 | Export CSV para consumo local |
| 7 | Persistência da `base_final` no Spark/Delta |

## Autor

Nathan Alves
