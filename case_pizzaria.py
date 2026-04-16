# Databricks notebook source
# Importação das bibliotecas

import pandas as pd
import numpy as np

# Leitura das tabelas do catálogo Databricks
df_sales = spark.table("workspace.case_pizzaria.pizza_sales").toPandas()
df_pizza = spark.table("workspace.case_pizzaria.pizzas").toPandas()

print("pizza_sales:", df_sales.shape)
print("pizzas:", df_pizza.shape)

display(df_sales.head())
display(df_pizza.head())

# COMMAND ----------

# Padronização dos campos-chave
df_sales["pizza_name_id"] = df_sales["pizza_name_id"].astype(str).str.strip().str.lower()
df_pizza["pizza_type_id"] = df_pizza["pizza_type_id"].astype(str).str.strip().str.lower()

# Criar chave correta de relacionamento
df_sales["pizza_type_id"] = df_sales["pizza_name_id"].str.replace(r"_[a-z]+$", "", regex=True)

# Tipos numéricos
df_sales["pizza_id"] = pd.to_numeric(df_sales["pizza_id"], errors="coerce")
df_sales["order_id"] = pd.to_numeric(df_sales["order_id"], errors="coerce")
df_sales["quantity"] = pd.to_numeric(df_sales["quantity"], errors="coerce")
df_sales["unit_price"] = pd.to_numeric(df_sales["unit_price"], errors="coerce")

# Remover possíveis inconsistências após conversão
df_sales = df_sales.dropna(subset=["pizza_id", "order_id", "pizza_name_id", "quantity", "unit_price"])

# Datas e horas
df_sales["order_date"] = pd.to_datetime(df_sales["order_date"], errors="coerce")
df_sales["order_time"] = pd.to_datetime(df_sales["order_time"], errors="coerce")

# Derivações temporais
df_sales["order_time_only"] = df_sales["order_time"].dt.strftime("%H:%M:%S")
df_sales["order_hour"] = df_sales["order_time"].dt.hour
df_sales["order_minute"] = df_sales["order_time"].dt.minute

# Garantir unicidade da dimensão
df_pizza = df_pizza.drop_duplicates(subset=["pizza_type_id"])

# COMMAND ----------

# Join entre as tabelas

df_final = df_sales.merge(
    df_pizza,
    on="pizza_type_id",
    how="left"
)

print("Linhas antes merge:", len(df_sales))
print("Linhas depois merge:", len(df_final))
print("Registros sem match:", df_final["pizza_name"].isna().sum())

display(df_final.head())

# COMMAND ----------

# Receita
df_final["revenue"] = df_final["quantity"] * df_final["unit_price"]

# Datas
df_final["order_year"] = df_final["order_date"].dt.year
df_final["order_month"] = df_final["order_date"].dt.month
df_final["order_day"] = df_final["order_date"].dt.day
df_final["order_weekday"] = df_final["order_date"].dt.day_name()

# Período do dia
df_final["day_period"] = np.select(
    [
        df_final["order_hour"].between(6, 11),
        df_final["order_hour"].between(12, 17),
        df_final["order_hour"].between(18, 23)
    ],
    ["Manhã", "Tarde", "Noite"],
    default="Madrugada"
)

# Organização final
df_final = df_final[
    [
        "pizza_id",
        "order_id",
        "order_date",
        "order_time_only",
        "order_hour",
        "order_minute",
        "order_year",
        "order_month",
        "order_day",
        "order_weekday",
        "day_period",
        "pizza_name_id",
        "pizza_type_id",
        "pizza_name",
        "pizza_category",
        "pizza_ingredients",
        "pizza_size",
        "quantity",
        "unit_price",
        "revenue"
    ]
]

display(df_final.head())

# COMMAND ----------

# ── Formatação ─────────────────────────────────────────────────────
def format_currency(value):
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def format_number(value):
    return f"{value:,.0f}".replace(",", ".")

def format_decimal(value):
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ── Métricas ──────────────────────────────────────────────────────────────────
receita_total    = df_final["revenue"].sum()
quantidade_total = df_final["quantity"].sum()
pedidos_unicos   = df_final["order_id"].nunique()
pizza_id_unico   = df_final["pizza_id"].nunique()
preco_min        = df_final["unit_price"].min()
preco_max        = df_final["unit_price"].max()
preco_medio      = df_final["unit_price"].mean()
ticket_medio     = receita_total / pedidos_unicos if pedidos_unicos != 0 else 0

print("🔢 ESTRUTURA:")
print("Linhas:", format_number(len(df_final)))
print("Pizza_id único:", format_number(pizza_id_unico))
print("Pedidos únicos:", format_number(pedidos_unicos))

print("\n📊 VOLUME:")
print("Quantidade total:", format_number(quantidade_total))

print("\n💰 RECEITA:")
print("Receita total:", format_currency(receita_total))
print("Ticket médio por pedido:", format_currency(ticket_medio))

print("\n📉 PREÇO:")
print("Menor preço unitário:", format_currency(preco_min))
print("Maior preço unitário:", format_currency(preco_max))
print("Preço médio unitário:", format_currency(preco_medio))

print("\n🧪 NULOS:")
print(df_final.isna().sum())

print("\n🔍 DUPLICIDADE:")
print("Duplicidade por pizza_id:", format_number(df_final.duplicated(subset=["pizza_id"]).sum()))

print("\n✅ VALIDAÇÕES:")
print("Linhas da base final = pizza_id único:", len(df_final) == pizza_id_unico)
print("Registros sem match de produto:", df_final["pizza_name"].isna().sum())
print("Linhas da base final = linhas da base de vendas:", len(df_final) == len(df_sales))

# Validações
assert len(df_final) == pizza_id_unico,                    "Há duplicidade em pizza_id na base final"
assert df_final["pizza_name"].isna().sum() == 0,           "Existem registros sem correspondência de produto"
assert len(df_final) == len(df_sales),                     "O merge alterou a quantidade de linhas da base fato"
assert df_pizza["pizza_type_id"].duplicated().sum() == 0,  "A dimensão de pizzas possui chaves duplicadas"
assert quantidade_total == df_sales["quantity"].sum(),      "A soma de quantity mudou após o merge"

# COMMAND ----------

# ── Export CSV ────────────────────────────────────────────────────────────────
path_tmp = "/tmp/pizza_base_final.csv"

df_final.to_csv(path_tmp, index=False, encoding="utf-8-sig")
print("Arquivo salvo em:", path_tmp)

# ── Validação do export ───────────────────────────────────────────────────────
df_export_check = pd.read_csv(path_tmp)

print(df_export_check.shape)
display(df_export_check.head())

# COMMAND ----------

# ── Persistência no Spark ─────────────────────────────────────────────────────
spark_df = spark.createDataFrame(df_final)

spark_df.write.mode("overwrite").saveAsTable("workspace.case_pizzaria.base_final")
print("Tabela criada com sucesso: workspace.case_pizzaria.base_final")

# ── Validação SQL ─────────────────────────────────────────────────────────────
display(spark.sql("""
    SELECT
        COUNT(*)              AS linhas,
        COUNT(DISTINCT pizza_id)  AS pizza_id_unico,
        COUNT(DISTINCT order_id)  AS pedidos_unicos,
        SUM(quantity)         AS quantidade_total,
        ROUND(SUM(revenue), 2)    AS receita_total
    FROM workspace.case_pizzaria.base_final
"""))