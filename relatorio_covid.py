import pandas as pd
import mysql.connector

# -------------------------
# 1️⃣ Conexão com MySQL
# -------------------------
db_config = {
    'host': 'localhost',
    'user': 'dbeaver',
    'password': '123456',
    'database': 'covid_data'
}

conn = mysql.connector.connect(**db_config)

# Pega todos os dados da tabela
query = "SELECT * FROM casos_covid"
df = pd.read_sql(query, conn)
conn.close()

# -------------------------
# 2️⃣ Todos casos de morte por cidade
# -------------------------
mortes_por_cidade = df.groupby('city')['new_deaths'].sum().reset_index()
mortes_por_cidade = mortes_por_cidade.sort_values(by='new_deaths', ascending=False)
print("=== Mortes por cidade ===")
print(mortes_por_cidade)

# -------------------------
# 3️⃣ População estimada antes e depois dos casos
# -------------------------
populacao_total = df.groupby('city')['population'].max().reset_index()
mortes_acumuladas = df.groupby('city')['new_deaths'].sum().reset_index()
populacao_apos_casos = populacao_total.copy()
populacao_apos_casos['population_after_cases'] = populacao_total['population'] - mortes_acumuladas['new_deaths']
print("\n=== População antes e depois dos casos ===")
print(populacao_apos_casos)

# -------------------------
# 4️⃣ Maior cidade com quantidade de casos
# -------------------------
casos_por_cidade = df.groupby('city')['new_confirmed'].sum().reset_index()
maior_cidade = casos_por_cidade.sort_values(by='new_confirmed', ascending=False).head(1)
print("\n=== Maior cidade em casos confirmados ===")
print(maior_cidade)

# -------------------------
# 5️⃣ Menor cidade com quantidade de casos
# -------------------------
menor_cidade = casos_por_cidade.sort_values(by='new_confirmed', ascending=True).head(1)
print("\n=== Menor cidade em casos confirmados ===")
print(menor_cidade)

# -------------------------
# 6️⃣ Salvar relatório em Excel (opcional)
# -------------------------
with pd.ExcelWriter("relatorio_covid.xlsx") as writer:
    mortes_por_cidade.to_excel(writer, sheet_name="MortesPorCidade", index=False)
    populacao_apos_casos.to_excel(writer, sheet_name="Populacao", index=False)
    casos_por_cidade.to_excel(writer, sheet_name="CasosPorCidade", index=False)
