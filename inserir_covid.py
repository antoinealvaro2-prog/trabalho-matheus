import pandas as pd
import mysql.connector
from mysql.connector import Error

# -------------------------
# CONFIGURAÇÃO
# -------------------------
caminho_arquivo = r"C:/Users/aluno.lorena/Documents/Abra/CSV_Cadabra/caso_full.csv"
chunksize = 100000  # processa 100k linhas por vez

db_config = {
    'host': 'localhost',
    'user': 'dbeaver',       # seu usuário MySQL
    'password': '123456',    # sua senha MySQL
    'database': 'covid_data'
}

# -------------------------
# FUNÇÃO PARA LIMPAR CADA CHUNK
# -------------------------
def limpar_chunk(df):
    df = df.drop_duplicates()
    df = df[df['city'].notna()]
    # converter colunas numéricas e preencher NaNs
    df['population'] = df['estimated_population'].fillna(0).astype(int)
    df['new_confirmed'] = df['new_confirmed'].fillna(0).astype(int)
    df['new_deaths'] = df['new_deaths'].fillna(0).astype(int)
    df['last_available_confirmed'] = df['last_available_confirmed'].fillna(0).astype(int)
    df['last_available_deaths'] = df['last_available_deaths'].fillna(0).astype(int)
    return df

# -------------------------
# FUNÇÃO PARA CRIAR TABELA NO MYSQL (UMA VEZ)
# -------------------------
def criar_tabela(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS casos_covid (
        city VARCHAR(255),
        city_ibge_code VARCHAR(20),
        state VARCHAR(10),
        date DATE,
        population INT,
        new_confirmed INT,
        new_deaths INT,
        last_available_confirmed INT,
        last_available_deaths INT
    )
    """)
    print("Tabela 'casos_covid' pronta.")

# -------------------------
# CONEXÃO E INSERÇÃO
# -------------------------
try:
    conn = mysql.connector.connect(**db_config)
    if conn.is_connected():
        print("Conexão bem-sucedida com o MySQL!")
    cursor = conn.cursor()
    
    # cria tabela se não existir
    criar_tabela(cursor)

    # processa o CSV em chunks
    for i, chunk in enumerate(pd.read_csv(caminho_arquivo, chunksize=chunksize, low_memory=False)):
        chunk = limpar_chunk(chunk)
        
        # prepara dados para inserção
        dados = [
            (
                row['city'],
                str(row['city_ibge_code']),  # converte float para string
                row['state'],
                row['date'],
                row['population'],
                row['new_confirmed'],
                row['new_deaths'],
                row['last_available_confirmed'],
                row['last_available_deaths']
            )
            for _, row in chunk.iterrows()
        ]
        
        # insere em lote
        cursor.executemany("""
            INSERT INTO casos_covid
            (city, city_ibge_code, state, date, population, new_confirmed, new_deaths, last_available_confirmed, last_available_deaths)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, dados)
        conn.commit()
        print(f"Chunk {i+1} inserido, linhas: {len(dados)}")

    cursor.close()
    conn.close()
    print("Todos os dados inseridos com sucesso!")

except Error as err:
    print("Erro ao conectar ou inserir no MySQL:", err)
