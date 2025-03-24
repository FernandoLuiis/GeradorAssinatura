import os
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuração do banco de dados
DB_CONFIG = {
    "dbname": "assinatura_email",
    "user": "user",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

EXCEL_FOLDER_PATH = r"C:\GitHub\AssinaturaEmail\assinatura-email\backend\pandas\funcionarios"
EXCEL_FILE_NAME = "atualiza.xlsx"  # Nome do arquivo
EXCEL_FILE_PATH = os.path.join(EXCEL_FOLDER_PATH, EXCEL_FILE_NAME)

# Criar conexão com o banco
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Criar engine SQLAlchemy para inserções mais fáceis
engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")

# Função para verificar se a tabela existe
def check_table_exists(table_name):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM assinatura_email 
                );
            """, (table_name,))
            return cur.fetchone()[0]

# Função para processar o Excel e atualizar o banco
def update_database_from_excel():
    try:
        print(f"📂 Lendo arquivo: {EXCEL_FILE_PATH}")

        # Aguarda um tempo para garantir que o Excel fechou o arquivo
        time.sleep(2)

        # Lendo o arquivo Excel
        df = pd.read_excel(EXCEL_FILE_PATH)

        # Verifica se o DataFrame tem dados
        if df.empty:
            print("⚠️ Arquivo Excel está vazio. Nenhuma atualização foi feita.")
            return

        print(f"🔍 Colunas encontradas: {df.columns.tolist()}")

        # Remover colunas duplicadas (ex: 'cod_cracha' e 'cod_cracha.1')
        df = df.loc[:, ~df.columns.duplicated()]  # Remove colunas duplicadas

        # Ajustar as colunas para garantir a correspondência com o banco de dados
        colunas_esperadas = ["cod_cracha", "nm_funcionario", "cargo", "email"]
        df = df.iloc[:, :len(colunas_esperadas)]  # Mantém apenas as primeiras colunas esperadas
        df.columns = colunas_esperadas  # Renomeia as colunas corretamente

        # Limpar valores indesejados (como NaN ou células vazias)
        df.dropna(subset=["cod_cracha", "nm_funcionario", "cargo", "email"], how="any", inplace=True)

        # Certificar que 'cod_cracha' é lido como string
        df['cod_cracha'] = df['cod_cracha'].astype(str).str.strip()

        # Verifique se há valores inesperados ou NaN em cod_cracha após a conversão
        df = df[df['cod_cracha'].notna()]  # Remove qualquer linha com 'cod_cracha' vazio ou NaN

        # Remove linhas que estejam completamente vazias
        df.dropna(how="all", inplace=True)

        # Verificar se a tabela 'assinatura_email' existe no banco de dados
        if not check_table_exists('assinatura_email'):
            print("⚠️ A tabela 'assinatura_email' não existe no banco de dados.")
            return

        # Realizando inserções e atualizações na tabela
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    # Corrigir o SQL para garantir que o conflito será tratado corretamente
                    cur.execute(""" 
                        INSERT INTO assinatura_email (cod_cracha, nm_funcionario, cargo, email)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (cod_cracha) 
                        DO UPDATE SET 
                            nm_funcionario = EXCLUDED.nm_funcionario,
                            cargo = EXCLUDED.cargo,
                            email = EXCLUDED.email;
                    """, (row.cod_cracha, row.nm_funcionario, row.cargo, row.email))

        print("✅ Atualização concluída com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao atualizar o banco: {e}")

# Classe para monitorar alterações no Excel
class ExcelFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        # Garante que a alteração ocorreu no arquivo correto
        if event.src_path.endswith(".xlsx") and os.path.basename(event.src_path) == EXCEL_FILE_NAME:
            print("📢 Detecção de modificação no arquivo Excel.")
            update_database_from_excel()

# Iniciar monitoramento da pasta (não do arquivo específico)
def start_monitoring():
    print(f"👀 Monitorando alterações na pasta: {EXCEL_FOLDER_PATH}")
    
    event_handler = ExcelFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=EXCEL_FOLDER_PATH, recursive=False)  # Monitorar a PASTA
    observer.start()

    try:
        while True:
            time.sleep(5)  # Verifica a cada 5 segundos
    except KeyboardInterrupt:
        observer.stop()
        print("🛑 Monitoramento encerrado.")

    observer.join()

if __name__ == "__main__":
    start_monitoring()
