import os
import pyodbc
import win32security
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

load_dotenv()

class DatabaseManager:

    def __init__(self):
        self.username = os.getenv('SECRET_DB_USERNAME')
        self.password = os.getenv('SECRET_DB_PASSWORD')
        self.domain = os.getenv('SECRET_DB_DOMAIN')
        self.server = os.getenv('SECRET_DB_SERVER')
        self.database = os.getenv('SECRET_DB_DATABASE')
        self.port = os.getenv('SECRET_DB_PORT', '1433')  # Porta padrão 1433 se não especificada
        self.conn = None

    def authenticate_user(self):
        try:
            token = win32security.LogonUser(
                self.username,
                self.domain,
                self.password,
                win32security.LOGON32_LOGON_NEW_CREDENTIALS,
                win32security.LOGON32_PROVIDER_DEFAULT
            )
            win32security.ImpersonateLoggedOnUser(token)
        except Exception as e:
            print(f"Erro ao autenticar o usuário: {e}")

    def connect_to_database(self):
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server},{self.port};"  # Adiciona a porta após o servidor
                f"DATABASE={self.database};"
                "Trusted_Connection=yes"  # Indica autenticação do Windows (SSPI)
            )
            self.conn = pyodbc.connect(conn_str)
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
    def get_connection_string(self):
        """
        Retorna a string de conexão para ser usada pelos processadores externos.
        
        Returns:
            str: String de conexão ODBC para SQL Server
        """
        if not self.server or not self.database or not self.port:
            raise ValueError("Configurações do banco de dados não encontradas. Verifique as variáveis de ambiente.")
        
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"  # Adiciona a porta
            f"DATABASE={self.database};"
            "Trusted_Connection=yes"
        )
    def get_sqlalchemy_connection_string(self):
        """
        Retorna a string de conexão para SQLAlchemy (caso seja necessário).
        
        Returns:
            str: String de conexão SQLAlchemy para SQL Server
        """
        if not self.server or not self.database or not self.port:
            raise ValueError("Configurações do banco de dados não encontradas. Verifique as variáveis de ambiente.")
        
        # Para SQLAlchemy com autenticação Windows
        return f"mssql+pyodbc://@{self.server}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    def execute_query(self, query, params=None):
        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if cursor.description:  # Verifica se a consulta retorna resultados
                    columns = [column[0] for column in cursor.description]
                    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    return result
                return []  # Retorna lista vazia para consultas sem resultados
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise
        except Exception as e:
            print(f"Erro ao executar a consulta: {e}")
            raise

    def execute_non_query(self, query, params=None):
        """
        Executa uma consulta SQL que não retorna dados (ex.: INSERT, UPDATE, DELETE).
        Faz commit das alterações e retorna o número de linhas afetadas.

        Args:
            query (str): A consulta SQL a ser executada.
            params (tuple): Os parâmetros a serem passados para a consulta (opcional).
            
        Returns:
            int: Número de linhas afetadas pela operação
        """
        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Capturar o número de linhas afetadas ANTES do commit
                rows_affected = cursor.rowcount
                self.conn.commit()
                
                return rows_affected
                
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise
        except Exception as e:
            print(f"Erro ao executar a consulta não retornável: {e}")
            raise

    def close_connection(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            win32security.RevertToSelf()

    def insert_data(self, table, data):
        """
        Insere dados em uma tabela específica e faz commit.

        Args:
            table (str): Nome da tabela onde os dados serão inseridos.
            data (dict): Dicionário contendo os dados a serem inseridos. As chaves devem corresponder aos nomes das colunas da tabela.
        """
        try:
            # Constrói a string de consulta SQL
            columns = ', '.join(data.keys())
            placeholders = ', '.join('?' for _ in data)
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

            # Executa a consulta
            with self.conn.cursor() as cursor:
                cursor.execute(query, tuple(data.values()))
                self.conn.commit()  # Efetua o commit das alterações
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise e
        except Exception as e:
            print(f"Erro ao inserir dados: {e}")
            raise e

    def insert_batch(self, table, data_list):
        """
        Insere múltiplos registros em uma tabela específica usando executemany para melhor performance.
        
        Args:
            table (str): Nome da tabela onde os dados serão inseridos (com schema, ex: 'SCHEMA.tabela').
            data_list (list): Lista de dicionários contendo os dados a serem inseridos. 
                            Todos os dicionários devem ter as mesmas chaves.
        
        Returns:
            int: Número de linhas inseridas.
        """
        if not data_list:
            return 0
            
        try:
            # Usar as chaves do primeiro dicionário como base
            columns = ', '.join(data_list[0].keys())
            placeholders = ', '.join('?' for _ in data_list[0])
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            
            # Preparar lista de valores para executemany
            values_list = [tuple(item.values()) for item in data_list]
            
            # Executar inserção em lote
            with self.conn.cursor() as cursor:
                cursor.executemany(query, values_list)
                rows_affected = cursor.rowcount
                self.conn.commit()
                return rows_affected
                
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server ao inserir em lote: {e}")
            print(f"SQL State: {sqlstate}")
            self.conn.rollback()
            raise e
        except Exception as e:
            print(f"Erro ao inserir dados em lote: {e}")
            self.conn.rollback()
            raise e

    def update_data(self, table, data, condition):
        """
        Atualiza dados em uma tabela específica baseado em uma condição.

        Args:
            table (str): Nome da tabela onde os dados serão atualizados.
            data (dict): Dicionário contendo os dados a serem atualizados. As chaves devem corresponder aos nomes das colunas da tabela.
            condition (str): Condição SQL para especificar quais registros devem ser atualizados.

        Exemplo:
            update_data('minha_tabela', {'nome_coluna': 'novo_valor'}, "id = 1")
        """
        try:
            # Constrói a string de consulta SQL para atualização
            set_clause = ', '.join(f"{key} = ?" for key in data.keys())
            query = f"UPDATE {table} SET {set_clause} WHERE {condition}"

            # Executa a consulta
            with self.conn.cursor() as cursor:
                cursor.execute(query, tuple(data.values()))
                self.conn.commit()  # Efetua o commit das alterações
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
        except Exception as e:
            print(f"Erro ao atualizar dados: {e}")

    def execute_procedure(self, procedure_name, params=None):
        try:
            with self.conn.cursor() as cursor:
                # Preparar a string de chamada da procedure com os parâmetros
                query = f"EXEC {procedure_name}"
                if params:
                    param_placeholders = ', '.join(['?'] * len(params))
                    query = f"EXEC {procedure_name} {param_placeholders}"

                    # Executar a procedure
                    cursor.execute(query, params)
                    self.conn.commit()  # Efetua o commit das alterações
                else:
                    cursor.execute(query)
                    self.conn.commit()  # Efetua o commit das alterações
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise
        except Exception as e:
            print(f"Erro ao executar a procedure: {e}")
            raise

    def select_data(self, table, columns, condition):
        """
        Seleciona dados de uma tabela específica com base em uma condição.

        Args:
            table (str): Nome da tabela de onde os dados serão selecionados.
            columns (list): Lista das colunas a serem selecionadas.
            condition (str): Condição SQL para especificar quais registros selecionar.

        Returns:
            list: Lista de dicionários contendo os registros selecionados.
        """
        try:
            query = f"SELECT {', '.join(columns)} FROM {table} WHERE {condition}"
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise e
        except Exception as e:
            print(f"Erro ao selecionar dados: {e}")
            raise e

    def delete_disparo(self, disparo_id):
        try:
            with self.conn.cursor() as cursor:
                # Deletar os anexos associados
                cursor.execute(
                    "DELETE FROM RE.anexos WHERE anexo_id IN (SELECT anexo_id FROM RE.resumo_envios WHERE pulse_id = ?) OR anexo_id IN (SELECT anexo_id FROM RE.email_envios WHERE pulse_id = ?)",
                    (disparo_id, disparo_id))

                # Encontrar IDs de resumo e email relacionados ao disparo
                cursor.execute("SELECT resumo_id FROM RE.resumo_envios WHERE pulse_id = ?", (disparo_id,))
                resumo_ids = [row[0] for row in cursor.fetchall()]

                cursor.execute("SELECT envio_id FROM RE.email_envios WHERE pulse_id = ?", (disparo_id,))
                email_ids = [row[0] for row in cursor.fetchall()]

                # Deletar de tabelas relacionadas ao resumo e email
                for resumo_id in resumo_ids:
                    cursor.execute("DELETE FROM RE.resumo_destinatarios WHERE resumo_id = ?", (resumo_id,))

                for email_id in email_ids:
                    cursor.execute("DELETE FROM RE.email_destinatarios WHERE envio_id = ?", (email_id,))

                # Deletar de 'resumo_envios' e 'email_envios'
                cursor.execute("DELETE FROM RE.resumo_envios WHERE pulse_id = ?", (disparo_id,))
                cursor.execute("DELETE FROM RE.email_envios WHERE pulse_id = ?", (disparo_id,))

                # Por fim, deletar da tabela 'disparos'
                cursor.execute("DELETE FROM RE.disparos WHERE disparo_id = ?", (disparo_id,))

                self.conn.commit()
                return True
        except pyodbc.Error as e:
            print(f"Erro de SQL Server: {e}")
            self.conn.rollback()  # Importante para desfazer alterações parciais
            return False
        except Exception as e:
            print(f"Erro geral: {e}")
            self.conn.rollback()  # Importante para desfazer alterações parciais
            return False

    def delete_powerbi(self, powerbi_id):
        try:
            with self.conn.cursor() as cursor:
                # Por fim, deletar da tabela 'powerbi_relatorios_log'
                cursor.execute("DELETE FROM DBA_CONTROLE.RE.powerbi_relatorios_log WHERE powerbi_id = ?", (powerbi_id,))
                # Por fim, deletar da tabela 'exportar_powerbi'
                cursor.execute("DELETE FROM DBA_CONTROLE.RE.powerbi_relatorios WHERE powerbi_id = ?", (powerbi_id,))

                self.conn.commit()
                return True
        except pyodbc.Error as e:
            print(f"Erro de SQL Server: {e}")
            self.conn.rollback()  # Importante para desfazer alterações parciais
            return False
        except Exception as e:
            print(f"Erro geral: {e}")
            self.conn.rollback()  # Importante para desfazer alterações parciais
            return False

    def get_last_insert_id(self):
        """
        Obtém o último ID inserido na conexão atual.

        Returns:
            int: O último ID inserido.
        """
        try:
            with self.conn.cursor() as cursor:
                # Consulta específica para obter o último ID inserido no SQL Server
                cursor.execute("SELECT @@IDENTITY AS last_insert_id")
                row = cursor.fetchone()
                if row:
                    last_insert_id = int(row[0])
                    return last_insert_id
                else:
                    raise ValueError("Não foi possível obter o último ID inserido.")
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise
        except Exception as e:
            print(f"Erro ao obter o último ID inserido: {e}")
            raise

    def record_exists(self, table, condition):
        """
        Verifica se um registro existe na tabela com base em uma condição.
        Args:
            table (str): Nome da tabela onde procurar o registro.
            condition (str): Condição SQL para especificar qual registro procurar.
        Returns:
            bool: True se o registro existir, False caso contrário.
        """
        try:
            query = f"SELECT COUNT(*) FROM {table} WHERE {condition}"
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                count = cursor.fetchone()[0]
                return count > 0
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise e
        except Exception as e:
            print(f"Erro ao verificar a existência do registro: {e}")
            raise e

    def execute_scalar(self, query, params=None):
        """
        Executa uma consulta que retorna um único valor.

        Args:
            query (str): A consulta SQL a ser executada.
            params (tuple): Os parâmetros a serem passados para a consulta.

        Returns:
            qualquer: O valor retornado pela consulta.
        """
        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                row = cursor.fetchone()
                return row[0] if row else None
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise e
        except Exception as e:
            print(f"Erro ao executar a consulta escalar: {e}")
            raise e

    def execute_query_single(self, query, params=None):
        """
        Executa uma consulta que retorna uma única linha (dict) ou None.

        Args:
            query (str): A consulta SQL a ser executada.
            params (tuple): Os parâmetros a serem passados para a consulta.

        Returns:
            dict: Retorna um dicionário {coluna: valor} com a linha encontrada,
                  ou None se não houver resultado.
        """
        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Tenta buscar apenas uma linha
                row = cursor.fetchone()
                if not row:
                    # Se não encontrou linha alguma, retorna None
                    return None

                # Monta a lista de colunas
                columns = [desc[0] for desc in cursor.description]

                # Retorna a linha em forma de dicionário
                return dict(zip(columns, row))

        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise e
        except Exception as e:
            print(f"Erro ao executar a consulta single: {e}")
            raise e

    def execute_procedure_indicador(self, procedure_name, params):
        try:
            with self.conn.cursor() as cursor:
                # Criar a string de chamada com o número correto de marcadores
                param_placeholders = ', '.join(['?'] * len(params))
                query = f"EXEC {procedure_name} {param_placeholders}"
                cursor.execute(query, params)
                # Capturar o resultado retornado, se houver
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    self.conn.commit()
                    return result if result else None
                else:
                    self.conn.commit()
                    return None
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            print(f"Erro de SQL Server: {e}")
            print(f"SQL State: {sqlstate}")
            raise
        except Exception as e:
            print(f"Erro ao executar a procedure: {e}")
            raise