# db_manager.py em ~/projects/ProjetoAPI/venv/Database


import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import List, Dict, Any, Optional

# Configuração do logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

'''
Classe para gerenciar a conexão com o banco de dados PostgreSQL
Métodos para conectar, desconectar e executar consultas para API
'''

class DatabaseManager:
    def __init__(self, db_url: str = None):
        if db_url is None:
            self.db_url = "postgresql://api_user:api_password123@localhost:5432/projetoapi"
        else:
            self.db_url = db_url
            
        self.engine = None
        self.connection = None
        self.is_connected = False
    
    def connect(self) -> bool:
        """
        Estabelece conexão com o banco de dados
        
        Returns:
            bool: True se conexão foi bem sucedida, False caso contrário
        """
        try:
            self.engine = create_engine(self.db_url)
            self.connection = self.engine.connect()
            self.is_connected = True
            logger.info(" Conexão com o banco de dados estabelecida com sucesso!")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f" Erro ao conectar ao banco de dados: {e}")
            self.is_connected = False
            return False
    
    def disconnect(self):
        """
        Fecha a conexão com o banco de dados
        """
        try:
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()
                
            self.is_connected = False
            logger.info(" Conexão com o banco de dados fechada.")
            
        except SQLAlchemyError as e:
            logger.error(f" Erro ao desconectar do banco de dados: {e}")
    
    def test_connection(self) -> bool:
        """
        Testa se a conexão está ativa
        
        Returns:
            bool: True se conexão está ativa
        """
        if not self.is_connected:
            return self.connect()
            
        try:
            result = self.connection.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            logger.warning(" Conexão perdida, tentando reconectar...")
            return self.connect()
    
    def execute_query(self, query: str, params: dict = None) -> Optional[List[Dict]]:
        """
        Executa uma consulta SQL genérica
        """
        if not self.test_connection():
            return None
            
        try:
            if params:
                result = self.connection.execute(text(query), params)
            else:
                result = self.connection.execute(text(query))
            
            if query.strip().upper().startswith('SELECT'):
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result]
            else:
                self.connection.commit()
                return None
                
        except SQLAlchemyError as e:
            logger.error(f" Erro ao executar consulta: {e}")
            self.connection.rollback()
            return None

    # ========== MÉTODOS PARA API ==========

    def get_all_data(self, table: str = "dados_consolidados", limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        Obtém todos os dados da tabela (para endpoint GET /api/data)
        
        Args:
            table: Nome da tabela
            limit: Limite de registros
            offset: Offset para paginação
            
        Returns:
            Dict com dados e metadados
        """
        if not self.test_connection():
            return {"error": "Não foi possível conectar ao banco de dados"}
        
        try:
            # Query principal com paginação
            query = f"""
            SELECT * FROM {table} 
            ORDER BY id 
            LIMIT {limit} OFFSET {offset}
            """
            
            # Contar total de registros
            count_query = f"SELECT COUNT(*) as total FROM {table}"
            
            # Executar queries
            data_result = self.connection.execute(text(query))
            count_result = self.connection.execute(text(count_query))
            
            # Processar resultados
            columns = data_result.keys()
            data = [dict(zip(columns, row)) for row in data_result]
            total = count_result.scalar()
            
            return {
                "success": True,
                "data": data,
                "pagination": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "has_next": (offset + limit) < total,
                    "has_prev": offset > 0
                }
            }
            
        except SQLAlchemyError as e:
            logger.error(f" Erro ao obter dados: {e}")
            return {"error": f"Erro ao obter dados: {str(e)}"}

    def get_data_by_id(self, record_id: int, table: str = "dados_consolidados") -> Dict[str, Any]:
        """
        Obtém um registro específico por ID (para endpoint GET /api/data/{id})
        
        Args:
            record_id: ID do registro
            table: Nome da tabela
            
        Returns:
            Dict com os dados do registro
        """
        if not self.test_connection():
            return {"error": "Não foi possível conectar ao banco de dados"}
        
        try:
            query = f"SELECT * FROM {table} WHERE id = :id"
            result = self.connection.execute(text(query), {"id": record_id})
            
            columns = result.keys()
            row = result.fetchone()
            
            if row:
                return {
                    "success": True,
                    "data": dict(zip(columns, row))
                }
            else:
                return {
                    "success": False,
                    "error": f"Registro com ID {record_id} não encontrado"
                }
                
        except SQLAlchemyError as e:
            logger.error(f" Erro ao obter registro: {e}")
            return {"error": f"Erro ao obter registro: {str(e)}"}

    def get_filtered_data(self, filters: Dict[str, Any], table: str = "dados_consolidados", 
                         limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        Obtém dados com filtros (para endpoint POST /api/data/filter)
        
        Args:
            filters: Dicionário com filtros
            table: Nome da tabela
            limit: Limite de registros
            offset: Offset para paginação
            
        Returns:
            Dict com dados filtrados
        """
        if not self.test_connection():
            return {"error": "Não foi possível conectar ao banco de dados"}
        
        try:
            where_conditions = []
            params = {}
            
            # Construir condições WHERE baseadas nos filtros
            for key, value in filters.items():
                if key in ['idvenda', 'quantidade']:
                    where_conditions.append(f"{key} = :{key}")
                    params[key] = value
                elif key in ['produto', 'categoria', 'cliente', 'marca']:
                    where_conditions.append(f"{key} ILIKE :{key}")
                    params[key] = f"%{value}%"
                elif key == 'datavenda':
                    if isinstance(value, dict):
                        if 'start' in value and 'end' in value:
                            where_conditions.append(f"{key} BETWEEN :start_date AND :end_date")
                            params['start_date'] = value['start']
                            params['end_date'] = value['end']
                    else:
                        where_conditions.append(f"{key} = :{key}")
                        params[key] = value
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            # Query principal
            query = f"""
            SELECT * FROM {table} 
            WHERE {where_clause}
            ORDER BY id 
            LIMIT {limit} OFFSET {offset}
            """
            
            # Query para contar total
            count_query = f"SELECT COUNT(*) as total FROM {table} WHERE {where_clause}"
            
            # Executar queries
            data_result = self.connection.execute(text(query), params)
            count_result = self.connection.execute(text(count_query), params)
            
            # Processar resultados
            columns = data_result.keys()
            data = [dict(zip(columns, row)) for row in data_result]
            total = count_result.scalar()
            
            return {
                "success": True,
                "data": data,
                "filters": filters,
                "pagination": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "has_next": (offset + limit) < total,
                    "has_prev": offset > 0
                }
            }
            
        except SQLAlchemyError as e:
            logger.error(f" Erro ao filtrar dados: {e}")
            return {"error": f"Erro ao filtrar dados: {str(e)}"}

    def get_aggregated_data(self, table: str = "dados_consolidados") -> Dict[str, Any]:
        """
        Obtém dados agregados para dashboards (para endpoint GET /api/data/stats)
        
        Args:
            table: Nome da tabela
            
        Returns:
            Dict com estatísticas agregadas
        """
        if not self.test_connection():
            return {"error": "Não foi possível conectar ao banco de dados"}
        
        try:
            # Estatísticas básicas
            stats_queries = {
                "total_vendas": "SELECT COUNT(*) as total FROM {table}",
                "total_quantidade": "SELECT SUM(quantidade) as total FROM {table}",
                "total_receita": "SELECT SUM(precototal) as total FROM {table}",
                "media_preco": "SELECT AVG(precototal) as media FROM {table}",
                "vendas_por_categoria": """
                    SELECT categoria, COUNT(*) as quantidade, SUM(precototal) as receita
                    FROM {table} 
                    WHERE categoria IS NOT NULL 
                    GROUP BY categoria 
                    ORDER BY receita DESC
                """,
                "vendas_por_mes": """
                    SELECT DATE_TRUNC('month', datavenda) as mes, 
                           COUNT(*) as quantidade_vendas, 
                           SUM(precototal) as receita_total
                    FROM {table} 
                    WHERE datavenda IS NOT NULL 
                    GROUP BY mes 
                    ORDER BY mes
                """,
                "top_produtos": """
                    SELECT produto, COUNT(*) as quantidade_vendas, SUM(precototal) as receita
                    FROM {table} 
                    WHERE produto IS NOT NULL 
                    GROUP BY produto 
                    ORDER BY receita DESC 
                    LIMIT 10
                """,
                "top_clientes": """
                    SELECT cliente, COUNT(*) as quantidade_compras, SUM(precototal) as total_gasto
                    FROM {table} 
                    WHERE cliente IS NOT NULL 
                    GROUP BY cliente 
                    ORDER BY total_gasto DESC 
                    LIMIT 10
                """
            }
            
            results = {}
            for key, query_template in stats_queries.items():
                query = query_template.format(table=table)
                result = self.connection.execute(text(query))
                
                if key in ['vendas_por_categoria', 'vendas_por_mes', 'top_produtos', 'top_clientes']:
                    columns = result.keys()
                    results[key] = [dict(zip(columns, row)) for row in result]
                else:
                    row = result.fetchone()
                    results[key] = row[0] if row else 0
            
            return {
                "success": True,
                "stats": results,
                "timestamp": pd.Timestamp.now().isoformat()
            }
            
        except SQLAlchemyError as e:
            logger.error(f" Erro ao obter estatísticas: {e}")
            return {"error": f"Erro ao obter estatísticas: {str(e)}"}

    def get_columns_info(self, table: str = "dados_consolidados") -> Dict[str, Any]:
        """
        Obtém informações sobre as colunas da tabela (para endpoint GET /api/data/columns)
        
        Args:
            table: Nome da tabela
            
        Returns:
            Dict com informações das colunas
        """
        if not self.test_connection():
            return {"error": "Não foi possível conectar ao banco de dados"}
        
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table)
            
            column_info = []
            for col in columns:
                column_info.append({
                    "name": col['name'],
                    "type": str(col['type']),
                    "nullable": col['nullable'],
                    "primary_key": col.get('primary_key', False),
                    "description": self._get_column_description(col['name'])
                })
            
            return {
                "success": True,
                "table": table,
                "columns": column_info,
                "total_columns": len(column_info)
            }
            
        except SQLAlchemyError as e:
            logger.error(f" Erro ao obter informações das colunas: {e}")
            return {"error": f"Erro ao obter informações das colunas: {str(e)}"}

    def _get_column_description(self, column_name: str) -> str:
        """
        Retorna descrição amigável para as colunas
        
        Args:
            column_name: Nome da coluna
            
        Returns:
            Descrição da coluna
        """
        descriptions = {
            "id": "Identificador único do registro",
            "idvenda": "ID único da venda",
            "datavenda": "Data em que a venda foi realizada",
            "produto": "Nome do produto vendido",
            "categoria": "Categoria do produto",
            "quantidade": "Quantidade de itens vendidos",
            "precounitario": "Preço unitário do produto",
            "precototal": "Valor total da venda",
            "tipopagamento": "Forma de pagamento utilizada",
            "cliente": "Nome do cliente",
            "emailcliente": "E-mail do cliente",
            "marca": "Marca do produto",
            "sheet_name": "Nome da planilha de origem (para Excel)",
            "file_name": "Nome do arquivo de origem",
            "data_importacao": "Data de importação do registro"
        }
        
        return descriptions.get(column_name, "Coluna sem descrição definida")

    def search_data(self, search_term: str, table: str = "dados_consolidados", 
                   limit: int = 50) -> Dict[str, Any]:
        """
        Busca dados por termo de pesquisa (para endpoint GET /api/data/search)
        
        Args:
            search_term: Termo para busca
            table: Nome da tabela
            limit: Limite de resultados
            
        Returns:
            Dict com resultados da busca
        """
        if not self.test_connection():
            return {"error": "Não foi possível conectar ao banco de dados"}
        
        try:
            # Colunas onde fazer a busca
            search_columns = ['produto', 'categoria', 'cliente', 'marca', 'tipopagamento']
            
            conditions = []
            params = {"search_term": f"%{search_term}%"}
            
            for col in search_columns:
                conditions.append(f"{col} ILIKE :search_term")
            
            where_clause = " OR ".join(conditions)
            
            query = f"""
            SELECT * FROM {table} 
            WHERE {where_clause}
            ORDER BY id 
            LIMIT {limit}
            """
            
            result = self.connection.execute(text(query), params)
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result]
            
            return {
                "success": True,
                "search_term": search_term,
                "results": data,
                "total_results": len(data),
                "searched_columns": search_columns
            }
            
        except SQLAlchemyError as e:
            logger.error(f" Erro na busca: {e}")
            return {"error": f"Erro na busca: {str(e)}"}

    def __enter__(self):
        """Suporte para context manager"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Suporte para context manager"""
        self.disconnect()