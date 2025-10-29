# Arquivo principal da API Flask em ~/projects/ProjetoAPI/venv/Flask/app.py
import sys
import os
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from flask import Flask, jsonify, request
from Database.db_manager import DatabaseManager

app = Flask(__name__)

@app.route('/')
def home():
    """Página inicial da API"""
    return jsonify({
        "message": "Bem-vindo à API de Dados Consolidados",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/data', methods=['GET'])
def get_all_data():
    """
    Retorna todos os dados consolidados
    Query parameters:
    - page: página (padrão: 1)
    - per_page: registros por página (padrão: 50)
    """
    try:
        with DatabaseManager() as db:
            page = request.args.get('page', 1, type=int)
            per_page = request.args.get('per_page', 50, type=int)
            offset = (page - 1) * per_page
            
            result = db.get_all_data(limit=per_page, offset=offset)
            
            # Formatar resposta conforme padrão
            if "success" in result and result["success"]:
                return jsonify({
                    "success": True,
                    "data": result["data"],
                    "pagination": {
                        "total": result["pagination"]["total"],
                        "page": page,
                        "per_page": per_page
                    },
                    "metadata": {
                        "colunas": [col["name"] for col in db.get_columns_info()["columns"]],
                        "timestamp": datetime.now().isoformat()
                    }
                })
            else:
                return jsonify(result), 500
                
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Erro interno: {str(e)}"
        }), 500

@app.route('/api/data/<int:id>', methods=['GET'])
def get_data_by_id(id):
    """Retorna registro específico"""
    try:
        with DatabaseManager() as db:
            result = db.get_data_by_id(id)
            
            if "success" in result and result["success"]:
                return jsonify({
                    "success": True,
                    "data": result["data"]
                })
            else:
                return jsonify(result), 404
                
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Erro interno: {str(e)}"
        }), 500

@app.route('/api/estatisticas', methods=['GET'])
def get_estatisticas():
    """Estatísticas dos dados"""
    try:
        with DatabaseManager() as db:
            result = db.get_aggregated_data()
            
            if "success" in result and result["success"]:
                return jsonify({
                    "success": True,
                    "estatisticas": result["stats"],
                    "timestamp": datetime.now().isoformat()
                })
            else:
                return jsonify(result), 500
                
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Erro interno: {str(e)}"
        }), 500

@app.route('/api/colunas', methods=['GET'])
def get_colunas():
    """Lista de colunas disponíveis"""
    try:
        with DatabaseManager() as db:
            result = db.get_columns_info()
            
            if "success" in result and result["success"]:
                return jsonify({
                    "success": True,
                    "colunas": [col["name"] for col in result["columns"]],
                    "detalhes": result["columns"],
                    "total_colunas": result["total_columns"]
                })
            else:
                return jsonify(result), 500
                
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Erro interno: {str(e)}"
        }), 500

@app.route('/api/filtrar', methods=['POST'])
def filtrar_dados():
    """
    Filtros personalizados
    Body (JSON):
    {
        "coluna": "datavenda",
        "operador": "between",
        "valor": ["2023-01-01", "2023-12-31"]
    }
    Ou múltiplos filtros:
    {
        "filtros": [
            {
                "coluna": "categoria",
                "operador": "igual",
                "valor": "Eletrônicos"
            },
            {
                "coluna": "precototal",
                "operador": "maior",
                "valor": 1000
            }
        ]
    }
    """
    try:
        filters = request.get_json()
        if not filters:
            return jsonify({
                "success": False,
                "error": "Corpo da requisição vazio"
            }), 400
        
        with DatabaseManager() as db:
            # Converter formato dos filtros
            formatted_filters = self._convert_filters(filters)
            
            result = db.get_filtered_data(
                filters=formatted_filters,
                limit=request.args.get('per_page', 50, type=int),
                offset=0
            )
            
            if "success" in result and result["success"]:
                return jsonify({
                    "success": True,
                    "data": result["data"],
                    "filtros_aplicados": filters,
                    "pagination": {
                        "total": result["pagination"]["total"],
                        "page": 1,
                        "per_page": result["pagination"]["limit"]
                    }
                })
            else:
                return jsonify(result), 500
                
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Erro interno: {str(e)}"
        }), 500

@app.route('/api/metadados', methods=['GET'])
def get_metadados():
    """Metadados das fontes"""
    try:
        with DatabaseManager() as db:
            # Obter estatísticas sobre origens dos dados
            query = """
            SELECT 
                file_name,
                sheet_name,
                COUNT(*) as total_registros,
                MIN(datavenda) as data_inicio,
                MAX(datavenda) as data_fim,
                SUM(precototal) as receita_total
            FROM dados_consolidados 
            GROUP BY file_name, sheet_name
            ORDER BY file_name, sheet_name
            """
            
            result = db.execute_query(query)
            
            if result:
                return jsonify({
                    "success": True,
                    "metadados": {
                        "fontes": result,
                        "total_fontes": len(result),
                        "timestamp": datetime.now().isoformat()
                    }
                })
            else:
                return jsonify({
                    "success": False,
                    "error": "Erro ao obter metadados"
                }), 500
                
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Erro interno: {str(e)}"
        }), 500

def _convert_filters(self, filters):
    """Converte filtros do formato da API para formato do DatabaseManager"""
    formatted = {}
    
    if 'filtros' in filters:
        # Múltiplos filtros
        for filtro in filters['filtros']:
            coluna = filtro['coluna']
            operador = filtro['operador']
            valor = filtro['valor']
            
            if operador == 'between' and len(valor) == 2:
                formatted[coluna] = {'start': valor[0], 'end': valor[1]}
            elif operador == 'igual':
                formatted[coluna] = valor
            elif operador == 'maior':
                formatted[f"{coluna}_min"] = valor
            elif operador == 'menor':
                formatted[f"{coluna}_max"] = valor
            elif operador == 'contem':
                formatted[coluna] = valor
    else:
        # Filtro único
        coluna = filters['coluna']
        operador = filters['operador']
        valor = filters['valor']
        
        if operador == 'between' and len(valor) == 2:
            formatted[coluna] = {'start': valor[0], 'end': valor[1]}
        else:
            formatted[coluna] = valor
    
    return formatted

if __name__ == '__main__':
    print("=" * 50)
    print(" API Flask - Dados Consolidados")
    print("=" * 50)
    print("📡 Endpoints disponíveis:")
    print("   GET  /api/data          - Todos os dados consolidados")
    print("   GET  /api/data/{id}     - Registro específico")
    print("   GET  /api/estatisticas  - Estatísticas dos dados")
    print("   GET  /api/colunas       - Lista de colunas disponíveis")
    print("   POST /api/filtrar       - Filtros personalizados")
    print("   GET  /api/metadados     - Metadados das fontes")
    print("=" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000)