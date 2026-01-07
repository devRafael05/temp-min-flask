from flask import Flask, jsonify
from helpers.DatabaseManager import DatabaseManager
import traceback

app = Flask(__name__)

@app.route('/')
def index():
    """Rota principal - Health Check"""
    return jsonify({
        "status": "ok",
        "message": "Flask está rodando na VM!"
    })

@app.route('/test-db')
def test_db():
    """Rota para testar conexão com o banco de dados"""
    db = None
    try:
        db = DatabaseManager()
        db.authenticate_user()
        db.connect_to_database()
        
        # Teste simples: buscar a data atual do servidor
        result = db.execute_query("SELECT GETDATE() AS data_servidor")
        
        return jsonify({
            "status": "success",
            "message": "Conexão com banco de dados OK!",
            "server_time": str(result[0]['data_servidor']) if result else None
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }), 500
        
    finally:
        if db:
            db.close_connection()

@app.route('/test-query')
def test_query():
    """Rota para testar uma query personalizada"""
    db = None
    try:
        db = DatabaseManager()
        db.authenticate_user()
        db.connect_to_database()
        
        # Exemplo: listar tabelas do banco
        result = db.execute_query("""
            SELECT TOP 10 
                TABLE_SCHEMA, 
                TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            ORDER BY TABLE_NAME
        """)
        
        return jsonify({
            "status": "success",
            "tables": result
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }), 500
        
    finally:
        if db:
            db.close_connection()

if __name__ == '__main__':
    print("=" * 50)
    print("🚀 Iniciando Flask Test App")
    print("=" * 50)
    print("Endpoints disponíveis:")
    print("  - GET /          -> Health Check")
    print("  - GET /test-db   -> Testar conexão com DB")
    print("  - GET /test-query -> Listar tabelas do banco")
    print("=" * 50)
    
    app.run(host='0.0.0.0', port=5000, debug=True)
