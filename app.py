from flask import Flask, jsonify
from flask_jwt_extended import JWTManager
from datetime import timedelta
from config import Config
from routes.auth_routes import auth_bp
from routes.game_routes import game_bp
from flask_cors import CORS # Importe Flask-CORS

app = Flask(__name__)

# Configurações do Flask
app.config.from_object(Config)

# Configuração do Flask-CORS
# Aplica CORS a todas as rotas da aplicação Flask.
# Para produção, é recomendado especificar as origens permitidas.
# Exemplo para permitir apenas o seu frontend do Netlify:
CORS(app, origins="https://savepoint-hub.netlify.app")
# Se precisar de múltiplas origens (ex: local e Netlify):
# CORS(app, origins=["https://savepoint-hub.netlify.app", "http://localhost:8000"])


# Configuração do JWT
jwt = JWTManager(app)

# Registro dos Blueprints
app.register_blueprint(auth_bp, url_prefix='/api/auth')
app.register_blueprint(game_bp, url_prefix='/api/games')

@app.route('/')
def home():
    return jsonify(message="Bem-vindo à API de Jogos!")

if __name__ == '__main__':
    app.run(debug=True)
