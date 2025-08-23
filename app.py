from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from config import Config
import os

# Importa os blueprints
from routes.game_routes import game_bp
from routes.auth_routes import auth_bp

app = Flask(__name__)

# Configurações do Flask e JWT a partir da classe Config
app.config["JWT_SECRET_KEY"] = Config.JWT_SECRET_KEY
jwt = JWTManager(app)

# --- MODIFICAÇÃO AQUI: Configurações para o CORS ---
# Permite requisições de todas as origens para todos os endpoints da API.
# Em produção, é mais seguro especificar as origens permitidas (ex: origins=["https://perfil-gamer.netlify.app"])
# No entanto, para fins de desenvolvimento e flexibilidade, '*' é comum.
# Para o seu caso, vamos especificar a origem do seu frontend para maior segurança.
CORS(app, resources={r"/api/*": {"origins": "https://perfil-gamer.netlify.app"}})
# --- FIM DA MODIFICAÇÃO ---

# Registra os blueprints
app.register_blueprint(auth_bp, url_prefix='/api')
app.register_blueprint(game_bp, url_prefix='/api/games')

@app.route('/')
def index():
    return "API de Jogos está no ar!"

if __name__ == '__main__':
    # A porta padrão do Render para Gunicorn é 8000
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=True)
