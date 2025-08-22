from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from config import Config
import os

# Importa os blueprints
from routes.game_routes import game_bp
from routes.auth_routes import auth_bp
from routes.notification_routes import notification_bp

app = Flask(__name__)

# Configurações do Flask e JWT a partir da classe Config
app.config["JWT_SECRET_KEY"] = Config.JWT_SECRET_KEY
jwt = JWTManager(app)

# Configurações para o CORS (para o frontend funcionar)
# Permite o acesso do domínio do Netlify
CORS(app, resources={r"/api/*": {"origins": "https://perfil-gamer.netlify.app"}})

# Registra os blueprints
app.register_blueprint(auth_bp, url_prefix='/api')
app.register_blueprint(game_bp, url_prefix='/api/games')
app.register_blueprint(notification_bp, url_prefix='/api/notifications')

@app.route('/')
def index():
    return "API de Jogos está no ar!"

if __name__ == '__main__':
    # A porta padrão do Render para Gunicorn é 8000
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=True)
