from flask import Blueprint, request, jsonify
from werkzeug.security import check_password_hash
from flask_jwt_extended import create_access_token
from config import Config

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['POST'])
def login():
    """
    Recebe username e senha, verifica as credenciais e retorna um token JWT.
    """
    username = request.json.get('username', None)
    password = request.json.get('password', None)

    # Verifica se as variáveis de ambiente foram configuradas
    if not Config.ADMIN_USERNAME or not Config.ADMIN_PASSWORD_HASH:
        return jsonify({"msg": "Servidor não configurado para autenticação."}), 500

    # Compara o usuário fornecido e a senha (usando o hash seguro)
    if username == Config.ADMIN_USERNAME and check_password_hash(Config.ADMIN_PASSWORD_HASH, password):
        access_token = create_access_token(identity=username)
        return jsonify(access_token=access_token)
    
    # Se as credenciais estiverem erradas
    return jsonify({"msg": "Usuário ou senha inválidos"}), 401
