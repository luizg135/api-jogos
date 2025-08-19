from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from services import game_service
import traceback

game_bp = Blueprint('games', __name__)

@game_bp.route('/data')
@jwt_required()
def get_all_game_data():
    """Retorna todos os dados de jogos, desejos e perfil."""
    try:
        data = game_service.get_all_game_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "Não foi possível obter os dados.", "detalhes_tecnicos": str(e)}), 500

# NOVO: Rota para editar o perfil
@game_bp.route('/profile/edit', methods=['PUT'])
@jwt_required()
def edit_profile():
    """Edita os dados do perfil."""
    try:
        data = request.json
        result = game_service.update_profile_in_sheet(data)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": "Erro ao editar perfil.", "detalhes_tecnicos": str(e)}), 500

# As outras rotas (add, edit, delete de jogos) permanecem as mesmas.
@game_bp.route('/add', methods=['POST']) # ... (sem alterações)
@game_bp.route('/edit', methods=['PUT']) # ... (sem alterações)
@game_bp.route('/delete/<list_type>/<string:item_name>', methods=['DELETE']) # ... (sem alterações)
