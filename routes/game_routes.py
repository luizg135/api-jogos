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

@game_bp.route('/add', methods=['POST'])
@jwt_required()
def add_new_item():
    """Adiciona um novo jogo ou item de desejo."""
    try:
        data = request.json
        list_type = data.get('list_type')
        item_data = data.get('item_data')
        
        if list_type == 'games':
            result = game_service.add_game_to_sheet(item_data)
        elif list_type == 'wishlist':
            result = game_service.add_wish_to_sheet(item_data)
        else:
            return jsonify({"success": False, "message": "Tipo de lista inválido."}), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": "Erro ao adicionar item.", "detalhes_tecnicos": str(e)}), 500

@game_bp.route('/edit', methods=['PUT'])
@jwt_required()
def edit_item():
    """Edita um jogo ou item de desejo existente."""
    try:
        data = request.json
        list_type = data.get('list_type')
        item_name = data.get('item_name')
        updated_data = data.get('updated_data')
        
        if list_type == 'games':
            result = game_service.update_game_in_sheet(item_name, updated_data)
        elif list_type == 'wishlist':
            result = game_service.update_wish_in_sheet(item_name, updated_data)
        else:
            return jsonify({"success": False, "message": "Tipo de lista inválido."}), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": "Erro ao editar item.", "detalhes_tecnicos": str(e)}), 500

@game_bp.route('/delete/<list_type>/<string:item_name>', methods=['DELETE'])
@jwt_required()
def delete_item(list_type, item_name):
    """Deleta um jogo ou item de desejo."""
    try:
        if list_type == 'games':
            result = game_service.delete_game_from_sheet(item_name)
        elif list_type == 'wishlist':
            result = game_service.delete_wish_from_sheet(item_name)
        else:
            return jsonify({"success": False, "message": "Tipo de lista inválido."}), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": "Erro ao deletar item.", "detalhes_tecnicos": str(e)}), 500
