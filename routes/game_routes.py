from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from services import game_service
import traceback
import requests
from config import Config

game_bp = Blueprint('games', __name__)

GENRE_TRANSLATIONS = {
    "Action": "Ação", "Indie": "Indie", "Adventure": "Aventura",
    "RPG": "RPG", "Strategy": "Estratégia", "Shooter": "Tiro",
    "Casual": "Casual", "Simulation": "Simulação", "Puzzle": "Puzzle",
    "Arcade": "Arcade", "Platformer": "Plataforma", "Racing": "Corrida",
    "Massively Multiplayer": "MMO", "Sports": "Esportes", "Fighting": "Luta",
    "Family": "Família", "Board Games": "Jogos de Tabuleiro", "Educational": "Educacional",
    "Card": "Cartas"
}

@game_bp.route('/data')
@jwt_required()
def get_all_game_data():
    """Retorna todos os dados de jogos, desejos e perfil."""
    try:
        data = game_service.get_all_game_data()
        
        # Recupera e adiciona os dados de background do perfil
        profile_data = data.get('perfil', {})
        data['perfil']['headerBackgroundUrl'] = profile_data.get('headerBackgroundUrl', '')
        data['perfil']['headerBackgroundName'] = profile_data.get('headerBackgroundName', '')
        
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "Não foi possível obter os dados.", "detalhes_tecnicos": str(e)}), 500

    try:
        url = f"https://api.rawg.io/api/games?key={Config.RAWG_API_KEY}&search={query}&page_size=5"
        response = requests.get(url)
        response.raise_for_status()
        rawg_data = response.json()

        results = []
        for game in rawg_data.get('results', []):
            genres_pt = [GENRE_TRANSLATIONS.get(g['name'], g['name']) for g in game.get('genres', [])]
            
            game_tags = game.get('tags') or []
            
            for tag in game_tags:
                if tag.get('language') == 'eng' and tag.get('slug') == 'souls-like':
                    soulslike_tag_name = tag.get('name')
                    if soulslike_tag_name and soulslike_tag_name not in genres_pt:
                        genres_pt.append("Soulslike")
                    break

            release_date = game.get('released')
            
            results.append({
                'id': game.get('id'),
                'name': game.get('name'),
                'background_image': game.get('background_image'),
                'released_for_input': release_date,
                'styles': ', '.join(genres_pt)
            })
            
        return jsonify(results)

    except requests.exceptions.RequestException as e:
        print(f"!!! ERRO DE COMUNICAÇÃO COM A API EXTERNA: {e}")
        return jsonify({"error": "Falha ao se comunicar com a API da RAWG."}), 503

    except Exception as e:
        print(f"!!! ERRO INESPERADO NA ROTA /search-external: {e}")
        traceback.print_exc()
        return jsonify({"error": "Ocorreu um erro interno no servidor. Verifique os logs."}), 500

@game_bp.route('/data')
@jwt_required()
def get_all_game_data():
    """Retorna todos os dados de jogos, desejos e perfil."""
    try:
        data = game_service.get_all_game_data()
        
        # Recupera e adiciona os dados de background do perfil
        profile_data = data.get('perfil', {})
        data['perfil']['headerBackgroundUrl'] = profile_data.get('headerBackgroundUrl', '')
        data['perfil']['headerBackgroundName'] = profile_data.get('headerBackgroundName', '')
        
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

@game_bp.route('/wishlist/purchase/<string:item_name>', methods=['POST'])
@jwt_required()
def purchase_wish_item(item_name):
    """Marca um item da lista de desejos como 'Comprado'."""
    try:
        result = game_service.purchase_wish_item_in_sheet(item_name)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": "Erro ao marcar item como comprado.", "detalhes_tecnicos": str(e)}), 500
        
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

@game_bp.route('/public-profile')
def get_public_profile():
    """Retorna dados públicos do perfil para visualização compartilhável."""
    try:
        data = game_service.get_public_profile_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "Não foi possível obter os dados públicos."}), 500
