from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from services import game_service
import traceback
import requests
from config import Config
from services.game_service import GENRE_TRANSLATIONS

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

@game_bp.route('/search-external', methods=['GET'])
@jwt_required()
def search_external_games():
    query = request.args.get('query', '')
    if not query or len(query) < 3:
        return jsonify({"error": "A busca deve ter pelo menos 3 caracteres."}), 400

    if not Config.RAWG_API_KEY:
        return jsonify({"error": "Chave da API externa não configurada no servidor."}), 500

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
def get_dashboard_data():
    """Retorna todos os dados de jogos, desejos e perfil para o dashboard."""
    try:
        data = game_service.get_all_game_data()
        
        profile_data = data.get('perfil', {})
        data['perfil']['headerBackgroundUrl'] = profile_data.get('headerBackgroundUrl', '')
        data['perfil']['headerBackgroundName'] = profile_data.get('headerBackgroundName', '')
        
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "Não foi possível obter os dados.", "detalhes_tecnicos": str(e)}), 500

@game_bp.route('/public-profile')
def get_public_profile():
    """Retorna dados públicos do perfil para visualização compartilhável."""
    try:
        data = game_service.get_public_profile_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "Não foi possível obter os dados públicos."}), 500

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

# --- Rotas para Notificações ---
@game_bp.route('/notifications', methods=['GET'])
@jwt_required()
def get_notifications():
    """Retorna todas as notificações (lidas e não lidas) para o usuário."""
    try:
        notifications = game_service.get_all_notifications_for_frontend()
        return jsonify(notifications)
    except Exception as e:
        print(f"!!! ERRO AO BUSCAR NOTIFICAÇÕES: {e}")
        traceback.print_exc()
        return jsonify({"error": "Não foi possível buscar as notificações.", "detalhes_tecnicos": str(e)}), 500

@game_bp.route('/notifications/mark-read/<int:notification_id>', methods=['POST'])
@jwt_required()
def mark_notification_read(notification_id):
    """Marca uma notificação específica como lida."""
    try:
        result = game_service.mark_notification_as_read(notification_id)
        return jsonify(result)
    except Exception as e:
        print(f"!!! ERRO AO MARCAR NOTIFICAÇÃO COMO LIDA: {e}")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Erro ao marcar notificação como lida.", "detalhes_tecnicos": str(e)}), 500

# --- ROTA PARA SORTEAR JOGO ---
@game_bp.route('/random', methods=['GET'])
@jwt_required()
def get_random_game_route():
    """
    Sorteia um jogo aleatório com base em filtros opcionais da query string.
    Ex: /api/games/random?plataforma=Computador&estilo=RPG
    """
    try:
        plataforma = request.args.get('plataforma')
        estilo = request.args.get('estilo')
        metacritic_min = request.args.get('metacritic_min')
        metacritic_max = request.args.get('metacritic_max')
        
        random_game = game_service.get_random_game(plataforma, estilo, metacritic_min, metacritic_max)
        
        if random_game:
            return jsonify(random_game)
        
        return jsonify({'message': 'Nenhum jogo encontrado com os critérios especificados'}), 404
    except Exception as e:
        print(f"!!! ERRO NA ROTA /random: {e}")
        traceback.print_exc()
        return jsonify({"error": "Ocorreu um erro interno ao sortear o jogo.", "detalhes_tecnicos": str(e)}), 500

@game_bp.route('/wishlist/update-prices', methods=['POST'])
@jwt_required()
def update_wishlist_prices():
    """Aciona a GitHub Action para atualizar os preços da lista de desejos."""
    try:
        result = game_service.trigger_wishlist_scraper_action()
        if result["success"]:
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    except Exception as e:
        print(f"ERRO NA ROTA /wishlist/update-prices: {e}")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Erro no servidor ao tentar atualizar os preços."}), 500

# --- NOVA ROTA: Histórico de Preços para um Jogo ---
@game_bp.route('/wishlist/price-history/<string:game_name>', methods=['GET'])
@jwt_required()
def get_wish_price_history(game_name):
    """
    Retorna o histórico de preços para um jogo específico da lista de desejos.
    """
    try:
        history = game_service.get_price_history_for_game(game_name)
        return jsonify(history)
    except Exception as e:
        print(f"!!! ERRO NA ROTA /wishlist/price-history/{game_name}: {e}")
        traceback.print_exc()
        return jsonify({"error": "Não foi possível obter o histórico de preços.", "detalhes_tecnicos": str(e)}), 500

# --- NOVA ROTA: Jogos Similares da Planilha ---
@game_bp.route('/similar-games/<string:game_name>', methods=['GET'])
@jwt_required()
def get_similar_games_from_sheet_route(game_name):
    """
    Retorna uma lista de jogos similares pré-processados da planilha,
    buscando e salvando imagens se necessário.
    """
    try:
        similar_games = game_service.get_similar_games_from_sheet(game_name)
        return jsonify(similar_games)
    except Exception as e:
        print(f"!!! ERRO NA ROTA /similar-games/{game_name}: {e}")
        traceback.print_exc()
        return jsonify({"error": "Ocorreu um erro interno ao buscar jogos similares.", "detalhes_tecnicos": str(e)}), 500