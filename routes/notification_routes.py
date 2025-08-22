from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from services import game_service
import traceback

notification_bp = Blueprint('notifications', __name__)

@notification_bp.route('/', methods=['GET')
@jwt_required()
def get_user_notifications():
    """
    Retorna as notificações do usuário, separando-as em lidas e não lidas.
    """
    try:
        notifications = game_service.get_notifications()
        return jsonify(notifications)
    except Exception as e:
        print(f"!!! ERRO ao buscar notificações: {e}")
        traceback.print_exc()
        return jsonify({"error": "Não foi possível obter as notificações.", "detalhes_tecnicos": str(e)}), 500

@notification_bp.route('/mark-as-read', methods=['PUT'])
@jwt_required()
def mark_notifications_as_read_route():
    """
    Marca uma lista de notificações como lidas.
    Espera um JSON com a chave 'notification_ids' contendo uma lista de IDs.
    """
    try:
        data = request.json
        notification_ids = data.get('notification_ids', [])
        if not notification_ids:
            return jsonify({"success": False, "message": "Nenhum ID de notificação fornecido."}), 400
        
        result = game_service.mark_notifications_as_read(notification_ids)
        return jsonify(result)
    except Exception as e:
        print(f"!!! ERRO ao marcar notificações como lidas: {e}")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Erro ao marcar notificações como lidas.", "detalhes_tecnicos": str(e)}), 500
