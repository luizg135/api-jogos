# routes/notification_routes.py
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from services import notification_service
import traceback

notification_bp = Blueprint('notifications', __name__)

@notification_bp.route('/check', methods=['POST'])
@jwt_required()
def check_notifications():
    """
    Executa a verificação de todas as notificações e retorna as não lidas.
    """
    try:
        data = request.json
        notification_service.check_for_notifications(
            data.get('biblioteca', []),
            data.get('desejos', []),
            data.get('conquistas_concluidas', [])
        )
        unread_notifications = [notif for notif in notification_service.get_notifications() if notif.get('Lida') == 'Não']
        return jsonify(unread_notifications)
    except Exception as e:
        print(f"!!! ERRO NA ROTA /check: {e}")
        traceback.print_exc()
        return jsonify({"error": "Ocorreu um erro interno na checagem de notificações."}), 500

@notification_bp.route('/mark-as-read/<int:notification_id>', methods=['PUT'])
@jwt_required()
def mark_as_read(notification_id):
    """
    Marca uma notificação como lida.
    """
    try:
        result = notification_service.mark_notification_as_read(notification_id)
        return jsonify(result)
    except Exception as e:
        print(f"!!! ERRO NA ROTA /mark-as-read: {e}")
        traceback.print_exc()
        return jsonify({"error": "Ocorreu um erro interno ao marcar a notificação."}), 500
