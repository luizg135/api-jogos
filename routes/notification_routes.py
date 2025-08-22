from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required
from services import notification_service # Altere a importação
import traceback

notification_bp = Blueprint('notifications', __name__)

@notification_bp.route('', methods=['GET'])
@jwt_required()
def get_user_notifications():
    """
    Retorna as notificações do usuário, separando-as em lidas e não lidas.
    """
    try:
        all_notifications = notification_service.get_all_notifications()
        # Aqui a sua lógica para separar as notificações lidas das não lidas
        unread_notifications = [n for n in all_notifications if n['Status'] == 'unread']
        read_notifications = [n for n in all_notifications if n['Status'] == 'read']
        
        return jsonify({
            'unread_count': len(unread_notifications),
            'unread': unread_notifications,
            'read': read_notifications
        })
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
        
        # Chama a função do novo serviço
        result = notification_service.mark_notifications_as_read(notification_ids)
        return jsonify(result)
    except Exception as e:
        print(f"!!! ERRO ao marcar notificações como lidas: {e}")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Erro ao marcar notificações como lidas.", "detalhes_tecnicos": str(e)}), 500
