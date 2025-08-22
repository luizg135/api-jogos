import uuid
from datetime import datetime
import gspread
from services.game_service import get_sheet # Importação corrigida

# Nome da aba na planilha do Google Sheets
SHEET_NAME = "Notificações"

def add_notification(notification_type, message, jogo_name=None):
    """
    Adiciona uma nova notificação à planilha 'Notificações'.
    """
    try:
        sheet = get_sheet(SHEET_NAME) # Chamada corrigida para usar get_sheet
        notification_id = str(uuid.uuid4())

        row_data = [
            notification_id,
            notification_type,
            message,
            datetime.now().isoformat(),
            'unread',
            jogo_name if jogo_name else ''
        ]

        sheet.append_row(row_data)
        print(f">>> NOTIFICAÇÃO ADICIONADA: {message}")

        return {"success": True, "message": "Notificação adicionada com sucesso."}
    except Exception as e:
        print(f"!!! ERRO ao adicionar notificação: {e}")
        return {"success": False, "message": "Erro ao adicionar notificação."}

def get_all_notifications():
    """
    Lê todas as notificações da planilha 'Notificações'.
    """
    try:
        sheet = get_sheet(SHEET_NAME) # Chamada corrigida
        all_records = sheet.get_all_records()

        return all_records
    except Exception as e:
        print(f"!!! ERRO ao ler notificações: {e}")
        return []

def mark_notifications_as_read(notification_ids):
    """
    Marca uma lista de notificações como lidas na planilha.
    """
    try:
        sheet = get_sheet(SHEET_NAME) # Chamada corrigida
        all_values = sheet.get_all_values()

        updates = []
        for i, row in enumerate(all_values):
            if row[0] in notification_ids and row[4] != 'read':
                updates.append({'range': f'E{i+1}', 'values': [['read']]})

        if updates:
            sheet.batch_update(updates)
            return {"success": True, "message": "Notificações marcadas como lidas."}

        return {"success": False, "message": "Nenhuma notificação para marcar."}
    except Exception as e:
        print(f"!!! ERRO ao marcar notificações como lidas: {e}")
        return {"success": False, "message": "Erro ao marcar notificações."}
