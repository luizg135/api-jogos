import uuid
from datetime import datetime
from services.google_sheets_api import get_sheet_by_name # Ajuste para a sua função de acesso à planilha
import gspread # Adicione esta linha para importar a biblioteca gspread

# Nome da aba na planilha do Google Sheets
SHEET_NAME = "Notificações"

def add_notification(notification_type, message, jogo_name=None):
    """
    Adiciona uma nova notificação à planilha 'Notificações'.
    """
    try:
        sheet = get_sheet_by_name(SHEET_NAME)
        notification_id = str(uuid.uuid4())
        
        row_data = [
            notification_id,
            notification_type,
            message,
            datetime.now().isoformat(),
            'unread',
            jogo_name if jogo_name else ''
        ]
        
        sheet.append_row(row_data) # Use o método append_row()
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
        # Pega a planilha de Notificações
        sheet = get_sheet_by_name(SHEET_NAME)
        
        # Usa o método get_all_records() para ler os dados,
        # exatamente como você faz no game_service.py.
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
        sheet = get_sheet_by_name(SHEET_NAME)
        
        # Primeiro, pegue todas as linhas (valores) da planilha, incluindo o cabeçalho.
        all_values = sheet.get_all_values()
        
        updates = []
        for i, row in enumerate(all_values):
            # i+1 é o número da linha na planilha (já que o índice começa em 0)
            if row[0] in notification_ids and row[4] != 'read':
                # Cria a instrução para atualizar a célula na coluna 'Status' (índice 4).
                updates.append({'range': f'E{i+1}', 'values': [['read']]})
                
        if updates:
            # Envia as atualizações para a planilha de uma vez.
            sheet.batch_update(updates)
            return {"success": True, "message": "Notificações marcadas como lidas."}
        
        return {"success": False, "message": "Nenhuma notificação para marcar."}
    except Exception as e:
        print(f"!!! ERRO ao marcar notificações como lidas: {e}")
        return {"success": False, "message": "Erro ao marcar notificações."}
