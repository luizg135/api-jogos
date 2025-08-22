# services/notification_service.py
import gspread
import json
from datetime import datetime, timedelta
import traceback
import hashlib
from config import Config
from services import game_service
from oauth2client.service_account import ServiceAccountCredentials

def _get_sheet(sheet_name):
    try:
        creds_json = json.loads(Config.GOOGLE_SHEETS_CREDENTIALS_JSON)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(Config.GAME_SHEET_URL)
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        print(f"Erro ao autenticar: {e}")
        traceback.print_exc()
        return None

def _get_data_from_sheet(sheet):
    try:
        return sheet.get_all_records()
    except gspread.exceptions.APIError as e:
        if "unable to parse range" in str(e): return []
        print(f"Erro ao ler dados: {e}")
        traceback.print_exc()
        return []
    except Exception as e:
        print(f"Erro genérico ao ler dados: {e}")
        traceback.print_exc()
        return []

def _find_next_id(sheet):
    try:
        all_ids = [r['ID'] for r in sheet.get_all_records() if 'ID' in r and isinstance(r['ID'], (int, float))]
        return max(all_ids) + 1 if all_ids else 1
    except Exception:
        return 1

def add_notification(message, notification_type):
    try:
        sheet = _get_sheet('Notificacoes')
        if not sheet:
            return {"success": False, "message": "Conexão com a planilha de Notificacoes falhou."}

        # Verificação de duplicidade: Evita notificações idênticas e não lidas
        existing_notifications = get_notifications()
        is_duplicate = any(
            n.get('Mensagem') == message and n.get('Lida') == 'Não' and n.get('Tipo') == notification_type
            for n in existing_notifications
        )
        
        if is_duplicate:
            print("Notificação duplicada evitada.")
            return {"success": False, "message": "Notificação já existe e não foi lida."}

        new_id = _find_next_id(sheet)
        date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        row_data = [new_id, message, notification_type, date_str, 'Não', '']
        sheet.append_row(row_data)
        print(f"Notificação adicionada: {message}")
        return {"success": True, "message": "Notificação adicionada."}
    except Exception as e:
        print(f"Erro ao adicionar notificação: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar notificação."}

def get_notifications():
    try:
        sheet = _get_sheet('Notificacoes')
        if not sheet: return []
        return _get_data_from_sheet(sheet)
    except Exception:
        return []

def mark_notification_as_read(notification_id):
    try:
        sheet = _get_sheet('Notificacoes')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        # Encontra a célula pelo ID da notificação
        records = sheet.get_all_records()
        row_index = -1
        for i, record in enumerate(records):
            if record.get('ID') == notification_id:
                row_index = i + 2 # +2 pois a linha 1 é cabeçalho e os índices do Python começam em 0
                break
        
        if row_index == -1:
            return {"success": False, "message": "Notificação não encontrada."}

        # Encontra as colunas "Lida" e "DataLida" e atualiza
        headers = sheet.row_values(1)
        try:
            lida_col_index = headers.index('Lida') + 1
            data_lida_col_index = headers.index('DataLida') + 1
        except ValueError:
            return {"success": False, "message": "Colunas 'Lida' ou 'DataLida' não encontradas."}

        updates = [
            {'range': gspread.utils.rowcol_to_a1(row_index, lida_col_index), 'values': [['Sim']]},
            {'range': gspread.utils.rowcol_to_a1(row_index, data_lida_col_index), 'values': [[datetime.now().strftime('%Y-%m-%d %H:%M:%S')]]}
        ]
        sheet.batch_update(updates)

        return {"success": True, "message": "Notificação marcada como lida."}
    except Exception as e:
        print(f"Erro ao marcar notificação como lida: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro ao marcar notificação como lida."}

def _hash_data(data_list):
    """Gera um hash para uma lista de dicionários para comparar alterações."""
    data_str = json.dumps(sorted(data_list, key=lambda x: x.get('Nome', '')), sort_keys=True)
    return hashlib.sha256(data_str.encode('utf-8')).hexdigest()

def _update_profile_data(profile_sheet, key, value):
    try:
        cell = profile_sheet.find(key)
        profile_sheet.update_cell(cell.row, cell.col + 1, value)
    except gspread.exceptions.CellNotFound:
        profile_sheet.append_row([key, value])

def check_for_notifications(current_library, current_wishlist, current_achievements):
    try:
        profile_sheet = _get_sheet('Perfil')
        if not profile_sheet: return {"success": False, "message": "Conexão com a planilha de Perfil falhou."}
        
        profile_records = _get_data_from_sheet(profile_sheet)
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}

        last_game_state = json.loads(profile_data.get('last_full_game_data', '[]'))
        last_wish_state = json.loads(profile_data.get('last_full_wish_data', '[]'))
        last_completed_ach_ids = json.loads(profile_data.get('last_completed_achievements', '[]'))
        
        # Notificações de Conquistas
        new_achievements = [ach for ach in current_achievements if ach['ID'] not in last_completed_ach_ids]
        for ach in new_achievements:
            add_notification(f"Conquista adquirida: \"{ach['Nome']}\"!", 'conquista')
        
        # Notificações de Lançamento de Jogos
        today = datetime.now().date()
        for wish in current_wishlist:
            try:
                if wish.get('Data Lançamento') and '-' in wish.get('Data Lançamento'):
                    release_date = datetime.strptime(wish.get('Data Lançamento', ''), '%Y-%m-%d').date()
                    days_until_release = (release_date - today).days
                    if days_until_release >= 0 and days_until_release <= 7:
                        add_notification(f"O jogo da sua lista de desejos \"{wish['Nome']}\" será lançado em {days_until_release} dias!", 'lancamento')
            except (ValueError, KeyError):
                pass
        
        # Verifica alterações na Biblioteca e Wishlist
        old_game_names = {g['Nome'] for g in last_game_state}
        current_game_names = {g['Nome'] for g in current_library}
        added_games = current_game_names - old_game_names
        removed_games = old_game_names - current_game_names

        old_wish_names = {w['Nome'] for w in last_wish_state}
        current_wish_names = {w['Nome'] for w in current_wishlist}
        added_wishes = current_wish_names - old_wish_names
        removed_wishes = old_wish_names - current_wish_names
        
        # Adiciona notificações para alterações na biblioteca
        if added_games:
            for name in added_games:
                add_notification(f"O jogo \"{name}\" foi adicionado à sua biblioteca.", 'adicao_biblioteca')
        if removed_games:
            for name in removed_games:
                if name in current_wish_names:
                    # Isso é uma compra, notificação já tratada em purchase
                    continue
                add_notification(f"O jogo \"{name}\" foi removido da sua biblioteca.", 'remocao_biblioteca')
        
        # Adiciona notificações para alterações na wishlist
        if added_wishes:
            for name in added_wishes:
                add_notification(f"O jogo \"{name}\" foi adicionado à sua lista de desejos.", 'adicao_desejos')
        if removed_wishes:
            for name in removed_wishes:
                add_notification(f"O jogo \"{name}\" foi removido da sua lista de desejos.", 'remocao_desejos')

        # Notificações de Alteração de Jogo (biblioteca e wishlist)
        current_game_state_hash = _hash_data(current_library)
        if profile_data.get('last_game_state_hash') != current_game_state_hash:
            for game in current_library:
                if game['Nome'] in old_game_names:
                    old_game = next((g for g in last_game_state if g['Nome'] == game['Nome']), None)
                    if old_game and _hash_data([game]) != _hash_data([old_game]):
                        add_notification(f"O jogo \"{game['Nome']}\" na sua biblioteca foi alterado.", 'alteracao_biblioteca')
        
        current_wish_state_hash = _hash_data(current_wishlist)
        if profile_data.get('last_wish_state_hash') != current_wish_state_hash:
            for wish in current_wishlist:
                if wish['Nome'] in old_wish_names:
                    old_wish = next((w for w in last_wish_state if w['Nome'] == wish['Nome']), None)
                    if old_wish and _hash_data([wish]) != _hash_data([old_wish]):
                        add_notification(f"O item \"{wish['Nome']}\" na sua lista de desejos foi alterado.", 'alteracao_desejos')

        # Salva o novo estado para a próxima checagem
        _update_profile_data(profile_sheet, 'last_full_game_data', json.dumps(current_library))
        _update_profile_data(profile_sheet, 'last_full_wish_data', json.dumps(current_wishlist))
        _update_profile_data(profile_sheet, 'last_completed_achievements', json.dumps([ach['ID'] for ach in current_achievements]))
        _update_profile_data(profile_sheet, 'last_game_state_hash', current_game_state_hash)
        _update_profile_data(profile_sheet, 'last_wish_state_hash', current_wish_state_hash)

        return {"success": True, "message": "Checagem de notificações finalizada."}
    except Exception as e:
        print(f"Erro na checagem de notificações: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro na checagem de notificações."}
