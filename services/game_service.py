import gspread
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
from config import Config
from datetime import datetime
import traceback

def _get_sheet(sheet_name):
    """Autentica com a API do Google Sheets e retorna uma planilha específica."""
    try:
        creds_json = json.loads(Config.GOOGLE_SHEETS_CREDENTIALS_JSON)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(Config.GAME_SHEET_URL)
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        print(f"Erro ao autenticar com a API do Google Sheets: {e}")
        traceback.print_exc()
        return None

def get_all_game_data():
    """Lê os dados das planilhas 'Jogos' e 'Desejos' e os retorna em formato JSON."""
    try:
        game_sheet = _get_sheet('Jogos')
        if not game_sheet:
            return None
        games_data = game_sheet.get_all_records()

        wishlist_sheet = _get_sheet('Desejos')
        if not wishlist_sheet:
            return None
        wishlist_data = wishlist_sheet.get_all_records()

        return {
            'biblioteca': games_data,
            'desejos': wishlist_data
        }
    except Exception as e:
        print(f"Erro ao buscar dados das planilhas: {e}")
        traceback.print_exc()
        return None

def add_game_to_sheet(game_data):
    """Adiciona um novo jogo à planilha 'Jogos' com todos os campos do frontend."""
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        # Mapeia os dados do frontend para a ordem correta das colunas na sua planilha
        row_data = [
            game_data.get('Nome', ''),
            game_data.get('Plataforma', ''),
            game_data.get('Nota', ''),
            game_data.get('Preço', ''),
            game_data.get('Estilo', ''),
            '', # Coluna 'Adquirido em' - Deixada vazia
            '', # Coluna 'Início em' - Deixada vazia
            '', # Coluna 'Terminado em' - Deixada vazia
            '', # Coluna 'Conclusão' - Deixada vazia
            game_data.get('Tempo de Jogo', ''),
            game_data.get('Conquistas Obtidas', ''),
            game_data.get('Platinado?', ''),
            ''  # Coluna 'Abandonado?' - Deixada vazia
        ]
        
        sheet.append_row(row_data)
        return {"success": True, "message": "Jogo adicionado com sucesso."}
    except Exception as e:
        print(f"Erro ao adicionar jogo: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar jogo."}

def add_wish_to_sheet(wish_data):
    """Adiciona um novo item à planilha 'Desejos'."""
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        row_data = [
            wish_data.get('Nome', ''),
            wish_data.get('Link', ''),
            wish_data.get('Data Lançamento', ''),
            wish_data.get('Preço', '')
        ]
        sheet.append_row(row_data)
        return {"success": True, "message": "Item de desejo adicionado com sucesso."}
    except Exception as e:
        print(f"Erro ao adicionar item de desejo: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar item de desejo."}
    
def update_game_in_sheet(game_name, updated_data):
    """Atualiza as informações de um jogo existente na planilha 'Jogos'."""
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        cell = sheet.find(game_name)
        if not cell: return {"success": False, "message": "Jogo não encontrado."}

        # Mapeia as colunas da sua planilha
        update_map = {
            'Nome': 1, 'Plataforma': 2, 'Nota': 3, 'Preço': 4,
            'Estilo': 5, 'Adquirido em': 6, 'Início em': 7,
            'Terminado em': 8, 'Conclusão': 9, 'Tempo de Jogo': 10,
            'Conquistas Obtidas': 11, 'Platinado?': 12, 'Abandonado?': 13
        }

        updates = []
        for key, col in update_map.items():
            if key in updated_data:
                updates.append({'range': f'{gspread.utils.rowcol_to_a1(cell.row, col)}', 'values': [[updated_data[key]]]})

        if updates:
            sheet.batch_update(updates)
        
        return {"success": True, "message": "Jogo atualizado com sucesso."}
    except Exception as e:
        print(f"Erro ao atualizar jogo: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar jogo."}
        
def delete_game_from_sheet(game_name):
    """Deleta um jogo da planilha 'Jogos'."""
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        cell = sheet.find(game_name)
        if not cell: return {"success": False, "message": "Jogo não encontrado."}
        
        sheet.delete_rows(cell.row)
        return {"success": True, "message": "Jogo deletado com sucesso."}
    except Exception as e:
        print(f"Erro ao deletar jogo: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar jogo."}
    
def update_wish_in_sheet(wish_name, updated_data):
    """Atualiza as informações de um item da lista de desejos na planilha 'Desejos'."""
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}

        cell = sheet.find(wish_name)
        if not cell: return {"success": False, "message": "Item de desejo não encontrado."}
        
        update_map = {
            'Nome': 1, 'Link': 2, 'Data Lançamento': 3, 'Preço': 4
        }
        
        updates = []
        for key, col in update_map.items():
            if key in updated_data:
                updates.append({'range': f'{gspread.utils.rowcol_to_a1(cell.row, col)}', 'values': [[updated_data[key]]]})

        if updates:
            sheet.batch_update(updates)

        return {"success": True, "message": "Item de desejo atualizado com sucesso."}
    except Exception as e:
        print(f"Erro ao atualizar item de desejo: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar item de desejo."}

def delete_wish_from_sheet(wish_name):
    """Deleta um item da planilha 'Desejos'."""
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}

        cell = sheet.find(wish_name)
        if not cell: return {"success": False, "message": "Item de desejo não encontrado."}
        
        sheet.delete_rows(cell.row)
        return {"success": True, "message": "Item de desejo deletado com sucesso."}
    except Exception as e:
        print(f"Erro ao deletar item de desejo: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar item de desejo."}
