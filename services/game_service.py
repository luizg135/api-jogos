import gspread
import pandas as pd
import json
import math
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
        print(f"Erro ao autenticar com a API do Google Sheets: {e}"); traceback.print_exc()
        return None

def _get_data_from_sheet(sheet):
    """Lê todos os dados de uma planilha de forma segura, tratando planilhas vazias."""
    try:
        return sheet.get_all_records()
    except gspread.exceptions.APIError as e:
        if "unable to parse range" in str(e): return []
        print(f"Erro ao ler dados da planilha: {e}"); traceback.print_exc()
        return []
    except Exception as e:
        print(f"Erro genérico ao ler dados da planilha: {e}"); traceback.print_exc()
        return []

def _calculate_gamer_stats(games_data):
    """Calcula EXP, nível e rank com base nos dados dos jogos."""
    total_exp = 0
    for game in games_data:
        if game.get('Status') == 'Finalizado': total_exp += 100
        elif game.get('Status') == 'Platinado': total_exp += 500
        try:
            nota = float(str(game.get('Nota', '0')).replace(',', '.'))
            if nota > 0: total_exp += int(nota * 10)
        except ValueError: pass
        total_exp += int(game.get('Conquistas Obtidas', 0))
    exp_per_level = 1000
    nivel = math.floor(total_exp / exp_per_level)
    exp_no_nivel_atual = total_exp % exp_per_level
    ranks = {0: "Bronze", 10: "Prata", 20: "Ouro", 30: "Platina", 40: "Diamante", 50: "Mestre"}
    rank_gamer = "Bronze"
    for level_req, rank_name in ranks.items():
        if nivel >= level_req: rank_gamer = rank_name
    return {'nivel_gamer': nivel, 'rank_gamer': rank_gamer, 'exp_nivel_atual': exp_no_nivel_atual, 'exp_para_proximo_nivel': exp_per_level}

def get_all_game_data():
    """Lê, ORDENA e retorna todos os dados de jogos, desejos e perfil."""
    try:
        game_sheet = _get_sheet('Jogos'); games_data = _get_data_from_sheet(game_sheet) if game_sheet else []
        wishlist_sheet = _get_sheet('Desejos'); wishlist_data = _get_data_from_sheet(wishlist_sheet) if wishlist_sheet else []
        profile_sheet = _get_sheet('Perfil'); profile_records = _get_data_from_sheet(profile_sheet) if profile_sheet else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}

        def sort_key(game):
            try: nota = float(str(game.get('Nota', '-1')).replace(',', '.'))
            except (ValueError, TypeError): nota = -1
            return (-nota, game.get('Nome', '').lower())
        games_data.sort(key=sort_key)

        gamer_stats = _calculate_gamer_stats(games_data)
        notas = [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')]
        stats = {
            **gamer_stats,
            'total_jogos': len(games_data), 'total_na_fila': len([g for g in games_data if g.get('Status') == 'Na Fila']),
            'total_horas_jogadas': sum([int(str(g.get('Tempo de Jogo', 0)).replace('h', '')) for g in games_data]),
            'custo_total_biblioteca': sum([float(str(g.get('Preço', '0,00')).replace('R$', '').replace(',', '.')) for g in games_data]),
            'media_notas': round(sum(notas) / len(notas), 2) if notas else 0,
            'total_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim']),
            'total_conquistas': sum([int(g.get('Conquistas Obtidas', 0)) for g in games_data])
        }

        return {
            'estatisticas': stats,
            'biblioteca': games_data,
            'desejos': wishlist_data,
            'perfil': profile_data
        }
    except Exception as e:
        print(f"Erro ao buscar dados das planilhas: {e}"); traceback.print_exc()
        return { 'estatisticas': {}, 'biblioteca': [], 'desejos': [], 'perfil': {} }

def update_profile_in_sheet(profile_data):
    """Atualiza as informações de perfil na planilha 'Perfil'."""
    try:
        sheet = _get_sheet('Perfil')
        if not sheet: return {"success": False, "message": "Conexão com a planilha de perfil falhou."}
        for key, value in profile_data.items():
            try:
                cell = sheet.find(key)
                sheet.update_cell(cell.row, cell.col + 1, value)
            except gspread.exceptions.CellNotFound:
                sheet.append_row([key, value])
        return {"success": True, "message": "Perfil atualizado com sucesso."}
    except Exception as e:
        print(f"Erro ao atualizar perfil: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar perfil."}

def add_game_to_sheet(game_data):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        row_data = [
            game_data.get('Nome', ''), game_data.get('Plataforma', ''),
            game_data.get('Status', ''), game_data.get('Nota', ''),
            game_data.get('Preço', ''), game_data.get('Tempo de Jogo', ''),
            game_data.get('Conquistas Obtidas', ''), game_data.get('Platinado?', ''),
            game_data.get('Estilo', ''), game_data.get('Link', ''),
            '', '', game_data.get('Terminado em', ''), '', ''
        ]
        sheet.append_row(row_data)
        return {"success": True, "message": "Jogo adicionado com sucesso."}
    except Exception as e:
        print(f"Erro ao adicionar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar jogo."}

def add_wish_to_sheet(wish_data):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        row_data = [
            wish_data.get('Nome', ''), wish_data.get('Link', ''),
            wish_data.get('Data Lançamento', ''), wish_data.get('Preço', '')
        ]
        sheet.append_row(row_data)
        return {"success": True, "message": "Item de desejo adicionado com sucesso."}
    except Exception as e:
        print(f"Erro ao adicionar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar item de desejo."}
    
def update_game_in_sheet(game_name, updated_data):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        try: cell = sheet.find(game_name)
        except gspread.exceptions.CellNotFound: return {"success": False, "message": "Jogo não encontrado."}
        row_values = sheet.row_values(cell.row)
        column_map = {
            'Nome': 0, 'Plataforma': 1, 'Status': 2, 'Nota': 3, 'Preço': 4,
            'Tempo de Jogo': 5, 'Conquistas Obtidas': 6, 'Platinado?': 7,
            'Estilo': 8, 'Link': 9, 'Adquirido em': 10, 'Início em': 11,
            'Terminado em': 12, 'Conclusão': 13, 'Abandonado?': 14
        }
        new_row = list(row_values)
        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                while len(new_row) <= col_index: new_row.append('')
                new_row[col_index] = value
        sheet.update(f'A{cell.row}', [new_row])
        return {"success": True, "message": "Jogo atualizado com sucesso."}
    except Exception as e:
        print(f"Erro ao atualizar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar jogo."}
        
def delete_game_from_sheet(game_name):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(game_name)
        if not cell: return {"success": False, "message": "Jogo não encontrado."}
        sheet.delete_rows(cell.row)
        return {"success": True, "message": "Jogo deletado com sucesso."}
    except Exception as e:
        print(f"Erro ao deletar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar jogo."}
    
def update_wish_in_sheet(wish_name, updated_data):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(wish_name)
        if not cell: return {"success": False, "message": "Item de desejo não encontrado."}
        row_values = sheet.row_values(cell.row)
        column_map = {'Nome': 0, 'Link': 1, 'Data Lançamento': 2, 'Preço': 3}
        new_row = list(row_values)
        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                while len(new_row) <= col_index: new_row.append('')
                new_row[col_index] = value
        sheet.update(f'A{cell.row}', [new_row])
        return {"success": True, "message": "Item de desejo atualizado com sucesso."}
    except Exception as e:
        print(f"Erro ao atualizar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar item de desejo."}

def delete_wish_from_sheet(wish_name):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(wish_name)
        if not cell: return {"success": False, "message": "Item de desejo não encontrado."}
        sheet.delete_rows(cell.row)
        return {"success": True, "message": "Item de desejo deletado com sucesso."}
    except Exception as e:
        print(f"Erro ao deletar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar item de desejo."}
