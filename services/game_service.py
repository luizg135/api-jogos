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

def _get_data_from_sheet(sheet):
    """Lê todos os dados de uma planilha de forma segura, tratando planilhas vazias."""
    try:
        records = sheet.get_all_records()
        return records
    except gspread.exceptions.APIError as e:
        if "unable to parse range" in str(e):
            return []
        print(f"Erro ao ler dados da planilha: {e}")
        traceback.print_exc()
        return []
    except Exception as e:
        print(f"Erro genérico ao ler dados da planilha: {e}")
        traceback.print_exc()
        return []

def get_all_game_data():
    """Lê os dados das planilhas 'Jogos' e 'Desejos' e os retorna em formato JSON."""
    try:
        game_sheet = _get_sheet('Jogos')
        games_data = _get_data_from_sheet(game_sheet) if game_sheet else []

        wishlist_sheet = _get_sheet('Desejos')
        wishlist_data = _get_data_from_sheet(wishlist_sheet) if wishlist_sheet else []
        
        notas = [float(g.get('Nota', 0)) for g in games_data if g.get('Nota')]
        stats = {
            'nivel_gamer': 0, 'rank_gamer': 'N/A', 'exp_nivel_atual': 0, 'exp_para_proximo_nivel': 100,
            'total_jogos': len(games_data), 
            'total_na_fila': len([g for g in games_data if g.get('Status') == 'Na Fila']),
            'total_horas_jogadas': sum([int(str(g.get('Tempo de Jogo', 0)).replace('h', '')) for g in games_data]),
            'custo_total_biblioteca': sum([float(str(g.get('Preço', '0,00')).replace('R$', '').replace(',', '.')) for g in games_data]),
            'media_notas': round(sum(notas) / len(notas), 2) if notas else 0,
            'total_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim']),
            'total_conquistas': sum([int(g.get('Conquistas Obtidas', 0)) for g in games_data])
        }

        return {
            'estatisticas': stats,
            'biblioteca': games_data,
            'desejos': wishlist_data
        }
    except Exception as e:
        print(f"Erro ao buscar dados das planilhas: {e}")
        traceback.print_exc()
        return {
            'estatisticas': {},
            'biblioteca': [],
            'desejos': []
        }

# NOVO: Função de adicionar jogo ATUALIZADA
def add_game_to_sheet(game_data):
    """Adiciona um novo jogo à planilha 'Jogos' com todos os campos do frontend."""
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}

        # IMPORTANTE: A ORDEM DOS ITENS ABAIXO DEVE SER A MESMA DAS SUAS COLUNAS NA PLANILHA
        # Se sua planilha for diferente, reordene esta lista.
        row_data = [
            game_data.get('Nome', ''),
            game_data.get('Plataforma', ''),
            game_data.get('Status', ''),
            game_data.get('Nota', ''),
            game_data.get('Preço', ''),
            game_data.get('Tempo de Jogo', ''),
            game_data.get('Conquistas Obtidas', ''),
            game_data.get('Platinado?', ''),
            game_data.get('Estilo', ''),
            game_data.get('Link', ''),
            '',  # Coluna 'Adquirido em' - não usada pelo frontend
            '',  # Coluna 'Início em' - não usada pelo frontend
            game_data.get('Terminado em', ''), # NOVO: Salva a data de conclusão
            '',  # Coluna 'Conclusão' - não usada pelo frontend
            ''   # Coluna 'Abandonado?' - não usada pelo frontend
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
    
# NOVO: Função de atualizar jogo ATUALIZADA
def update_game_in_sheet(game_name, updated_data):
    """Atualiza as informações de um jogo existente na planilha 'Jogos'."""
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        # Encontra a linha pelo nome original do jogo
        try:
            cell = sheet.find(game_name)
        except gspread.exceptions.CellNotFound:
            return {"success": False, "message": "Jogo não encontrado."}
        
        # Pega todos os valores da linha encontrada para preservar os dados que não foram alterados
        row_values = sheet.row_values(cell.row)
        
        # IMPORTANTE: Este dicionário mapeia o nome do campo para o ÍNDICE (base 0) da coluna na sua planilha
        # Ex: 'Nome' está na primeira coluna (índice 0), 'Plataforma' na segunda (índice 1), etc.
        # AJUSTE ESTA ORDEM se a sua planilha for diferente.
        column_map = {
            'Nome': 0, 'Plataforma': 1, 'Status': 2, 'Nota': 3, 'Preço': 4,
            'Tempo de Jogo': 5, 'Conquistas Obtidas': 6, 'Platinado?': 7,
            'Estilo': 8, 'Link': 9, 'Adquirido em': 10, 'Início em': 11,
            'Terminado em': 12, 'Conclusão': 13, 'Abandonado?': 14
        }
        
        # Cria a nova linha com os dados atualizados
        new_row = list(row_values) # Copia os valores antigos
        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                # Garante que a lista 'new_row' tenha o tamanho necessário
                while len(new_row) <= col_index:
                    new_row.append('')
                new_row[col_index] = value

        # Atualiza a linha inteira na planilha
        sheet.update(f'A{cell.row}', [new_row])
        
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
        
        row_values = sheet.row_values(cell.row)
        column_map = {'Nome': 0, 'Link': 1, 'Data Lançamento': 2, 'Preço': 3}

        new_row = list(row_values)
        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                while len(new_row) <= col_index:
                    new_row.append('')
                new_row[col_index] = value

        sheet.update(f'A{cell.row}', [new_row])

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
