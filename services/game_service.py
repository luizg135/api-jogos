import gspread
import pandas as pd
import json
import math
from oauth2client.service_account import ServiceAccountCredentials
from config import Config
from datetime import datetime
import traceback

def _get_sheet(sheet_name):
    try:
        creds_json = json.loads(Config.GOOGLE_SHEETS_CREDENTIALS_JSON)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(Config.GAME_SHEET_URL)
        return spreadsheet.worksheet(sheet_name)
    except Exception as e:
        print(f"Erro ao autenticar: {e}"); traceback.print_exc()
        return None

def _get_data_from_sheet(sheet):
    try:
        return sheet.get_all_records()
    except gspread.exceptions.APIError as e:
        if "unable to parse range" in str(e): return []
        print(f"Erro ao ler dados: {e}"); traceback.print_exc()
        return []
    except Exception as e:
        print(f"Erro genérico ao ler dados: {e}"); traceback.print_exc()
        return []

def _check_achievements(games_data, stats, all_achievements, wishlist_data):
    completed = []
    pending = []
    
    progress_map = {
        'FINALIZADOS': stats.get('total_finalizados', 0),
        'PLATINADOS': stats.get('total_platinados', 0),
        'TOTAL_JOGOS': stats.get('total_jogos', 0),
        'HORAS_JOGADAS': stats.get('total_horas_jogadas', 0),
        'CUSTO_TOTAL': stats.get('custo_total_biblioteca', 0),
        'JOGOS_AVALIADOS': stats.get('total_avaliados', 0),
        'WISHLIST_TOTAL': len(wishlist_data),
        'JOGOS_LONGOS': stats.get('total_jogos_longos', 0),
        'SOULSLIKE_PLATINADOS': stats.get('total_soulslike_platinados', 0),
        'INDIE_TOTAL': stats.get('total_indie', 0),
        'JOGO_MAIS_JOGADO': stats.get('max_horas_um_jogo', 0),
        'FINALIZADOS_ACAO': stats.get('total_finalizados_acao', 0),
        'FINALIZADOS_ESTRATEGIA': stats.get('total_finalizados_estrategia', 0),
        'GENEROS_DIFERENTES': stats.get('total_generos_diferentes', 0),
        'NOTAS_10': stats.get('total_notas_10', 0),
        'NOTAS_BAIXAS': stats.get('total_notas_baixas', 0),
    }
    
    for ach in all_achievements:
        ach_type = ach.get('Tipo')
        try: target = float(ach.get('Meta', 0))
        except (ValueError, TypeError): target = 0
        current_progress = progress_map.get(ach_type, 0)
        
        ach['progresso_atual'] = current_progress
        ach['meta'] = target

        if current_progress >= target:
            completed.append(ach)
        else:
            pending.append(ach)
            
    return completed, pending

def _calculate_gamer_stats(games_data, unlocked_achievements):
    total_exp = 0
    for game in games_data:
        if game.get('Status') == 'Finalizado': total_exp += 100
        elif game.get('Status') == 'Platinado': total_exp += 500
        try:
            nota = float(str(game.get('Nota', '0')).replace(',', '.'))
            if nota > 0: total_exp += int(nota * 10)
        except ValueError: pass
        total_exp += int(game.get('Conquistas Obtidas', 0))

    for ach in unlocked_achievements:
        total_exp += int(ach.get('EXP', 0))

    exp_per_level = 1000
    nivel = math.floor(total_exp / exp_per_level)
    exp_no_nivel_atual = total_exp % exp_per_level
    ranks = {0: "Bronze", 10: "Prata", 20: "Ouro", 30: "Platina", 40: "Diamante", 50: "Mestre"}
    rank_gamer = "Bronze"
    for level_req, rank_name in ranks.items():
        if nivel >= level_req: rank_gamer = rank_name
    return {'nivel_gamer': nivel, 'rank_gamer': rank_gamer, 'exp_nivel_atual': exp_no_nivel_atual, 'exp_para_proximo_nivel': exp_per_level}

def get_all_game_data():
    try:
        game_sheet = _get_sheet('Jogos'); games_data = _get_data_from_sheet(game_sheet) if game_sheet else []
        wishlist_sheet = _get_sheet('Desejos'); wishlist_data = _get_data_from_sheet(wishlist_sheet) if wishlist_sheet else []
        profile_sheet = _get_sheet('Perfil'); profile_records = _get_data_from_sheet(profile_sheet) if profile_sheet else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet = _get_sheet('Conquistas'); all_achievements = _get_data_from_sheet(achievements_sheet) if achievements_sheet else []

        def sort_key(game):
            try: nota = float(str(game.get('Nota', '-1')).replace(',', '.'))
            except (ValueError, TypeError): nota = -1
            return (-nota, game.get('Nome', '').lower())
        games_data.sort(key=sort_key)

        # Contadores de estatísticas detalhadas
        notas = [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')]
        tempos_de_jogo = [int(str(g.get('Tempo de Jogo', 0)).replace('h', '')) for g in games_data]
        
        base_stats = {
            'total_jogos': len(games_data),
            'total_finalizados': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado']]),
            'total_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim']),
            'total_avaliados': len([g for g in games_data if g.get('Nota') and float(str(g.get('Nota')).replace(',', '.')) > 0]),
            'total_horas_jogadas': sum(tempos_de_jogo),
            'custo_total_biblioteca': sum([float(str(g.get('Preço', '0,00')).replace('R$', '').replace(',', '.')) for g in games_data]),
            'media_notas': round(sum(notas) / len(notas), 2) if notas else 0,
            'total_conquistas': sum([int(g.get('Conquistas Obtidas', 0)) for g in games_data]),
            'total_jogos_longos': len([t for t in tempos_de_jogo if t >= 50]),
            'total_soulslike_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim' and 'Soulslike' in g.get('Estilo', '')]),
            'total_indie': len([g for g in games_data if 'Indie' in g.get('Estilo', '')]),
            'max_horas_um_jogo': max(tempos_de_jogo) if tempos_de_jogo else 0,
            'total_finalizados_acao': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Ação' in g.get('Estilo', '')]),
            'total_finalizados_estrategia': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Estratégia' in g.get('Estilo', '')]),
            'total_generos_diferentes': len(set(g for game in games_data if game.get('Estilo') for g in game.get('Estilo').split(','))),
            'total_notas_10': len([n for n in notas if n == 10]),
            'total_notas_baixas': len([n for n in notas if n <= 3]),
        }

        completed_achievements, pending_achievements = _check_achievements(games_data, base_stats, all_achievements, wishlist_data)
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        final_stats = {**base_stats, **gamer_stats}

        return {
            'estatisticas': final_stats, 'biblioteca': games_data, 'desejos': wishlist_data, 'perfil': profile_data,
            'conquistas_concluidas': completed_achievements,
            'conquistas_pendentes': pending_achievements
        }
    except Exception as e:
        print(f"Erro ao buscar dados: {e}"); traceback.print_exc()
        return { 'estatisticas': {}, 'biblioteca': [], 'desejos': [], 'perfil': {}, 'conquistas_concluidas': [], 'conquistas_pendentes': [] }

# O restante das funções (update_profile, add_game, etc.) não precisa de alteração
def update_profile_in_sheet(profile_data): # ... (sem alterações)
def add_game_to_sheet(game_data): # ... (sem alterações)
def add_wish_to_sheet(wish_data): # ... (sem alterações)
def update_game_in_sheet(game_name, updated_data): # ... (sem alterações)
def delete_game_from_sheet(game_name): # ... (sem alterações)
def update_wish_in_sheet(wish_name, updated_data): # ... (sem alterações)
def delete_wish_from_sheet(wish_name): # ... (sem alterações)
