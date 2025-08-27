import gspread
import pandas as pd
import json
import math
from oauth2client.service_account import ServiceAccountCredentials
from config import Config
from datetime import datetime, timedelta
import traceback
import requests
import deepl
import pytz
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

GENRE_TRANSLATIONS = {
    "Action": "Ação", "Indie": "Indie", "Adventure": "Aventura",
    "RPG": "RPG", "Strategy": "Estratégia", "Shooter": "Tiro",
    "Casual": "Casual", "Simulation": "Simulação", "Puzzle": "Puzzle",
    "Arcade": "Arcade", "Platformer": "Plataforma", "Racing": "Corrida",
    "Massively Multiplayer": "MMO", "Sports": "Esportes", "Fighting": "Luta",
    "Family": "Família", "Board Games": "Jogos de Tabuleiro", "Educational": "Educacional",
    "Card": "Cartas"
}

# --- Cache global para planilhas e dados ---
_sheet_cache = {}
_data_cache = {}
_cache_ttl_seconds = 300 # Tempo de vida do cache em segundos (5 minutos)
_last_cache_update = {}

def _get_sheet(sheet_name):
    """Retorna o objeto da planilha, usando cache."""
    if sheet_name in _sheet_cache:
        print(f"DEBUG: Planilha '{sheet_name}' encontrada no cache de sheets.")
        return _sheet_cache[sheet_name]
    try:
        print(f"DEBUG: Tentando autenticar e abrir planilha '{sheet_name}'.")
        
        print(f"DEBUG: Config.GAME_SHEET_URL: {Config.GAME_SHEET_URL}")
        if not Config.GOOGLE_SHEETS_CREDENTIALS_JSON:
            print("CRITICAL ERROR: GOOGLE_SHEETS_CREDENTIALS_JSON não está definida em Config.")
            return None
        
        creds_json = json.loads(Config.GOOGLE_SHEETS_CREDENTIALS_JSON)
        print("DEBUG: GOOGLE_SHEETS_CREDENTIALS_JSON lida com sucesso (conteúdo não exibido por segurança).")

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open_by_url(Config.GAME_SHEET_URL)
        worksheet = spreadsheet.worksheet(sheet_name)
        _sheet_cache[sheet_name] = worksheet
        print(f"DEBUG: Planilha '{sheet_name}' aberta com sucesso.")
        return worksheet
    except Exception as e:
        print(f"ERRO CRÍTICO: Falha ao autenticar ou abrir planilha '{sheet_name}': {e}"); traceback.print_exc()
        return None

def _get_data_from_sheet(sheet_name):
    """Retorna os dados da planilha, usando cache com TTL."""
    current_time = datetime.now()
    if sheet_name in _data_cache and \
       (current_time - _last_cache_update.get(sheet_name, datetime.min)).total_seconds() < _cache_ttl_seconds:
        print(f"DEBUG: Dados da planilha '{sheet_name}' servidos do cache de dados.")
        return _data_cache[sheet_name]

    sheet = _get_sheet(sheet_name)
    if not sheet:
        print(f"DEBUG: Não foi possível obter o objeto da planilha para '{sheet_name}', retornando lista vazia.")
        return []

    try:
        print(f"DEBUG: Tentando ler todos os registros da planilha '{sheet_name}'.")
        data = sheet.get_all_records()
        
        print(f"DEBUG: Dados brutos de '{sheet_name}' (primeiros 5 registros): {data[:5]}")
        if data:
            print(f"DEBUG: Cabeçalhos da planilha '{sheet_name}': {list(data[0].keys())}")
        else:
            print(f"DEBUG: Planilha '{sheet_name}' retornou dados vazios.")

        _data_cache[sheet_name] = data
        _last_cache_update[sheet_name] = current_time
        print(f"DEBUG: Dados da planilha '{sheet_name}' atualizados do Google Sheets e armazenados em cache. Total de registros: {len(data)}")
        return data
    except gspread.exceptions.APIError as e:
        if "unable to parse range" in str(e): 
            print(f"AVISO: Planilha '{sheet_name}' vazia ou com erro de range, retornando lista vazia. Detalhes: {e}")
            return []
        print(f"ERRO: Erro ao ler dados da planilha '{sheet_name}': {e}"); traceback.print_exc()
        return []
    except Exception as e:
        print(f"ERRO GENÉRICO: Erro ao ler dados da planilha '{sheet_name}': {e}"); traceback.print_exc()
        return []

def _invalidate_cache(sheet_name):
    """Invalida o cache para uma planilha específica."""
    if sheet_name in _data_cache:
        del _data_cache[sheet_name]
    print(f"DEBUG: Cache para a planilha '{sheet_name}' invalidado.")

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
        'JOGOS_LONGOS': len([g for g in games_data if g.get('Tempo de Jogo') and int(str(g['Tempo de Jogo']).replace('h', '')) >= 50]),
        'SOULSLIKE_PLATINADOS': len([g for g in games_data if g.get('Platinado?') == 'Sim' and 'Soulslike' in g.get('Estilo', '')]),
        'INDIE_TOTAL': len([g for g in games_data if 'Indie' in g.get('Estilo', '')]),
        'JOGO_MAIS_JOGADO': stats.get('max_horas_um_jogo', 0),
        'FINALIZADOS_ACAO': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Ação' in g.get('Estilo', '')]),
        'FINALIZADOS_ESTRATEGIA': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Estratégia' in g.get('Estilo', '')]),
        'GENEROS_DIFERENTES': len(set(g for game in games_data if game.get('Estilo') for g in game.get('Estilo').split(','))),
        'NOTAS_10': len([n for n in [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')] if n == 100]),
        'NOTAS_BAIXAS': len([n for n in [float(str(g.get('Nota', 0)).replace(',', '.') ) for g in games_data if g.get('Nota')] if n <= 30]),
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
            if nota > 0: total_exp += int(nota)
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

# --- Funções para gerenciar notificações ---
def _get_notifications_sheet():
    """Retorna o objeto da aba de notificações."""
    return _get_sheet('Notificações')

def _add_notification(notification_type, message, link_target=None):
    """Adiciona uma nova notificação à planilha, incluindo um link de destino."""
    sheet = _get_notifications_sheet()
    if not sheet:
        print("ERRO: Conexão com a planilha de notificações falhou ao tentar adicionar notificação.")
        return {"success": False, "message": "Conexão com a planilha de notificações falhou."}

    notifications = _get_data_from_sheet('Notificações')
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    current_time = datetime.now(brasilia_tz)

    for notif in notifications:
        if notif.get('Tipo') == notification_type and notif.get('Mensagem') == message:
            print(f"DEBUG: Notificação duplicada evitada: Tipo='{notification_type}', Mensagem='{message}'")
            return {"success": False, "message": "Notificação duplicada evitada."}

    new_id = len(notifications) + 1
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    link_value = link_target if link_target is not None else ''

    row_data = [new_id, notification_type, message, timestamp, 'Não', link_value]
    sheet.append_row(row_data)
    _invalidate_cache('Notificações') 
    print(f"DEBUG: Notificação adicionada: ID={new_id}, Tipo='{notification_type}', Mensagem='{message}', Link='{link_value}'")
    return {"success": True, "message": "Notificação adicionada com sucesso."}

def get_all_notifications_for_frontend():
    """Retorna TODAS as notificações (lidas e não lidas) para o frontend."""
    notifications = _get_data_from_sheet('Notificações') 
    
    processed_notifications = []
    for notif in notifications:
        display_message = notif.get('Mensagem', '')
        if "(Marco:" in display_message:
            display_message = display_message.split("(Marco:")[0].strip()

        processed_notif = {
            'ID': int(notif.get('ID', 0)),
            'Tipo': notif.get('Tipo', ''),
            'Mensagem': display_message, 
            'Data': notif.get('Data', ''),
            'Lida': str(notif.get('Lida', 'Não')),
            'Link': notif.get('Link', '') 
        }
        processed_notifications.append(processed_notif)
    
    processed_notifications.sort(key=lambda x: datetime.strptime(x['Data'], "%Y-%m-%d %H:%M:%S"), reverse=True)

    return processed_notifications

def mark_notification_as_read(notification_id):
    """Marca uma notificação específica como lida."""
    sheet = _get_notifications_sheet()
    if not sheet:
        print("ERRO: Conexão com a planilha de notificações falhou ao tentar marcar como lida.")
        return {"success": False, "message": "Conexão com a planilha de notificações falhou."}
    
    try:
        all_records = sheet.get_all_values()
        headers = all_records[0]
        data_rows = all_records[1:]

        id_col_index = headers.index('ID')
        lida_col_index = headers.index('Lida')

        found_row_index = -1
        for i, row in enumerate(data_rows):
            if str(row[id_col_index]) == str(notification_id):
                found_row_index = i + 2
                break
        
        if found_row_index == -1:
            print(f"ERRO: Notificação com ID {notification_id} não encontrada na planilha.")
            return {"success": False, "message": "Notificação não encontrada."}

        sheet.update_cell(found_row_index, lida_col_index + 1, 'Sim')
        _invalidate_cache('Notificações') 
        print(f"DEBUG: Notificação {notification_id} marcada como lida na planilha. Linha: {found_row_index}, Coluna Lida: {lida_col_index + 1}")
        return {"success": True, "message": f"Notificação {notification_id} marcada como lida."}
    except ValueError:
        print("ERRO: Colunas 'ID' ou 'Lida' não encontradas na planilha de Notificações.")
        return {"success": False, "message": "Erro: Colunas necessárias não encontradas."}
    except Exception as e:
        print(f"ERRO ao marcar notificação {notification_id} como lida: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar notificação."}

# --- FIM DAS Funções de Notificação ---

def get_price_history_for_game(game_name: str):
    """
    Retorna o histórico de preços para um jogo específico da aba 'Historico de Preços'.
    """
    try:
        history_data = _get_data_from_sheet('Historico de Preços')
        if not history_data:
            return []

        game_history = [
            {'date': item.get('Data'), 'platform': item.get('Plataforma'), 'price': float(str(item.get('Preço', 0)).replace(',', '.'))}
            for item in history_data if item.get('Nome do Jogo') == game_name and item.get('Preço') not in ['Não encontrado', 'Gratuito', None, '']
        ]
        
        game_history.sort(key=lambda x: datetime.strptime(x['date'], "%Y-%m-%d"))
        
        return game_history
    except Exception as e:
        print(f"ERRO: Erro ao obter histórico de preços para '{game_name}': {e}"); traceback.print_exc()
        return []

def _check_for_promotions(wish, existing_notifications, all_history_data):
    game_name = wish.get('Nome', 'Um jogo')
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    today_date = datetime.now(brasilia_tz).date()
    today_timestamp = pd.Timestamp(today_date)
    promotion_found = False

    game_history_raw = [
        item for item in all_history_data
        if item.get('Nome do Jogo') == game_name and item.get('Preço') not in ['Não encontrado', 'Gratuito', None, '']
    ]
    
    if not game_history_raw:
        return False

    df_history = pd.DataFrame(game_history_raw)
    df_history['Data'] = pd.to_datetime(df_history['Data'])
    df_history['Preço'] = df_history['Preço'].astype(str).str.replace(',', '.').astype(float)
    df_history = df_history.sort_values(by='Data')

    steam_history = df_history[df_history['Plataforma'] == 'Steam']
    psn_history = df_history[df_history['Plataforma'] == 'PSN']

    def check_platform_promotion(platform_name, history_df, current_price_str):
        nonlocal promotion_found
        if history_df.empty:
            return

        current_price_float = float(str(current_price_str).replace(',', '.')) if current_price_str not in ['Não encontrado', 'Gratuito', None, ''] else float('inf')
        if current_price_float == float('inf') or current_price_float == 0.0:
            return
        
        last_30_days_data = history_df[history_df['Data'] >= (today_timestamp - timedelta(days=30))]
        if not last_30_days_data.empty:
            average_price_30_days = last_30_days_data['Preço'].mean()
            if current_price_float <= average_price_30_days * 0.80:
                notification_message = f"Promoção na {platform_name}! '{game_name}' por R${current_price_float:.2f}."
                _add_notification("Promoção", notification_message, link_target=game_name)
                promotion_found = True
                return

    check_platform_promotion('Steam', steam_history, wish.get('Steam Preco Atual'))
    check_platform_promotion('PSN', psn_history, wish.get('PSN Preco Atual'))

    return promotion_found

def get_all_game_data():
    try:
        brasilia_tz = pytz.timezone('America/Sao_Paulo')
        current_time = datetime.now(brasilia_tz)
        game_sheet_data = _get_data_from_sheet('Jogos'); games_data = game_sheet_data if game_sheet_data else []
        wishlist_sheet_data = _get_data_from_sheet('Desejos'); all_wishlist_data = wishlist_sheet_data if wishlist_sheet_data else []
        
        def safe_float_conversion(value):
            try: return float(str(value).replace('R$', '').replace(',', '.').strip())
            except (ValueError, TypeError): return 0.0 

        processed_wishlist_data = [
            {**wish, 
             'Steam Preco Atual': safe_float_conversion(wish.get('Steam Preco Atual')),
             'Steam Menor Preco Historico': safe_float_conversion(wish.get('Steam Menor Preco Historico')),
             'PSN Preco Atual': safe_float_conversion(wish.get('PSN Preco Atual')),
             'PSN Menor Preco Historico': safe_float_conversion(wish.get('PSN Menor Preco Historico')),
             'Preço': safe_float_conversion(wish.get('Preço'))}
            for wish in all_wishlist_data
        ]

        wishlist_data_filtered = [item for item in processed_wishlist_data if item.get('Status') != 'Comprado']
        profile_sheet_data = _get_data_from_sheet('Perfil'); profile_records = profile_sheet_data if profile_sheet_data else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet_data = _get_data_from_sheet('Conquistas'); all_achievements = achievements_sheet_data if achievements_sheet_data else []
        
        existing_notifications = get_all_notifications_for_frontend()
        all_price_history_data = _get_data_from_sheet('Historico de Preços')

        def sort_key(game):
            try: nota = float(str(game.get('Nota', '-1')).replace(',', '.'))
            except (ValueError, TypeError): nota = -1
            return (-nota, game.get('Nome', '').lower())
        
        games_data.sort(key=sort_key)
        notas = [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')]
        tempos_de_jogo = [int(str(g.get('Tempo de Jogo', 0)).replace('h', '')) for g in games_data]
        
        base_stats = {
            'total_jogos': len(games_data), 'total_finalizados': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado']]),
            'total_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim']), 'total_avaliados': len([g for g in games_data if g.get('Nota') and float(str(g.get('Nota')).replace(',', '.')) > 0]),
            'total_horas_jogadas': sum(tempos_de_jogo), 'custo_total_biblioteca': sum([float(str(g.get('Preço', '0,00')).replace('R$', '').replace(',', '.')) for g in games_data]),
            'media_notas': sum(notas) / len(notas) if notas else 0, 'total_conquistas': sum([int(g.get('Conquistas Obtidas', 0)) for g in games_data]),
        }

        completed_achievements, pending_achievements = _check_achievements(games_data, base_stats, all_achievements, wishlist_data_filtered) 
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        final_stats = {**base_stats, **gamer_stats}

        for ach in completed_achievements:
            notification_message = f"Você desbloqueou a conquista: '{ach.get('Nome')}'!"
            _add_notification("Conquista Desbloqueada", notification_message, link_target=ach.get('ID'))
        
        today = current_time.replace(hour=0, minute=0, second=0, microsecond=0) 
        release_notification_milestones = [30, 15, 7, 3, 1, 0] 

        for wish in processed_wishlist_data: 
            release_date_str = wish.get('Data Lançamento')
            if release_date_str:
                try:
                    release_date = None
                    if '/' in release_date_str: release_date = datetime.strptime(release_date_str, "%d/%m/%Y")
                    elif '-' in release_date_str: release_date = datetime.strptime(release_date_str, "%Y-%m-%d")
                    if not release_date: continue 
                    release_date = brasilia_tz.localize(release_date.replace(hour=0, minute=0, second=0, microsecond=0))
                    days_to_release = (release_date - today).days
                    for milestone in release_notification_milestones:
                        if days_to_release == milestone:
                            if milestone == 0: display_message = f"O jogo '{wish.get('Nome')}' foi lançado hoje!"
                            elif milestone == 1: display_message = f"O jogo '{wish.get('Nome')}' será lançado amanhã!"
                            else: display_message = f"O jogo '{wish.get('Nome')}' será lançado em {milestone} dias!"
                            message_with_milestone = f"{display_message} (Marco: {milestone} dias)"
                            _add_notification("Lançamento Próximo", message_with_milestone, link_target=wish.get('Nome'))
                            break 
                except (ValueError, TypeError): continue
       
        for wish in wishlist_data_filtered: 
            _check_for_promotions(wish, existing_notifications, all_price_history_data)
            
        return {
            'estatisticas': final_stats, 'biblioteca': games_data, 'desejos': wishlist_data_filtered, 'perfil': profile_data,
            'conquistas_concluidas': completed_achievements, 'conquistas_pendentes': pending_achievements
        }
    except Exception as e:
        print(f"ERRO CRÍTICO: Erro ao buscar dados na função get_all_game_data: {e}"); traceback.print_exc()
        return { 'estatisticas': {}, 'biblioteca': [], 'desejos': [], 'perfil': {}, 'conquistas_concluidas': [], 'conquistas_pendentes': [] }

def get_public_profile_data():
    try:
        game_sheet_data = _get_data_from_sheet('Jogos'); games_data = game_sheet_data if game_sheet_data else []
        wishlist_sheet_data = _get_data_from_sheet('Desejos'); all_wishlist_data = wishlist_sheet_data if wishlist_sheet_data else []
        profile_sheet_data = _get_data_from_sheet('Perfil'); profile_records = profile_sheet_data if profile_sheet_data else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet_data = _get_data_from_sheet('Conquistas'); all_achievements = achievements_sheet_data if achievements_sheet_data else []
        tempos_de_jogo = [int(str(g.get('Tempo de Jogo', 0)).replace('h', '')) for g in games_data]
        notas = [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')]

        base_stats = {
            'total_jogos': len(games_data), 'total_finalizados': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado']]),
            'total_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim']), 'total_avaliados': len([g for g in games_data if g.get('Nota') and float(str(g.get('Nota')).replace(',', '.')) > 0]),
            'total_horas_jogadas': sum(tempos_de_jogo), 'custo_total_biblioteca': sum([float(str(g.get('Preço', '0,00')).replace('R$', '').replace(',', '.')) for g in games_data]),
            'media_notas': sum(notas) / len(notas) if notas else 0, 'total_conquistas': sum([int(g.get('Conquistas Obtidas', 0)) for g in games_data]),
            'WISHLIST_TOTAL': len(all_wishlist_data) 
        }

        completed_achievements, _ = _check_achievements(games_data, base_stats, all_achievements, all_wishlist_data)
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        public_stats = {**base_stats, **gamer_stats}
        
        recent_platinums = sorted([g for g in games_data if g.get('Platinado?') == 'Sim' and g.get('Link')], key=lambda x: x.get('Terminado em', '0000-00-00'), reverse=True)
        
        return {
            'perfil': profile_data, 'estatisticas': public_stats, 'ultimos_platinados': recent_platinums[:5]
        }
    except Exception as e:
        print(f"ERRO: Erro ao buscar dados do perfil público: {e}"); traceback.print_exc()
        return {'perfil': {}, 'estatisticas': {}, 'ultimos_platinados': []}

def update_profile_in_sheet(profile_data):
    try:
        sheet = _get_sheet('Perfil')
        if not sheet: return {"success": False, "message": "Conexão com a planilha de perfil falhou."}
        for key, value in profile_data.items():
            try:
                cell = sheet.find(key)
                sheet.update_cell(cell.row, cell.col + 1, value)
            except gspread.exceptions.CellNotFound:
                sheet.append_row([key, value])
        _invalidate_cache('Perfil') 
        return {"success": True, "message": "Perfil atualizado com sucesso."}
    except Exception as e:
        print(f"Erro ao atualizar perfil: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar perfil."}

def trigger_similar_games_scraper(game_title: str):
    """
    Aciona a GitHub Action para fazer o scraping de jogos similares
    para um jogo específico recém-adicionado.
    """
    owner = os.environ.get('SIMILAR_SCRAPER_OWNER')
    repo = os.environ.get('SIMILAR_SCRAPER_REPO')
    pat = os.environ.get('SIMILAR_SCRAPER_PAT')
    workflow_file = os.environ.get('SIMILAR_SCRAPER_WORKFLOW_FILE')

    if not all([owner, repo, pat, workflow_file]):
        print("CRITICAL: Variáveis de ambiente para o scraper de SIMILARES não configuradas.")
        return {"success": False, "message": "Configuração da API do GitHub (Similares) ausente no servidor."}

    url = f"https://api.github.com/repos/{owner}/{repo}/dispatches"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {pat}"
    }
    
    data = {
        "event_type": "scrape-new-game",
        "client_payload": {
            "game": game_title
        }
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 204:
            print(f"SUCESSO: Gatilho da Action de similares disparado para o jogo '{game_title}'.")
            return {"success": True, "message": f"Scraping de similares para '{game_title}' iniciado."}
        else:
            print(f"ERRO: Falha ao disparar a Action de similares. Status: {response.status_code}, Resposta: {response.text}")
            return {"success": False, "message": "Falha ao iniciar o scraping de similares."}
            
    except requests.exceptions.RequestException as e:
        print(f"ERRO de Conexão com a API do GitHub (Similares): {e}")
        traceback.print_exc()
        return {"success": False, "message": "Erro de comunicação com o GitHub."}

def add_game_to_sheet(game_data):
    try:
        rawg_id = game_data.get('RAWG_ID')
        if rawg_id and Config.RAWG_API_KEY:
            try:
                url = f"https://api.rawg.io/api/games/{rawg_id}?key={Config.RAWG_API_KEY}"
                response = requests.get(url)
                if response.ok:
                    details = response.json()
                    description = details.get('description_raw', '')
                    translated_description = description
                    if Config.DEEPL_API_KEY and description:
                        try:
                            translator = deepl.Translator(Config.DEEPL_API_KEY)
                            result = translator.translate_text(description, target_lang="PT-BR")
                            translated_description = result.text
                        except Exception as deepl_e:
                            print(f"ERRO: Erro ao traduzir com DeepL: {deepl_e}")
                    game_data['Descricao'] = translated_description
                    game_data['Metacritic'] = details.get('metacritic', '')
                    game_data['Screenshots'] = ', '.join([sc.get('image') for sc in details.get('short_screenshots', [])[:3]])
            except requests.exceptions.RequestException as e:
                print(f"ERRO: Erro ao buscar detalhes da RAWG para o ID {rawg_id}: {e}")

        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        headers = sheet.row_values(1)
        row_data = [game_data.get(header, '') for header in headers]
        sheet.append_row(row_data)
        _invalidate_cache('Jogos') 
        
        game_name = game_data.get('Nome')
        _add_notification("Novo Jogo Adicionado", f"Você adicionou '{game_name}' à sua biblioteca!", link_target=game_name)
        
        if game_name:
            trigger_similar_games_scraper(game_name)

        return {"success": True, "message": "Jogo adicionado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao adicionar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar jogo."}
        
def add_wish_to_sheet(wish_data):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        headers = sheet.row_values(1)
        row_data = {header: wish_data.get(header, '') for header in headers}
        sheet.append_row(list(row_data.values()))
        _invalidate_cache('Desejos') 
        _add_notification("Novo Desejo Adicionado", f"Você adicionou '{wish_data.get('Nome')}' à sua lista de desejos!", link_target=wish_data.get('Nome'))
        return {"success": True, "message": "Item de desejo adicionado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao adicionar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar item de desejo."}
        
def update_game_in_sheet(game_name, updated_data):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        try: 
            cell = sheet.find(game_name)
        except gspread.exceptions.CellNotFound: 
            return {"success": False, "message": "Jogo não encontrado."}
        
        all_records = _get_data_from_sheet('Jogos')
        normalized_records = [{k.strip(): v for k, v in record.items()} for record in all_records]
        
        game_to_update = next((record for record in normalized_records if record.get('Nome') == game_name), None)

        if not game_to_update:
            return {"success": False, "message": "Erro ao encontrar os dados do jogo para preservar."}
            
        merged_data = {**game_to_update, **updated_data}

        headers = [h.strip() for h in sheet.row_values(1)]
        new_row = [merged_data.get(header, '') for header in headers]
        
        sheet.update(f'A{cell.row}', [new_row])
        _invalidate_cache('Jogos') 
        
        return {"success": True, "message": "Jogo atualizado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao atualizar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar jogo."}
        
def delete_game_from_sheet(game_name):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(game_name)
        sheet.delete_rows(cell.row)
        _invalidate_cache('Jogos') 
        _add_notification("Jogo Removido", f"O jogo '{game_name}' foi removido da sua biblioteca.", link_target=game_name)
        return {"success": True, "message": "Jogo deletado com sucesso."}
    except gspread.exceptions.CellNotFound:
        return {"success": False, "message": "Jogo não encontrado."}
    except Exception as e:
        print(f"ERRO: Erro ao deletar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar jogo."}
        
def update_wish_in_sheet(wish_name, updated_data):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(wish_name)
        headers = sheet.row_values(1)
        new_row = [updated_data.get(header, '') for header in headers]
        sheet.update(f'A{cell.row}', [new_row])
        _invalidate_cache('Desejos') 
        return {"success": True, "message": "Item de desejo atualizado com sucesso."}
    except gspread.exceptions.CellNotFound:
        return {"success": False, "message": "Item de desejo não encontrado."}
    except Exception as e:
        print(f"ERRO: Erro ao atualizar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar item de desejo."}

def delete_wish_from_sheet(wish_name):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(wish_name)
        sheet.delete_rows(cell.row)
        _invalidate_cache('Desejos') 
        _add_notification("Desejo Removido", f"O item '{wish_name}' foi removido da sua lista de desejos.", link_target=wish_name)
        return {"success": True, "message": "Item de desejo deletado com sucesso."}
    except gspread.exceptions.CellNotFound:
        return {"success": False, "message": "Item de desejo não encontrado."}
    except Exception as e:
        print(f"ERRO: Erro ao deletar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar item de desejo."}

def purchase_wish_item_in_sheet(item_name):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(item_name)
        headers = sheet.row_values(1)
        status_col_index = headers.index('Status') + 1
        sheet.update_cell(cell.row, status_col_index, 'Comprado')
        _invalidate_cache('Desejos') 
        _add_notification("Desejo Comprado", f"Você marcou '{item_name}' como comprado! Aproveite o jogo!", link_target=item_name)
        return {"success": True, "message": "Item marcado como comprado!"}
    except gspread.exceptions.CellNotFound:
        return {"success": False, "message": "Item de desejo não encontrado."}
    except ValueError:
        return {"success": False, "message": "Coluna 'Status' não encontrada."}
    except Exception as e:
        print(f"ERRO: Erro ao marcar item como comprado: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao processar a compra."}

def trigger_wishlist_scraper_action():
    try:
        github_pat = os.environ.get('GITHUB_PAT')
        repo_owner = os.environ.get('GITHUB_OWNER')
        repo_name = os.environ.get('GITHUB_REPO')
        workflow_file = os.environ.get('GITHUB_WORKFLOW_FILE_NAME')

        if not all([github_pat, repo_owner, repo_name, workflow_file]):
            return {"success": False, "message": "Configuração da API do GitHub ausente."}

        url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_file}/dispatches'
        headers = {'Accept': 'application/vnd.github.com+json', 'Authorization': f'token {github_pat}'}
        data = { 'ref': 'main' }
        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 204:
            return {"success": True, "message": "Atualização de preços iniciada com sucesso!"}
        else:
            return {"success": False, "message": f"Erro na API do GitHub: {response.text}"}
    except Exception as e:
        return {"success": False, "message": f"Erro: {e}"}

def get_random_game(plataforma=None, estilo=None, metacritic_min=None, metacritic_max=None):
    try:
        games_data = _get_data_from_sheet('Jogos')
        if not games_data: return None

        df = pd.DataFrame(games_data)
        df_filtered = df.dropna(subset=['Status'])
        jogos_elegiveis = df_filtered[~df_filtered['Status'].isin(['Platinado', 'Abandonado', 'Finalizado'])]

        if plataforma:
            jogos_elegiveis = jogos_elegiveis[jogos_elegiveis['Plataforma'].str.lower() == plataforma.lower()]
        if estilo:
            jogos_elegiveis = jogos_elegiveis[jogos_elegiveis['Estilo'].str.contains(estilo, case=False, na=False)]
        if metacritic_min:
            jogos_elegiveis['Metacritic'] = pd.to_numeric(jogos_elegiveis['Metacritic'], errors='coerce').fillna(0)
            jogos_elegiveis = jogos_elegiveis[jogos_elegiveis['Metacritic'] >= int(metacritic_min)]
        if metacritic_max:
            jogos_elegiveis['Metacritic'] = pd.to_numeric(jogos_elegiveis['Metacritic'], errors='coerce').fillna(0)
            jogos_elegiveis = jogos_elegiveis[jogos_elegiveis['Metacritic'] <= int(metacritic_max)]

        if not jogos_elegiveis.empty:
            return jogos_elegiveis.sample(n=1).to_dict(orient='records')[0]
        
        return None
    except Exception as e:
        print(f"ERRO na função get_random_game: {e}"); traceback.print_exc()
        return None

def get_image_for_game(game_info):
    """Função auxiliar para buscar uma única imagem na API da RAWG."""
    game_name_to_search = game_info.get('name')
    if not game_name_to_search:
        return game_info['row_num'], ''
    
    print(f"[API THREAD] Buscando imagem para '{game_name_to_search}'...")
    try:
        search_url = f"https://api.rawg.io/api/games?key={Config.RAWG_API_KEY}&search={requests.utils.quote(game_name_to_search)}&page_size=1"
        response = requests.get(search_url, timeout=10)
        response.raise_for_status()
        search_data = response.json()
        
        if search_data.get('results'):
            image_url = search_data['results'][0].get('background_image', '')
            return game_info['row_num'], image_url
    except requests.exceptions.RequestException as e:
        print(f"!!! ERRO de API ao buscar imagem para '{game_name_to_search}': {e}")
    
    return game_info['row_num'], ''

def get_similar_games_from_sheet(base_game_name: str):
    """
    Busca jogos similares na planilha. Se algum não tiver imagem, busca na API da RAWG
    de forma concorrente e atualiza a planilha antes de retornar os dados.
    """
    try:
        similar_sheet = _get_sheet('Jogos Similares')
        if not similar_sheet: return []

        all_rows = similar_sheet.get_all_values()
        if len(all_rows) < 2: return []

        header = all_rows[0]
        try:
            base_col_idx = header.index('Jogo Base')
            similar_col_idx = header.index('Jogo Similar')
            image_col_idx = header.index('Imagem')
        except ValueError:
            print("ERRO: Colunas essenciais ('Jogo Base', 'Jogo Similar', 'Imagem') não encontradas.")
            return []

        games_to_enrich = []
        games_for_frontend = []
        
        for i, row in enumerate(all_rows[1:]):
            row_num = i + 2
            if len(row) > base_col_idx and row[base_col_idx] == base_game_name:
                game_dict = {header[j]: (row[j] if j < len(row) else '') for j in range(len(header))}
                games_for_frontend.append(game_dict)
                
                image_url = row[image_col_idx] if len(row) > image_col_idx and row[image_col_idx] else ''
                if not image_url and game_dict.get('Jogo Similar'):
                    games_to_enrich.append({'name': game_dict.get('Jogo Similar'), 'row_num': row_num})

        if games_to_enrich:
            updates_to_perform = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_game = {executor.submit(get_image_for_game, game_info): game_info for game_info in games_to_enrich}
                for future in as_completed(future_to_game):
                    row_num, image_url = future.result()
                    if image_url:
                        updates_to_perform.append({
                            'range': f'F{row_num}',
                            'values': [[image_url]]
                        })
                        for game in games_for_frontend:
                            if game.get('Jogo Similar') == future_to_game[future]['name']:
                                game['Imagem'] = image_url
                                break
            
            if updates_to_perform:
                print(f"Atualizando {len(updates_to_perform)} URL(s) de imagem na planilha...")
                similar_sheet.batch_update(updates_to_perform, value_input_option='USER_ENTERED')
                _invalidate_cache('Jogos Similares')

        return games_for_frontend

    except Exception as e:
        print(f"!!! ERRO GERAL em get_similar_games_from_sheet: {e}")
        traceback.print_exc()
        return []

# Em services/game_service.py, adicione estas duas funções no final do arquivo

def get_steam_library():
    """
    Busca a biblioteca de jogos da Steam do usuário, compara com a planilha
    e retorna uma lista de jogos novos e jogos a serem atualizados.
    """
    if not Config.STEAM_API_KEY or not Config.STEAM_USER_ID:
        return {"error": "Credenciais da Steam não configuradas no servidor."}

    try:
        # 1. Buscar jogos da Steam
        steam_url = f"http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={Config.STEAM_API_KEY}&steamid={Config.STEAM_USER_ID}&format=json&include_appinfo=true"
        response = requests.get(steam_url)
        response.raise_for_status()
        steam_data = response.json().get('response', {}).get('games', [])

        steam_games = {
            game['name']: {
                'appid': game['appid'],
                'playtime_forever': game.get('playtime_forever', 0),
                'name': game['name']
            } for game in steam_data if game.get('playtime_forever', 0) > 0 # Ignora jogos não jogados
        }

        # 2. Buscar jogos da planilha
        library_games = _get_data_from_sheet('Jogos')
        library_game_names = {game.get('Nome').lower() for game in library_games}

        # 3. Comparar e separar as listas
        new_games = []
        games_to_update = []

        for name, steam_game_data in steam_games.items():
            playtime_hours = round(steam_game_data['playtime_forever'] / 60)
            
            game_payload = {
                'name': name,
                'playtime': f"{playtime_hours}h",
                'achievements': 'N/A', # API de conquistas é separada e mais complexa
                'appid': steam_game_data['appid']
            }

            if name.lower() in library_game_names:
                games_to_update.append(game_payload)
            else:
                new_games.append(game_payload)

        # Ordena as listas alfabeticamente
        new_games.sort(key=lambda x: x['name'])
        games_to_update.sort(key=lambda x: x['name'])

        return {"new_games": new_games, "games_to_update": games_to_update}

    except requests.exceptions.RequestException as e:
        print(f"ERRO ao buscar dados da Steam: {e}")
        return {"error": "Falha ao comunicar com a API da Steam."}
    except Exception as e:
        print(f"ERRO em get_steam_library: {e}"); traceback.print_exc()
        return {"error": "Ocorreu um erro interno ao processar a biblioteca da Steam."}


# Em services/game_service.py, adicione estas duas funções no final do arquivo

def get_steam_library():
    """
    Busca a biblioteca de jogos da Steam, enriquecendo com conquistas e capas,
    compara com a planilha e retorna uma lista de jogos novos e a serem atualizados.
    """
    if not Config.STEAM_API_KEY or not Config.STEAM_USER_ID:
        return {"error": "Credenciais da Steam não configuradas no servidor."}

    try:
        steam_url = f"http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={Config.STEAM_API_KEY}&steamid={Config.STEAM_USER_ID}&format=json&include_appinfo=true"
        response = requests.get(steam_url)
        response.raise_for_status()
        steam_games_raw = response.json().get('response', {}).get('games', [])
        
        steam_games_filtered = [game for game in steam_games_raw if game.get('playtime_forever', 0) > 0]

        library_games = _get_data_from_sheet('Jogos')
        library_map = {game.get('Nome').lower(): game for game in library_games}

        new_games = []
        games_to_update = []

        def enrich_game_data(game):
            appid = game['appid']
            name = game['name']
            playtime_hours = round(game.get('playtime_forever', 0) / 60)
            
            # Busca conquistas
            achievements_count = "N/A"
            try:
                ach_url = f"http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid={appid}&key={Config.STEAM_API_KEY}&steamid={Config.STEAM_USER_ID}"
                ach_response = requests.get(ach_url, timeout=5).json()
                if ach_response.get('playerstats', {}).get('success') and 'achievements' in ach_response['playerstats']:
                    achievements_count = len(ach_response['playerstats']['achievements'])
            except Exception:
                pass # Ignora se a busca de conquistas falhar

            game_payload = {
                'name': name,
                'playtime_steam': f"{playtime_hours}h",
                'achievements_steam': achievements_count,
                'appid': appid,
                'cover_image': f"https://steamcdn-a.akamaihd.net/steam/apps/{appid}/header.jpg"
            }

            if name.lower() in library_map:
                existing_game = library_map[name.lower()]
                game_payload['playtime_local'] = f"{existing_game.get('Tempo de Jogo', 0)}h"
                game_payload['achievements_local'] = existing_game.get('Conquistas Obtidas', 0)
                return 'update', game_payload
            else:
                return 'new', game_payload

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(enrich_game_data, game) for game in steam_games_filtered]
            for future in as_completed(futures):
                result_type, payload = future.result()
                if result_type == 'new':
                    new_games.append(payload)
                else:
                    games_to_update.append(payload)

        new_games.sort(key=lambda x: x['name'])
        games_to_update.sort(key=lambda x: x['name'])

        return {"new_games": new_games, "games_to_update": games_to_update}

    except requests.exceptions.RequestException as e:
        return {"error": f"Falha ao comunicar com a API da Steam: {e}"}
    except Exception as e:
        traceback.print_exc()
        return {"error": "Ocorreu um erro interno ao processar a biblioteca da Steam."}


def sync_steam_games(games_to_sync):
    """
    Recebe uma lista de jogos selecionados, enriquece com dados da RAWG/DeepL
    e adiciona/atualiza na planilha 'Jogos'.
    """
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        all_library_games = sheet.get_all_records()
        library_map = {game.get('Nome').lower(): game for game in all_library_games}
        
        added_count = 0
        updated_count = 0

        for game in games_to_sync:
            game_name = game.get('name')
            
            rawg_data = {}
            if Config.RAWG_API_KEY:
                try:
                    search_url = f"https://api.rawg.io/api/games?key={Config.RAWG_API_KEY}&search={requests.utils.quote(game_name)}&page_size=1"
                    rawg_response = requests.get(search_url).json().get('results', [])
                    if rawg_response:
                        rawg_id = rawg_response[0].get('id')
                        details_url = f"https://api.rawg.io/api/games/{rawg_id}?key={Config.RAWG_API_KEY}"
                        details_response = requests.get(details_url).json()
                        
                        description = details_response.get('description_raw', '')
                        translated_description = description
                        if Config.DEEPL_API_KEY and description:
                            try:
                                translator = deepl.Translator(Config.DEEPL_API_KEY)
                                result = translator.translate_text(description, target_lang="PT-BR")
                                translated_description = result.text
                            except Exception: pass

                        rawg_data = {
                            'RAWG_ID': rawg_id,
                            'Estilo': ', '.join([GENRE_TRANSLATIONS.get(g['name'], g['name']) for g in details_response.get('genres', [])]),
                            'Metacritic': details_response.get('metacritic', ''),
                            'Descricao': translated_description
                        }
                except Exception as rawg_e:
                    print(f"Erro ao buscar dados da RAWG para '{game_name}': {rawg_e}")

            if game_name.lower() in library_map:
                # ATUALIZA JOGO EXISTENTE
                updated_data = {
                    'Tempo de Jogo': int(game.get('playtime_steam', '0h').replace('h','')),
                    'Conquistas Obtidas': game.get('achievements_steam', 0)
                }
                update_game_in_sheet(game_name, updated_data)
                updated_count += 1
            else:
                # ADICIONA NOVO JOGO
                new_game_data = {
                    'Nome': game_name,
                    'Plataforma': 'PC',
                    'Status': 'Na Fila',
                    'Tempo de Jogo': int(game.get('playtime_steam', '0h').replace('h','')),
                    'Conquistas Obtidas': game.get('achievements_steam', 0),
                    'Link': game.get('cover_image', ''),
                    **rawg_data
                }
                add_game_to_sheet(new_game_data)
                added_count += 1
        
        _invalidate_cache('Jogos')
        return {"success": True, "message": f"{added_count} jogos adicionados e {updated_count} atualizados com sucesso!"}

    except Exception as e:
        print(f"ERRO em sync_steam_games: {e}"); traceback.print_exc()
        return {"success": False, "message": "Ocorreu um erro durante a sincronização."}