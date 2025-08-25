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
        creds_json = json.loads(Config.GOOGLE_SHEETS_CREDENTIALS_JSON)
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

def _add_notification(notification_type, message_to_save, message_for_display=None, game_name=None):
    """Adiciona uma nova notificação à planilha, evitando duplicatas recentes ou re-notificando promoções após um período.
        message_to_save: A mensagem completa com o marco (para desduplicação).
        message_for_display: A mensagem sem o marco (para exibição no frontend).
        game_name: O nome do jogo, usado para desduplicação de promoções.
    """
    sheet = _get_notifications_sheet()
    if not sheet:
        print("ERRO: Conexão com a planilha de notificações falhou ao tentar adicionar notificação.")
        return {"success": False, "message": "Conexão com a planilha de notificações falhou."}

    notifications = _get_data_from_sheet('Notificações') # Busca do cache ou da planilha
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    current_time = datetime.now(brasilia_tz)

    if notification_type == "Promoção" and game_name:
        existing_promotions = [
            n for n in notifications
            if n.get('Tipo') == "Promoção" and game_name in n.get('Mensagem', '') 
        ]

        if existing_promotions:
            latest_promotion_date = None
            for notif in existing_promotions:
                try:
                    notif_date = brasilia_tz.localize(datetime.strptime(notif.get('Data'), "%Y-%m-%d %H:%M:%S"))
                    if latest_promotion_date is None or notif_date > latest_promotion_date:
                        latest_promotion_date = notif_date
                except (ValueError, TypeError):
                    continue

            if latest_promotion_date and (current_time - latest_promotion_date).days < 30:
                print(f"DEBUG: Notificação de promoção para '{game_name}' evitada (já notificada há menos de 30 dias).")
                return {"success": False, "message": "Notificação de promoção duplicada evitada."}
    else:
        for notif in notifications:
            if notif.get('Tipo') == notification_type and \
               notif.get('Mensagem') == message_to_save:
                print(f"DEBUG: Notificação duplicada evitada: Tipo='{notification_type}', Mensagem='{message_to_save}'")
                return {"success": False, "message": "Notificação duplicada evitada."}

    new_id = len(notifications) + 1
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    final_message_to_save = message_to_save
    final_message_for_display = message_for_display if message_for_display is not None else message_to_save

    row_data = [new_id, notification_type, final_message_to_save, timestamp, 'Não']
    sheet.append_row(row_data)
    _invalidate_cache('Notificações') 
    print(f"DEBUG: Notificação adicionada: ID={new_id}, Tipo='{notification_type}', Mensagem='{final_message_to_save}' (Exibição: '{final_message_for_display}')")
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
            'Lida': str(notif.get('Lida', 'Não'))
        }
        processed_notifications.append(processed_notif)
    
    processed_notifications.sort(key=lambda x: datetime.strptime(x['Data'], "%Y-%m-%d %H:%M:%S"), reverse=True)

    print(f"DEBUG: Total de notificações (lidas e não lidas) encontradas para o frontend: {len(processed_notifications)}")
    for i, notif in enumerate(processed_notifications[:5]):
        print(f"DEBUG:   Notificação {notif['ID']} - Tipo: {notif['Tipo']}, Lida: '{notif['Lida']}', Mensagem Display: '{notif['Mensagem']}'")
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

# --- NOVA FUNÇÃO: Obter histórico de preços para um jogo ---
def get_price_history_for_game(game_name: str):
    """
    Retorna o histórico de preços para um jogo específico da aba 'Historico de Preços'.
    """
    try:
        history_data = _get_data_from_sheet('Historico de Preços')
        if not history_data:
            return []

        # Filtrar por nome do jogo
        game_history = [
            {'date': item.get('Data'), 'platform': item.get('Plataforma'), 'price': float(str(item.get('Preço', 0)).replace(',', '.'))}
            for item in history_data if item.get('Nome do Jogo') == game_name and item.get('Preço') not in ['Não encontrado', 'Gratuito', None, '']
        ]
        
        # Ordenar por data
        game_history.sort(key=lambda x: datetime.strptime(x['date'], "%Y-%m-%d"))
        
        return game_history
    except Exception as e:
        print(f"ERRO: Erro ao obter histórico de preços para '{game_name}': {e}"); traceback.print_exc()
        return []

# --- NOVA LÓGICA DE PROMOÇÃO ---
def _check_for_promotions(wish, existing_notifications, all_history_data):
    """
    Verifica se um jogo está em promoção com base no histórico de preços.
    Regras:
    1. Preço atual está 20% abaixo da média dos últimos 30 dias.
    2. Queda de 10% ou mais em relação à semana anterior.
    """
    game_name = wish.get('Nome', 'Um jogo')
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    today = datetime.now(brasilia_tz).date()

    promotion_found = False

    # Filtrar histórico para o jogo atual
    game_history_raw = [
        item for item in all_history_data
        if item.get('Nome do Jogo') == game_name and item.get('Preço') not in ['Não encontrado', 'Gratuito', None, '']
    ]
    
    if not game_history_raw:
        return False # Sem histórico para verificar promoções

    # Converter para DataFrame para facilitar a manipulação
    df_history = pd.DataFrame(game_history_raw)
    df_history['Data'] = pd.to_datetime(df_history['Data'])
    df_history['Preço'] = df_history['Preço'].astype(str).str.replace(',', '.').astype(float)
    df_history = df_history.sort_values(by='Data')

    # Filtrar por Steam e PSN para análise separada
    steam_history = df_history[df_history['Plataforma'] == 'Steam']
    psn_history = df_history[df_history['Plataforma'] == 'PSN']

    def check_platform_promotion(platform_name, history_df, current_price_str):
        nonlocal promotion_found
        if history_df.empty:
            return

        current_price_float = float(str(current_price_str).replace(',', '.')) if current_price_str not in ['Não encontrado', 'Gratuito', None, ''] else float('inf')
        if current_price_float == float('inf') or current_price_float == 0.0: # Não verifica se não tem preço ou é gratuito
            return
        
        # Últimos 30 dias
        last_30_days_data = history_df[history_df['Data'] >= (today - timedelta(days=30))]
        if not last_30_days_data.empty:
            average_price_30_days = last_30_days_data['Preço'].mean()
            if current_price_float <= average_price_30_days * 0.80: # 20% abaixo da média
                notification_message = f"Promoção na {platform_name}! '{game_name}' está R${current_price_float:.2f}, 20% abaixo da média dos últimos 30 dias."
                _add_notification("Promoção", notification_message, game_name=game_name)
                promotion_found = True
                print(f"DEBUG: Promoção detectada (média 30 dias) para {game_name} na {platform_name}.")
                return

        # Queda em relação à semana anterior
        one_week_ago = today - timedelta(days=7)
        price_one_week_ago_df = history_df[history_df['Data'].dt.date == one_week_ago]
        if not price_one_week_ago_df.empty:
            price_one_week_ago = price_one_week_ago_df['Preço'].iloc[-1] # Último preço registrado uma semana atrás
            if current_price_float <= price_one_week_ago * 0.90: # Queda de 10% ou mais
                notification_message = f"Queda de Preço na {platform_name}! '{game_name}' está R${current_price_float:.2f}, caiu 10% ou mais desde a semana passada."
                _add_notification("Promoção", notification_message, game_name=game_name)
                promotion_found = True
                print(f"DEBUG: Promoção detectada (queda semanal) para {game_name} na {platform_name}.")
                return
    
    # Chama a função para Steam e PSN
    check_platform_promotion('Steam', steam_history, wish.get('Steam Preco Atual'))
    check_platform_promotion('PSN', psn_history, wish.get('PSN Preco Atual'))

    return promotion_found

def get_all_game_data():
    try:
        print("DEBUG: Iniciando get_all_game_data.")
        brasilia_tz = pytz.timezone('America/Sao_Paulo')
        current_time = datetime.now(brasilia_tz)

        game_sheet_data = _get_data_from_sheet('Jogos'); games_data = game_sheet_data if game_sheet_data else []
        print(f"DEBUG: Dados de 'Jogos' carregados. Total: {len(games_data)}")

        wishlist_sheet_data = _get_data_from_sheet('Desejos')
        all_wishlist_data = wishlist_sheet_data if wishlist_sheet_data else []
        
        def safe_float_conversion(value):
            try:
                cleaned_value = str(value).replace('R$', '').replace(',', '.').strip()
                return float(cleaned_value)
            except (ValueError, TypeError):
                return 0.0 

        processed_wishlist_data = []
        for wish in all_wishlist_data:
            processed_wish = wish.copy()
            processed_wish['Steam Preco Atual'] = safe_float_conversion(wish.get('Steam Preco Atual'))
            processed_wish['Steam Menor Preco Historico'] = safe_float_conversion(wish.get('Steam Menor Preco Historico'))
            processed_wish['PSN Preco Atual'] = safe_float_conversion(wish.get('PSN Preco Atual'))
            processed_wish['PSN Menor Preco Historico'] = safe_float_conversion(wish.get('PSN Menor Preco Historico'))
            processed_wish['Preço'] = safe_float_conversion(wish.get('Preço')) 
            processed_wishlist_data.append(processed_wish)

        wishlist_data_filtered = [item for item in processed_wishlist_data if item.get('Status') != 'Comprado']
        print(f"DEBUG: Dados de 'Desejos' carregados. Total: {len(all_wishlist_data)}, Filtrados: {len(wishlist_data_filtered)}")

        profile_sheet_data = _get_data_from_sheet('Perfil'); profile_records = profile_sheet_data if profile_sheet_data else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        print(f"DEBUG: Dados de 'Perfil' carregados: {profile_data}")

        achievements_sheet_data = _get_data_from_sheet('Conquistas'); all_achievements = achievements_sheet_data if achievements_sheet_data else []
        print(f"DEBUG: Dados de 'Conquistas' carregados. Total: {len(all_achievements)}")
        
        existing_notifications = get_all_notifications_for_frontend()
        # Adiciona o carregamento do histórico de preços para uso na lógica de promoções
        all_price_history_data = _get_data_from_sheet('Historico de Preços')

        def sort_key(game): # Corrigido: 'game' é o argumento, não 'g'
            try: 
                # Certifica-se de que 'Nota' é um número antes de tentar converter
                nota_str = str(game.get('Nota', '-1')).replace(',', '.')
                nota = float(nota_str)
            except (ValueError, TypeError): 
                nota = -1 # Valor padrão para jogos sem nota válida
            return (-nota, game.get('Nome', '').lower())
        
        games_data.sort(key=sort_key)

        notas = [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')]
        tempos_de_jogo = [int(str(g.get('Tempo de Jogo', 0)).replace('h', '')) for g in games_data]
        
        base_stats = {
            'total_jogos': len(games_data),
            'total_finalizados': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado']]),
            'total_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim']),
            'total_avaliados': len([g for g in games_data if g.get('Nota') and float(str(g.get('Nota')).replace(',', '.')) > 0]),
            'total_horas_jogadas': sum(tempos_de_jogo),
            'custo_total_biblioteca': sum([float(str(g.get('Preço', '0,00')).replace('R$', '').replace(',', '.')) for g in games_data]),
            'media_notas': sum(notas) / len(notas) if notas else 0,
            'total_conquistas': sum([int(g.get('Conquistas Obtidas', 0)) for g in games_data]),
            'total_jogos_longos': len([t for t in tempos_de_jogo if t >= 50]),
            'total_soulslike_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim' and 'Soulslike' in g.get('Estilo', '')]),
            'total_indie': len([g for g in games_data if 'Indie' in g.get('Estilo', '')]),
            'JOGO_MAIS_JOGADO': max(tempos_de_jogo) if tempos_de_jogo else 0,
            'total_finalizados_acao': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Ação' in g.get('Estilo', '')]),
            'total_finalizados_estrategia': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Estratégia' in g.get('Estilo', '')]),
            'total_generos_diferentes': len(set(g for game in games_data if game.get('Estilo') for g in game.get('Estilo').split(','))),
            'total_notas_10': len([n for n in notas if n == 100]),
            'total_notas_baixas': len([n for n in notas if n <= 30]),
        }

        completed_achievements, pending_achievements = _check_achievements(games_data, base_stats, all_achievements, wishlist_data_filtered) 
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        final_stats = {**base_stats, **gamer_stats}

        for ach in completed_achievements:
            notification_message = f"Você desbloqueou a conquista: '{ach.get('Nome')}'!"
            if not any(n.get('Tipo') == "Conquista Desbloqueada" and n.get('Mensagem') == notification_message for n in existing_notifications):
                _add_notification("Conquista Desbloqueada", notification_message)
        
        today = current_time.replace(hour=0, minute=0, second=0, microsecond=0) 
        release_notification_milestones = [30, 15, 7, 3, 1, 0] 

        for wish in processed_wishlist_data: 
            release_date_str = wish.get('Data Lançamento')
            if release_date_str:
                try:
                    release_date = None
                    if '/' in release_date_str: 
                        release_date = datetime.strptime(release_date_str, "%d/%m/%Y")
                    elif '-' in release_date_str: 
                        release_date = datetime.strptime(release_date_str, "%Y-%m-%d")
                    
                    if not release_date:
                        print(f"AVISO: Data de lançamento inválida ou formato desconhecido para '{wish.get('Nome')}': {release_date_str}")
                        continue 
                    
                    release_date = brasilia_tz.localize(release_date.replace(hour=0, minute=0, second=0, microsecond=0))

                    time_to_release = release_date - today
                    days_to_release = time_to_release.days

                    for milestone in release_notification_milestones:
                        if days_to_release == milestone:
                            notification_display_message = "" 
                            if milestone == 0:
                                notification_display_message = f"O jogo '{wish.get('Nome')}' foi lançado hoje!"
                            elif milestone == 1:
                                notification_display_message = f"O jogo '{wish.get('Nome')}' será lançado amanhã!"
                            else:
                                notification_display_message = f"O jogo '{wish.get('Nome')}' será lançado em {milestone} dias!"
                            
                            notification_message_with_milestone = f"{notification_display_message} (Marco: {milestone} dias)"

                            if not any(n.get('Tipo') == "Lançamento Próximo" and n.get('Mensagem') == notification_message_with_milestone for n in existing_notifications):
                                _add_notification("Lançamento Próximo", notification_message_with_milestone, notification_display_message)
                                print(f"DEBUG: Notificação de lançamento gerada para '{wish.get('Nome')}': {notification_message_with_milestone}")
                            break 
                except ValueError:
                    print(f"AVISO: Erro ao parsear data de lançamento para '{wish.get('Nome')}': {release_date_str}. Ignorando.")
                except Exception as e:
                    print(f"ERRO ao processar data de lançamento para '{wish.get('Nome')}': {e}")
       
        for wish in wishlist_data_filtered: 
            # Chama a nova função de verificação de promoções
            _check_for_promotions(wish, existing_notifications, all_price_history_data)
            
        print("DEBUG: get_all_game_data finalizado com sucesso.")
        return {
            'estatisticas': final_stats, 'biblioteca': games_data, 'desejos': wishlist_data_filtered, 'perfil': profile_data,
            'conquistas_concluidas': completed_achievements,
            'conquistas_pendentes': pending_achievements
        }
    except Exception as e:
        print(f"ERRO CRÍTICO: Erro ao buscar dados na função get_all_game_data: {e}"); traceback.print_exc()
        return { 'estatisticas': {}, 'biblioteca': [], 'desejos': [], 'perfil': {}, 'conquistas_concluidas': [], 'conquistas_pendentes': [] }

def get_public_profile_data():
    try:
        game_sheet_data = _get_data_from_sheet('Jogos'); games_data = game_sheet_data if game_sheet_data else []
        wishlist_sheet_data = _get_data_from_sheet('Desejos')
        all_wishlist_data = wishlist_sheet_data if wishlist_sheet_data else []
        profile_sheet_data = _get_data_from_sheet('Perfil'); profile_records = profile_sheet_data if profile_sheet_data else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet_data = _get_data_from_sheet('Conquistas'); all_achievements = achievements_sheet_data if achievements_sheet_data else []

        tempos_de_jogo = [int(str(g.get('Tempo de Jogo', 0)).replace('h', '')) for g in games_data]
        notas = [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')]

        base_stats = {
            'total_jogos': len(games_data),
            'total_finalizados': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado']]),
            'total_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim']),
            'total_avaliados': len([g for g in games_data if g.get('Nota') and float(str(g.get('Nota')).replace(',', '.')) > 0]),
            'total_horas_jogadas': sum(tempos_de_jogo),
            'custo_total_biblioteca': sum([float(str(g.get('Preço', '0,00')).replace('R$', '').replace(',', '.')) for g in games_data]),
            'media_notas': sum(notas) / len(notas) if notas else 0,
            'total_conquistas': sum([int(g.get('Conquistas Obtidas', 0)) for g in games_data]),
            'total_jogos_longos': len([t for t in tempos_de_jogo if t >= 50]),
            'total_soulslike_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim' and 'Soulslike' in g.get('Estilo', '')]),
            'total_indie': len([g for g in games_data if 'Indie' in g.get('Estilo', '')]),
            'JOGO_MAIS_JOGADO': max(tempos_de_jogo) if tempos_de_jogo else 0,
            'total_finalizados_acao': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Ação' in g.get('Estilo', '')]),
            'total_finalizados_estrategia': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Estratégia' in g.get('Estilo', '')]),
            'total_generos_diferentes': len(set(g for game in games_data if game.get('Estilo') for g in game.get('Estilo').split(','))),
            'total_notas_10': len([n for n in notas if n == 100]),
            'total_notas_baixas': len([n for n in notas if n <= 30]),
            'WISHLIST_TOTAL': len(all_wishlist_data) 
        }

        completed_achievements, _ = _check_achievements(games_data, base_stats, all_achievements, all_wishlist_data)
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        public_stats = {**base_stats, **gamer_stats}
        
        recent_platinums = [g for g in games_data if g.get('Platinado?') == 'Sim' and g.get('Link')]
        recent_platinums.sort(key=lambda x: x.get('Terminado em', '0000-00-00'), reverse=True)
        
        return {
            'perfil': profile_data,
            'estatisticas': public_stats,
            'ultimos_platinados': recent_platinums[:5]
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
                    
                    translated_description = ""
                    if Config.DEEPL_API_KEY and description:
                        try:
                            translator = deepl.Translator(Config.DEEPL_API_KEY)
                            result = translator.translate_text(description, target_lang="PT-BR")
                            translated_description = result.text
                            print(f"DEBUG: Descrição traduzida com sucesso: {translated_description[:50]}...")
                        except Exception as deepl_e:
                            print(f"ERRO: Erro ao traduzir com DeepL: {deepl_e}")
                            translated_description = description
                    else:
                        translated_description = description

                    game_data['Descricao'] = translated_description
                    game_data['Metacritic'] = details.get('metacritic', '')

                    screenshots_list = [sc.get('image') for sc in details.get('short_screenshots', [])[:3]]
                    game_data['Screenshots'] = ', '.join(screenshots_list)
            except requests.exceptions.RequestException as e:
                print(f"ERRO: Erro ao buscar detalhes da RAWG para o ID {rawg_id}: {e}")

        sheet = _get_sheet('Jogos')
        if not sheet:
            return {"success": False, "message": "Conexão com a planilha falhou."}

        headers = sheet.row_values(1)
        row_data = [game_data.get(header, '') for header in headers]

        sheet.append_row(row_data)
        _invalidate_cache('Jogos') 
        _add_notification("Novo Jogo Adicionado", f"Você adicionou '{game_data.get('Nome', 'Um novo jogo')}' à sua biblioteca!")

        return {"success": True, "message": "Jogo adicionado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao adicionar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar jogo."}
        
def add_wish_to_sheet(wish_data):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        headers = sheet.row_values(1)
        
        row_data = {
            'Nome': wish_data.get('Nome', ''),
            'Link': wish_data.get('Link', ''),
            'Data Lançamento': wish_data.get('Data Lançamento', ''),
            'Preço': wish_data.get('Preço', 0), 
            'Status': wish_data.get('Status', ''), 
            'Steam Preco Atual': wish_data.get('Steam Preco Atual', 0),
            'Steam Menor Preco Historico': wish_data.get('Steam Menor Preco Historico', 0),
            'PSN Preco Atual': wish_data.get('PSN Preco Atual', 0),
            'PSN Menor Preco Historico': wish_data.get('PSN Menor Preco Historico', 0),
            'Ultima Atualizacao': '' 
        }

        ordered_row_values = [row_data.get(header, '') for header in headers]

        sheet.append_row(ordered_row_values)
        _invalidate_cache('Desejos') 
        _add_notification("Novo Desejo Adicionado", f"Você adicionou '{wish_data.get('Nome', 'Um novo jogo')}' à sua lista de desejos!")

        return {"success": True, "message": "Item de desejo adicionado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao adicionar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar item de desejo."}
        
def update_game_in_sheet(game_name, updated_data):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        try: cell = sheet.find(game_name)
        except gspread.exceptions.CellNotFound: return {"success": False, "message": "Jogo não encontrado."}
        
        row_values = sheet.row_values(cell.row)
        headers = sheet.row_values(1)
        
        existing_game_dict = dict(zip(headers, row_values))

        column_map = {
            'Nome': 0, 'Plataforma': 1, 'Status': 2, 'Nota': 3, 'Preço': 4,
            'Tempo de Jogo': 5, 'Conquistas Obtidas': 6, 'Platinado?': 7,
            'Estilo': 8, 'Link': 9, 'Adquirido em': 10, 'Início em': 11,
            'Terminado em': 12, 'Conclusão': 13, 'Abandonado?': 14,
            'RAWG_ID': 15, 'Descricao': 16, 'Metacritic': 17, 'Screenshots': 18
        }
        
        new_row = list(row_values)
        
        while len(new_row) < len(headers):
            new_row.append('')

        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                
                if key in ['Descricao', 'Metacritic']:
                    if value is None or value == '':
                        if existing_game_dict.get(key, '') != '':
                            new_row[col_index] = existing_game_dict[key]
                        else:
                            new_row[col_index] = ''
                    else:
                        new_row[col_index] = value
                
                elif key in ['Nota', 'Preço']:
                    new_row[col_index] = float(value) if value is not None and value != '' else ''
                elif key == 'Tempo de Jogo' or key == 'Conquistas Obtidas':
                    new_row[col_index] = int(value) if value is not None and value != '' else 0
                else:
                    new_row[col_index] = value

        sheet.update(f'A{cell.row}', [new_row])
        _invalidate_cache('Jogos') 
        
        if existing_game_dict.get('Platinado?', 'Não') == 'Não' and updated_data.get('Platinado?') == 'Sim':
            _add_notification("Jogo Platinado", f"Parabéns! Você platinou '{updated_data.get('Nome', game_name)}'!")
        
        if existing_game_dict.get('Status') not in ['Finalizado', 'Platinado'] and updated_data.get('Status') == 'Finalizado':
            _add_notification("Jogo Finalizado", f"Você finalizou '{updated_data.get('Nome', game_name)}'!")

        return {"success": True, "message": "Jogo atualizado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao atualizar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar jogo."}
        
def delete_game_from_sheet(game_name):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(game_name)
        if not cell: return {"success": False, "message": "Jogo não encontrado."}
        sheet.delete_rows(cell.row)
        _invalidate_cache('Jogos') 
        _add_notification("Jogo Removido", f"O jogo '{game_name}' foi removido da sua biblioteca.")

        return {"success": True, "message": "Jogo deletado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao deletar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar jogo."}
        
def update_wish_in_sheet(wish_name, updated_data):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(wish_name)
        if not cell: return {"success": False, "message": "Item de desejo não encontrado."}
        
        row_values = sheet.row_values(cell.row) 
        headers = sheet.row_values(1)
        
        column_map = {
            'Nome': 0, 'Link': 1, 'Data Lançamento': 2, 'Preço': 3, 'Status': 4,
            'Steam Preco Atual': 5, 'Steam Menor Preco Historico': 6,
            'PSN Preco Atual': 7, 'PSN Menor Preco Historico': 8,
            'Ultima Atualizacao': 9
        }
        new_row = list(row_values)

        while len(new_row) < len(headers):
            new_row.append('') 

        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                if key in ['Preço', 'Steam Preco Atual', 'Steam Menor Preco Historico', 'PSN Preco Atual', 'PSN Menor Preco Historico']:
                    new_row[col_index] = float(value) if value is not None and value != '' else ''
                else:
                    new_row[col_index] = value

        sheet.update(f'A{cell.row}', [new_row])
        _invalidate_cache('Desejos') 
        return {"success": True, "message": "Item de desejo atualizado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao atualizar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar item de desejo."}

def delete_wish_from_sheet(wish_name):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(wish_name)
        if not cell: return {"success": False, "message": "Item de desejo não encontrado."}
        sheet.delete_rows(cell.row)
        _invalidate_cache('Desejos') 
        _add_notification("Desejo Removido", f"O item '{wish_name}' foi removido da sua lista de desejos.")

        return {"success": True, "message": "Item de desejo deletado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao deletar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar item de desejo."}

def purchase_wish_item_in_sheet(item_name):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}

        try:
            cell = sheet.find(item_name)
        except gspread.exceptions.CellNotFound:
            return {"success": False, "message": "Item de desejo não encontrado."}

        headers = sheet.row_values(1)
        try:
            status_col_index = headers.index('Status') + 1
            sheet.update_cell(cell.row, status_col_index, 'Comprado')
            _invalidate_cache('Desejos') 
            _add_notification("Desejo Comprado", f"Você marcou '{item_name}' como comprado! Aproveite o jogo!")

            return {"success": True, "message": "Item marcado como comprado!"}
        except ValueError:
            return {"success": False, "message": "Coluna 'Status' não encontrada na planilha de Desejos."}

    except Exception as e:
        print(f"ERRO: Erro ao marcar item como comprado: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao processar a compra."}

def trigger_wishlist_scraper_action():
    """Aciona a GitHub Action de web scraping da lista de desejos via API REST."""
    try:
        github_pat = os.environ.get('GITHUB_PAT')
        repo_owner = os.environ.get('GITHUB_OWNER')
        repo_name = os.environ.get('GITHUB_REPO')
        workflow_file = os.environ.get('GITHUB_WORKFLOW_FILE_NAME')

        if not all([github_pat, repo_owner, repo_name, workflow_file]):
            print("ERRO: Variáveis de ambiente da GitHub Action não configuradas.")
            return {"success": False, "message": "Configuração da API do GitHub ausente."}

        url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_file}/dispatches'
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': f'token {github_pat}',
        }
        data = { 'ref': 'main' }

        response = requests.post(url, headers=headers, json=data)
        
        if response.status_code == 204:
            print("DEBUG: GitHub Action de web scraping acionada com sucesso!")
            return {"success": True, "message": "Atualização de preços iniciada com sucesso!"}
        else:
            print(f"ERRO: Falha ao acionar a GitHub Action. Status: {response.status_code}, Resposta: {response.text}")
            return {"success": False, "message": f"Erro na API do GitHub: {response.text}"}

    except requests.exceptions.RequestException as e:
        print(f"ERRO DE CONEXÃO: Falha ao se conectar com a API do GitHub: {e}")
        return {"success": False, "message": "Erro de conexão. Verifique se a URL está correta."}
    except Exception as e:
        print(f"ERRO GENÉRICO: Erro ao acionar a Action: {e}")
        return {"success": False, "message": "Ocorreu um erro interno ao tentar acionar a action."}

# --- NOVA FUNÇÃO PARA SORTEAR JOGO ---
def get_random_game(plataforma=None, estilo=None, metacritic_min=None, metacritic_max=None):
    """
    Filtra e sorteia um jogo aleatório da biblioteca que não esteja finalizado.
    """
    try:
        games_data = _get_data_from_sheet('Jogos')
        if not games_data:
            return None

        df = pd.DataFrame(games_data)
        df_filtered = df.dropna(subset=['Status'])
        
        jogos_elegiveis = df_filtered[~df_filtered['Status'].isin(['Platinado', 'Abandonado', 'Finalizado'])]

        if plataforma:
            jogos_elegiveis = jogos_elegiveis[jogos_elegiveis['Plataforma'].str.lower() == plataforma.lower()]
        
        if estilo:
            jogos_elegiveis_estilo = jogos_elegiveis.dropna(subset=['Estilo'])
            jogos_elegiveis = jogos_elegiveis_estilo[jogos_elegiveis_estilo['Estilo'].str.contains(estilo, case=False, na=False)]

        if metacritic_min:
            jogos_elegiveis['Metacritic'] = pd.to_numeric(jogos_elegiveis['Metacritic'], errors='coerce')
            jogos_elegiveis = jogos_elegiveis.dropna(subset=['Metacritic'])
            jogos_elegiveis = jogos_elegiveis[jogos_elegiveis['Metacritic'] >= int(metacritic_min)]

        if metacritic_max:
            jogos_elegiveis['Metacritic'] = pd.to_numeric(jogos_elegiveis['Metacritic'], errors='coerce')
            jogos_elegiveis = jogos_elegiveis.dropna(subset=['Metacritic'])
            jogos_elegiveis = jogos_elegiveis[jogos_elegiveis['Metacritic'] <= int(metacritic_max)]

        if not jogos_elegiveis.empty:
            jogo_sorteado = jogos_elegiveis.sample(n=1)
            return jogo_sorteado.to_dict(orient='records')[0]
        
        return None
    except Exception as e:
        print(f"ERRO na função get_random_game: {e}"); traceback.print_exc()
        return None
