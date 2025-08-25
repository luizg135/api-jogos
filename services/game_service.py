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
import random # Importado para a funcionalidade de sortear jogo

# NOVO: Dicionário de tradução de gêneros, movido para cá para ser acessível
GENRE_TRANSLATIONS = {
    "Action": "Ação", "Indie": "Indie", "Adventure": "Aventura",
    "RPG": "RPG", "Strategy": "Estratégia", "Shooter": "Tiro",
    "Casual": "Casual", "Simulation": "Simulação", "Puzzle": "Puzzle",
    "Arcade": "Arcade", "Plataforma": "Plataforma", "Racing": "Corrida",
    "Massively Multiplayer": "MMO", "Sports": "Esportes", "Fighting": "Luta",
    "Family": "Família", "Board Games": "Jogos de Tabuleiro", "Educational": "Educacional",
    "Card": "Cartas", "Soulslike": "Soulslike" # Adicionado Soulslike para consistência
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

def _add_notification(notification_type, message_to_save, message_for_display=None, game_name=None, internal_link=None):
    """Adiciona uma nova notificação à planilha, evitando duplicatas recentes ou re-notificando promoções após um período.
        message_to_save: A mensagem completa com o marco (para desduplicação).
        message_for_display: A mensagem sem o marco (para exibição no frontend).
        game_name: O nome do jogo, usado para desduplicação de promoções.
        internal_link: O link interno para navegação no frontend.
    """
    sheet = _get_notifications_sheet()
    if not sheet:
        print("ERRO: Conexão com a planilha de notificações falhou ao tentar adicionar notificação.")
        return {"success": False, "message": "Conexão com a planilha de notificações falhou."}

    notifications = _get_data_from_sheet('Notificações') # Busca do cache ou da planilha
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    current_time = datetime.now(brasilia_tz)

    if notification_type == "Promoção" and game_name:
        # Filter for existing promotion notifications for this specific game
        existing_promotions = [
            n for n in notifications
            if n.get('Tipo') == "Promoção" and game_name in n.get('Mensagem', '') # Simple check for game_name in message
        ]

        if existing_promotions:
            # Find the most recent promotion notification for this game
            latest_promotion_date = None
            for notif in existing_promotions:
                try:
                    notif_date = brasilia_tz.localize(datetime.strptime(notif.get('Data'), "%Y-%m-%d %H:%M:%S"))
                    if latest_promotion_date is None or notif_date > latest_promotion_date:
                        latest_promotion_date = notif_date
                except (ValueError, TypeError):
                    continue

            # If the latest promotion notification is less than 30 days old, do not re-notify
            if latest_promotion_date and (current_time - latest_promotion_date).days < 30:
                print(f"DEBUG: Notificação de promoção para '{game_name}' evitada (já notificada há menos de 30 dias).")
                return {"success": False, "message": "Notificação de promoção duplicada evitada."}
    else:
        # Standard deduplication for other notification types
        for notif in notifications:
            if notif.get('Tipo') == notification_type and \
               notif.get('Mensagem') == message_to_save:
                print(f"DEBUG: Notificação duplicada evitada: Tipo='{notification_type}', Mensagem='{message_to_save}'")
                return {"success": False, "message": "Notificação duplicada evitada."}

    new_id = len(notifications) + 1
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S")
    
    final_message_to_save = message_to_save
    final_message_for_display = message_for_display if message_for_display is not None else message_to_save

    # Adiciona o internal_link à linha, se fornecido
    row_data = [new_id, notification_type, final_message_to_save, timestamp, 'Não', internal_link if internal_link else '']
    sheet.append_row(row_data)
    _invalidate_cache('Notificações') # Invalida o cache de notificações após adicionar
    print(f"DEBUG: Notificação adicionada: ID={new_id}, Tipo='{notification_type}', Mensagem='{final_message_to_save}' (Exibição: '{final_message_for_display}'), Link: '{internal_link}'")
    return {"success": True, "message": "Notificação adicionada com sucesso."}

def get_all_notifications_for_frontend():
    """Retorna TODAS as notificações (lidas e não lidas) para o frontend."""
    notifications = _get_data_from_sheet('Notificações') # Busca do cache ou da planilha
    
    processed_notifications = []
    for notif in notifications:
        # Remove o "(Marco: X dias)" da mensagem antes de enviar para o frontend
        display_message = notif.get('Mensagem', '')
        if "(Marco:" in display_message:
            display_message = display_message.split("(Marco:")[0].strip()

        processed_notif = {
            'ID': int(notif.get('ID', 0)),
            'Tipo': notif.get('Tipo', ''),
            'Mensagem': display_message, # Usa a mensagem limpa para exibição
            'Data': notif.get('Data', ''),
            'Lida': str(notif.get('Lida', 'Não')),
            'LinkInterno': notif.get('LinkInterno', '') # Inclui o link interno
        }
        processed_notifications.append(processed_notif)
    
    processed_notifications.sort(key=lambda x: datetime.strptime(x['Data'], "%Y-%m-%d %H:%M:%S"), reverse=True)

    print(f"DEBUG: Total de notificações (lidas e não lidas) encontradas para o frontend: {len(processed_notifications)}")
    for i, notif in enumerate(processed_notifications[:5]):
        print(f"DEBUG:   Notificação {notif['ID']} - Tipo: {notif['Tipo']}, Lida: '{notif['Lida']}', Mensagem Display: '{notif['Mensagem']}', Link: '{notif['LinkInterno']}'")
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
        _invalidate_cache('Notificações') # Invalida o cache de notificações após marcar como lida
        print(f"DEBUG: Notificação {notification_id} marcada como lida na planilha. Linha: {found_row_index}, Coluna Lida: {lida_col_index + 1}")
        return {"success": True, "message": f"Notificação {notification_id} marcada como lida."}
    except ValueError:
        print("ERRO: Colunas 'ID' ou 'Lida' não encontradas na planilha de Notificações.")
        return {"success": False, "message": "Erro: Colunas necessárias não encontradas."}
    except Exception as e:
        print(f"ERRO ao marcar notificação {notification_id} como lida: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar notificação."}

# --- FIM DAS Funções de Notificação ---

def get_all_game_data():
    try:
        print("DEBUG: Iniciando get_all_game_data.")
        # Define current_time aqui para ser usado na lógica de promoções
        brasilia_tz = pytz.timezone('America/Sao_Paulo')
        current_time = datetime.now(brasilia_tz)

        game_sheet_data = _get_data_from_sheet('Jogos'); games_data = game_sheet_data if game_sheet_data else []
        print(f"DEBUG: Dados de 'Jogos' carregados. Total: {len(games_data)}")

        wishlist_sheet_data = _get_data_from_sheet('Desejos')
        all_wishlist_data = wishlist_sheet_data if wishlist_sheet_data else []
        
        # Função auxiliar para converter string para float, tratando "Não encontrado" e outros não numéricos
        def safe_float_conversion(value):
            try:
                # Substitui vírgula por ponto para conversão de float e remove "R$"
                cleaned_value = str(value).replace('R$', '').replace(',', '.').strip()
                return float(cleaned_value)
            except (ValueError, TypeError):
                return 0.0 # Retorna 0.0 se a conversão falhar

        # Processa os dados da wishlist para garantir que os campos de preço sejam numéricos
        processed_wishlist_data = []
        for wish in all_wishlist_data:
            processed_wish = wish.copy()
            processed_wish['Steam Preco Atual'] = safe_float_conversion(wish.get('Steam Preco Atual'))
            processed_wish['Steam Menor Preco Historico'] = safe_float_conversion(wish.get('Steam Menor Preco Historico'))
            processed_wish['PSN Preco Atual'] = safe_float_conversion(wish.get('PSN Preco Atual'))
            processed_wish['PSN Menor Preco Historico'] = safe_float_conversion(wish.get('PSN Menor Preco Historico'))
            processed_wish['Preço'] = safe_float_conversion(wish.get('Preço')) # Também para o campo 'Preço' geral
            processed_wishlist_data.append(processed_wish)

        wishlist_data_filtered = [item for item in processed_wishlist_data if item.get('Status') != 'Comprado']
        print(f"DEBUG: Dados de 'Desejos' carregados. Total: {len(all_wishlist_data)}, Filtrados: {len(wishlist_data_filtered)}")

        profile_sheet_data = _get_data_from_sheet('Perfil'); profile_records = profile_sheet_data if profile_sheet_data else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        print(f"DEBUG: Dados de 'Perfil' carregados: {profile_data}")

        achievements_sheet_data = _get_data_from_sheet('Conquistas'); all_achievements = achievements_sheet_data if achievements_sheet_data else []
        print(f"DEBUG: Dados de 'Conquistas' carregados. Total: {len(all_achievements)}")
        
        # Pega todas as notificações existentes para evitar duplicatas
        existing_notifications = get_all_notifications_for_frontend()

        def sort_key(game):
            try: nota = float(str(game.get('Nota', '-1')).replace(',', '.'))
            except (ValueError, TypeError): nota = -1
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

        completed_achievements, pending_achievements = _check_achievements(games_data, base_stats, all_achievements, wishlist_data_filtered) # Usar wishlist_data_filtered aqui
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        final_stats = {**base_stats, **gamer_stats}

        # --- MODIFICAÇÃO: Lógica para notificar conquistas do aplicativo ---
        for ach in completed_achievements:
            notification_message = f"Você desbloqueou a conquista: '{ach.get('Nome')}'!"
            # Link interno para o modal de conquistas
            internal_link = f"page=conquistas&modal=achievements&achId={ach.get('ID')}"
            # Verifica se já existe uma notificação para esta conquista específica
            if not any(n.get('Tipo') == "Conquista Desbloqueada" and n.get('Mensagem') == notification_message for n in existing_notifications):
                _add_notification("Conquista Desbloqueada", notification_message, internal_link=internal_link)
        # --- FIM MODIFICAÇÃO ---

        # --- NOVO: Lógica para notificar lançamentos próximos da lista de desejos ---
        # brasilia_tz já está definida no escopo desta função
        today = current_time.replace(hour=0, minute=0, second=0, microsecond=0) # Data de hoje em Brasília, sem hora
        
        # Definir os marcos de notificação em dias antes do lançamento
        release_notification_milestones = [30, 15, 7, 3, 1, 0] # 1 mês, 15 dias, 7 dias, 3 dias, 1 dia, Lançamento

        for wish in processed_wishlist_data: # Usar processed_wishlist_data aqui
            release_date_str = wish.get('Data Lançamento')
            if release_date_str:
                try:
                    # --- MODIFICAÇÃO AQUI: Tentar parsear a data no formato DD/MM/YYYY primeiro ---
                    release_date = None
                    if '/' in release_date_str: # dd/mm/yyyy
                        release_date = datetime.strptime(release_date_str, "%d/%m/%Y")
                    elif '-' in release_date_str: # yyyy-mm-dd (formato comum de APIs)
                        release_date = datetime.strptime(release_date_str, "%Y-%m-%d")
                    
                    if not release_date:
                        print(f"AVISO: Data de lançamento inválida ou formato desconhecido para '{wish.get('Nome')}': {release_date_str}")
                        continue # Ignora datas em formato desconhecido
                    
                    # --- NOVO: Atribuir o fuso horário de Brasília à release_date ---
                    release_date = brasilia_tz.localize(release_date.replace(hour=0, minute=0, second=0, microsecond=0))
                    # --- FIM NOVO ---

                    time_to_release = release_date - today
                    days_to_release = time_to_release.days

                    for milestone in release_notification_milestones:
                        if days_to_release == milestone:
                            notification_display_message = "" # Mensagem para o frontend
                            if milestone == 0:
                                notification_display_message = f"O jogo '{wish.get('Nome')}' foi lançado hoje!"
                            elif milestone == 1:
                                notification_display_message = f"O jogo '{wish.get('Nome')}' será lançado amanhã!"
                            else:
                                notification_display_message = f"O jogo '{wish.get('Nome')}' será lançado em {milestone} dias!"
                            
                            # Mensagem completa com o marco para desduplicação no backend
                            notification_message_with_milestone = f"{notification_display_message} (Marco: {milestone} dias)"
                            # Link interno para o modal de detalhes do desejo
                            internal_link = f"page=desejos&modal=wish-details&name={wish.get('Nome')}"

                            if not any(n.get('Tipo') == "Lançamento Próximo" and n.get('Mensagem') == notification_message_with_milestone for n in existing_notifications):
                                _add_notification("Lançamento Próximo", notification_message_with_milestone, notification_display_message, internal_link=internal_link)
                                print(f"DEBUG: Notificação de lançamento gerada para '{wish.get('Nome')}': {notification_message_with_milestone}")
                            break # Notifica apenas o marco mais próximo (maior milestone)
                except ValueError:
                    print(f"AVISO: Erro ao parsear data de lançamento para '{wish.get('Nome')}': {release_date_str}. Ignorando.")
                except Exception as e:
                    print(f"ERRO ao processar data de lançamento para '{wish.get('Nome')}': {e}")
        # --- FIM NOVO ---

        # NOVO: Lógica para notificar promoções na lista de desejos
        for wish in wishlist_data_filtered: # <-- Alteração aqui: usa a lista já filtrada
            wish_name = wish.get('Nome', 'Um jogo')
            last_update_str = wish.get('Ultima Atualizacao')
            
            # Converte a string de data/hora para um objeto datetime em Brasília
            last_update_datetime = None
            if last_update_str:
                try:
                    last_update_datetime = brasilia_tz.localize(datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S"))
                except ValueError:
                    print(f"AVISO: Não foi possível parsear 'Ultima Atualizacao' para '{wish_name}': {last_update_str}")
            
            # Se não houver data de atualização válida, não podemos verificar promoções recentes
            if not last_update_datetime:
                continue

            # Verifica se a atualização foi nas últimas 24 horas
            if (current_time - last_update_datetime).total_seconds() / 3600 <= 24: # Usar current_time aqui
                steam_current = wish['Steam Preco Atual'] # Já é float devido ao processamento anterior
                steam_lowest = wish['Steam Menor Preco Historico'] # Já é float
                psn_current = wish['PSN Preco Atual'] # Já é float
                psn_lowest = wish['PSN Menor Preco Historico'] # Já é float

                # Condição de promoção: preço atual é igual ao menor histórico
                # Ou está muito próximo (ex: 1% de diferença)
                promotion_found = False
                if steam_current > 0 and (steam_current <= steam_lowest * 1.01): # Margem de 1%
                    notification_message = f"Promoção na Steam! '{wish_name}' por R${steam_current:.2f}."
                    internal_link = f"page=desejos&modal=wish-details&name={wish_name}"
                    _add_notification("Promoção", notification_message, game_name=wish_name, internal_link=internal_link) # Passa game_name e internal_link
                    promotion_found = True
                
                if psn_current > 0 and (psn_current <= psn_lowest * 1.01) and not promotion_found: # Evita duas notificações para o mesmo jogo se ambas as plataformas estiverem em promoção
                    notification_message = f"Promoção na PSN! '{wish_name}' por R${psn_current:.2f}."
                    internal_link = f"page=desejos&modal=wish-details&name={wish_name}"
                    _add_notification("Promoção", notification_message, game_name=wish_name, internal_link=internal_link) # Passa game_name e internal_link
            # FIM NOVO
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

        # Calcula as estatísticas públicas
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
            'WISHLIST_TOTAL': len(all_wishlist_data) # Inclui o total da wishlist para cálculo de conquistas
        }

        # Conquistas desbloqueadas para o cálculo do nível e rank
        # Passa all_wishlist_data para _check_achievements para que WISHLIST_TOTAL seja calculado corretamente
        completed_achievements, _ = _check_achievements(games_data, base_stats, all_achievements, all_wishlist_data)
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        public_stats = {**base_stats, **gamer_stats}
        
        # Filtra os últimos 5 jogos platinados com imagens
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
                # Cria a chave se ela não existir
                sheet.append_row([key, value])
        _invalidate_cache('Perfil') # Invalida o cache de perfil
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
        _invalidate_cache('Jogos') # Invalida o cache de jogos
        _add_notification("Novo Jogo Adicionado", f"Você adicionou '{game_data.get('Nome', 'Um novo jogo')}' à sua biblioteca!")

        return {"success": True, "message": "Jogo adicionado com sucesso."}
    except Exception as e:
        print(f"ERRO: Erro ao adicionar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar jogo."}
        
def add_wish_to_sheet(wish_data):
    """
    Adiciona um novo item à lista de desejos.
    Os campos de preço da plataforma e data de atualização são inicializados vazios
    ou com zero, pois serão preenchidos por um processo de atualização externo (ou manual).
    """
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        # Obter os cabeçalhos da planilha para garantir a ordem correta dos dados
        headers = sheet.row_values(1)
        
        # Preencher os dados da nova linha com os valores fornecidos ou padrões
        # Certifique-se de que todos os cabeçalhos esperados pela planilha estejam aqui
        row_data = {
            'Nome': wish_data.get('Nome', ''),
            'Link': wish_data.get('Link', ''),
            'Data Lançamento': wish_data.get('Data Lançamento', ''),
            'Preço': wish_data.get('Preço', 0), # Preço inicial, pode ser atualizado
            'Status': wish_data.get('Status', ''), # Pode ser 'Na Fila', 'Comprado', etc.
            'Steam Preco Atual': wish_data.get('Steam Preco Atual', 0),
            'Steam Menor Preco Historico': wish_data.get('Steam Menor Preco Historico', 0),
            'PSN Preco Atual': wish_data.get('PSN Preco Atual', 0),
            'PSN Menor Preco Historico': wish_data.get('PSN Menor Preco Historico', 0),
            'Ultima Atualizacao': wish_data.get('Ultima Atualizacao', '') # Data de atualização inicial
        }

        # Criar a lista de valores na ordem dos cabeçalhos da planilha
        ordered_row_values = [row_data.get(header, '') for header in headers]

        sheet.append_row(ordered_row_values)
        _invalidate_cache('Desejos') # Invalida o cache de desejos
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
        
        # Get existing row data and headers
        row_values = sheet.row_values(cell.row)
        headers = sheet.row_values(1)
        
        # Create a dictionary from existing data for easier merging
        existing_game_dict = dict(zip(headers, row_values))

        # Define column map for easier access to indices
        # Mantenho o seu column_map original que já é bem completo
        column_map = {
            'Nome': 0, 'Plataforma': 1, 'Status': 2, 'Nota': 3, 'Preço': 4,
            'Tempo de Jogo': 5, 'Conquistas Obtidas': 6, 'Platinado?': 7,
            'Estilo': 8, 'Link': 9, 'Adquirido em': 10, 'Início em': 11,
            'Terminado em': 12, 'Conclusão': 13, 'Abandonado?': 14,
            'RAWG_ID': 15, 'Descricao': 16, 'Metacritic': 17, 'Screenshots': 18
        }
        
        # Initialize new_row with existing values to preserve untouched data
        new_row = list(row_values)
        
        # Ensure new_row has enough elements to match headers, padding with empty strings
        while len(new_row) < len(headers):
            new_row.append('')

        # Iterate through updated_data to apply changes
        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                
                # Special handling for 'Descricao' and 'Metacritic':
                # If the incoming value for these specific fields is empty/None,
                # we want to retain the existing value from existing_game_dict,
                # UNLESS the existing value was also empty (meaning no prior data).
                if key in ['Descricao', 'Metacritic']:
                    if value is None or value == '':
                        # If the incoming value is empty, keep the existing one if it's not empty
                        if existing_game_dict.get(key, '') != '':
                            new_row[col_index] = existing_game_dict[key]
                        else:
                            # If both incoming and existing are empty, then it's genuinely empty
                            new_row[col_index] = ''
                    else:
                        # If a new value is provided, use it
                        new_row[col_index] = value
                
                # Existing type conversion logic for other fields
                elif key in ['Nota', 'Preço']:
                    new_row[col_index] = float(value) if value is not None and value != '' else ''
                elif key == 'Tempo de Jogo' or key == 'Conquistas Obtidas':
                    new_row[col_index] = int(value) if value is not None and value != '' else 0
                else:
                    new_row[col_index] = value

        sheet.update(f'A{cell.row}', [new_row])
        _invalidate_cache('Jogos') # Invalida o cache de jogos
        
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
        _invalidate_cache('Jogos') # Invalida o cache de jogos
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
        
        row_values = sheet.row_values(cell.row) # Obter a linha existente
        headers = sheet.row_values(1)
        
        # Atualiza o column_map para incluir todas as colunas existentes na sua planilha de Desejos
        column_map = {
            'Nome': 0, 'Link': 1, 'Data Lançamento': 2, 'Preço': 3, 'Status': 4,
            'Steam Preco Atual': 5, 'Steam Menor Preco Historico': 6,
            'PSN Preco Atual': 7, 'PSN Menor Preco Historico': 8,
            'Ultima Atualizacao': 9
        }
        new_row = list(row_values)

        # Garante que a new_row tenha tamanho suficiente para todas as colunas
        while len(new_row) < len(headers):
            new_row.append('') # Adiciona strings vazias para colunas ausentes

        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                # Converte para float apenas se for uma coluna de valor numérico formatado
                if key in ['Preço', 'Steam Preco Atual', 'Steam Menor Preco Historico', 'PSN Preco Atual', 'PSN Menor Preco Historico']:
                    new_row[col_index] = float(value) if value is not None and value != '' else ''
                else:
                    new_row[col_index] = value

        sheet.update(f'A{cell.row}', [new_row])
        _invalidate_cache('Desejos') # Invalida o cache de desejos
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
        _invalidate_cache('Desejos') # Invalida o cache de desejos
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
            _invalidate_cache('Desejos') # Invalida o cache de desejos
            _add_notification("Desejo Comprado", f"Você marcou '{item_name}' como comprado! Aproveite o jogo!")

            return {"success": True, "message": "Item marcado como comprado!"}
        except ValueError:
            return {"success": False, "message": "Coluna 'Status' não encontrada na planilha de Desejos."}

    except Exception as e:
        print(f"ERRO: Erro ao marcar item como comprado: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao processar a compra."}

# NOVO: Funções para acionar a GitHub Action de web scraping
def trigger_wishlist_scraper_action():
    """Aciona a GitHub Action de web scraping da lista de desejos via API REST."""
    try:
        # A URL do workflow deve ser configurada como variável de ambiente
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
        data = {
            'ref': 'main' # Mude para o nome da sua branch principal se for diferente de 'main'
        }

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

# --- NOVAS FUNÇÕES PARA SINCRONIZAÇÃO STEAM ---
def _get_rawg_game_details(rawg_id):
    """Busca detalhes de um jogo na RAWG, incluindo descrição e screenshots."""
    if not Config.RAWG_API_KEY:
        print("ERRO: Chave da API da RAWG não configurada.")
        return {}
    try:
        url = f"https://api.rawg.io/api/games/{rawg_id}?key={Config.RAWG_API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        details = response.json()

        description = details.get('description_raw', '')
        translated_description = ""
        if Config.DEEPL_API_KEY and description:
            try:
                translator = deepl.Translator(Config.DEEPL_API_KEY)
                result = translator.translate_text(description, target_lang="PT-BR")
                translated_description = result.text
            except Exception as deepl_e:
                print(f"AVISO: Erro ao traduzir descrição com DeepL: {deepl_e}")
                translated_description = description
        else:
            translated_description = description

        genres_pt = [g['name'] for g in details.get('genres', [])]
        game_tags = details.get('tags') or []
        for tag in game_tags:
            if tag.get('language') == 'eng' and tag.get('slug') == 'souls-like':
                if "Soulslike" not in genres_pt:
                    genres_pt.append("Soulslike")
                break
        
        screenshots_list = [sc.get('image') for sc in details.get('short_screenshots', [])[:3]]

        return {
            'Descricao': translated_description,
            'Metacritic': details.get('metacritic', ''),
            'Screenshots': ', '.join(screenshots_list),
            'Estilo': ', '.join([GENRE_TRANSLATIONS.get(g, g) for g in genres_pt]),
            'Link': details.get('background_image', '') # Usar background_image como link da capa
        }
    except requests.exceptions.RequestException as e:
        print(f"ERRO: Falha ao buscar detalhes do jogo RAWG ID {rawg_id}: {e}")
        return {}
    except Exception as e:
        print(f"ERRO inesperado ao buscar detalhes RAWG: {e}")
        return {}

def get_steam_library_preview(steam_id):
    """
    Busca a biblioteca de jogos da Steam do usuário e compara com a planilha existente.
    Retorna jogos novos e existentes com dados da Steam.
    """
    if not Config.STEAM_API_KEY:
        return {"success": False, "message": "STEAM_API_KEY não configurada no servidor."}

    try:
        # API para obter a lista de jogos do usuário
        steam_games_url = f"http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={Config.STEAM_API_KEY}&steamid={steam_id}&format=json&include_appinfo=1&include_played_free_games=1"
        response = requests.get(steam_games_url)
        response.raise_for_status()
        steam_data = response.json()

        owned_games = steam_data.get('response', {}).get('games', [])
        
        # Buscar jogos existentes na planilha para comparação
        existing_games_sheet = _get_data_from_sheet('Jogos')
        existing_game_names = {game.get('Nome').lower() for game in existing_games_sheet if game.get('Nome')}

        new_games = []
        existing_games_to_update = []

        for steam_game in owned_games:
            game_name = steam_game.get('name')
            if not game_name:
                continue

            # Buscar RAWG ID para o jogo Steam
            rawg_id = None
            if Config.RAWG_API_KEY: # Só tenta buscar RAWG se a chave estiver configurada
                rawg_search_url = f"https://api.rawg.io/api/games?key={Config.RAWG_API_KEY}&search={game_name}&page_size=1"
                rawg_response = requests.get(rawg_search_url)
                if rawg_response.ok and rawg_response.json().get('results'):
                    rawg_id = rawg_response.json()['results'][0]['id']

            game_info = {
                'Nome': game_name,
                'Plataforma': 'PC (Steam)',
                'Tempo de Jogo': round(steam_game.get('playtime_forever', 0) / 60), # Convertendo minutos para horas
                'Conquistas Obtidas': 0, # Será atualizado em um passo posterior se houver API de conquistas
                'RAWG_ID': rawg_id,
                'Status': 'Na Fila' # Status inicial para jogos importados
            }

            if game_name.lower() in existing_game_names:
                existing_games_to_update.append(game_info)
            else:
                new_games.append(game_info)
        
        return {
            "success": True,
            "new_games": new_games,
            "existing_games_to_update": existing_games_to_update
        }

    except requests.exceptions.RequestException as e:
        print(f"ERRO DE CONEXÃO COM A API DA STEAM: {e}")
        return {"success": False, "message": "Falha ao se comunicar com a API da Steam."}
    except Exception as e:
        print(f"ERRO INESPERADO ao obter prévia da biblioteca Steam: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Ocorreu um erro interno no servidor."}

def sync_steam_library(steam_id, selected_games_data):
    """
    Sincroniza os jogos selecionados da Steam com a planilha.
    selected_games_data é uma lista de dicionários com 'Nome', 'RAWG_ID', 'Tempo de Jogo', etc.
    """
    if not Config.STEAM_API_KEY:
        return {"success": False, "message": "STEAM_API_KEY não configurada no servidor."}

    games_added = 0
    games_updated = 0
    errors = []

    existing_games_sheet = _get_data_from_sheet('Jogos')
    existing_game_map = {game.get('Nome').lower(): game for game in existing_games_sheet if game.get('Nome')}
    
    sheet = _get_sheet('Jogos')
    if not sheet:
        return {"success": False, "message": "Conexão com a planilha de jogos falhou."}
    headers = sheet.row_values(1)
    
    # Mapeamento de colunas para facilitar a atualização
    column_map = {header: i for i, header in enumerate(headers)}

    for game_data_from_steam in selected_games_data:
        game_name = game_data_from_steam.get('Nome')
        rawg_id = game_data_from_steam.get('RAWG_ID')
        steam_playtime = game_data_from_steam.get('Tempo de Jogo', 0) # Já em horas

        if not game_name:
            errors.append(f"Jogo sem nome na lista de sincronização: {game_data_from_steam}")
            continue

        existing_game = existing_game_map.get(game_name.lower())

        try:
            if existing_game:
                # Jogo existente: atualizar Tempo de Jogo e Conquistas Obtidas (se houver)
                # Preservar outros campos como Nota, Preço, Status, Platinado?, etc.
                try:
                    cell = sheet.find(game_name)
                    row_index = cell.row
                except gspread.exceptions.CellNotFound:
                    errors.append(f"Jogo '{game_name}' não encontrado na planilha para atualização.")
                    continue

                current_row_values = sheet.row_values(row_index)
                
                updated_row_values = list(current_row_values) # Copia a linha existente

                # Atualiza Tempo de Jogo
                if 'Tempo de Jogo' in column_map:
                    updated_row_values[column_map['Tempo de Jogo']] = steam_playtime
                
                # Se houver API de conquistas da Steam, buscar e atualizar aqui
                # Por enquanto, mantém o valor existente ou 0
                # if 'Conquistas Obtidas' in column_map:
                #     updated_row_values[column_map['Conquistas Obtidas']] = new_steam_achievements_count

                sheet.update(f'A{row_index}', [updated_row_values])
                games_updated += 1
                print(f"DEBUG: Jogo '{game_name}' atualizado (Tempo de Jogo).")

            else:
                # Novo jogo: buscar detalhes na RAWG e adicionar
                full_game_data = {
                    'Nome': game_name,
                    'Plataforma': 'PC (Steam)',
                    'Status': 'Na Fila',
                    'Nota': '',
                    'Preço': 0,
                    'Tempo de Jogo': steam_playtime,
                    'Conquistas Obtidas': 0,
                    'Platinado?': 'Não',
                    'Adquirido em': datetime.now().strftime("%Y-%m-%d"),
                    'Início em': '',
                    'Terminado em': '',
                    'Conclusão': '',
                    'Abandonado?': 'Não',
                    'RAWG_ID': rawg_id,
                    'Metacritic': '',
                    'Descricao': '',
                    'Screenshots': '',
                    'Estilo': '',
                    'Link': ''
                }

                if rawg_id:
                    rawg_details = _get_rawg_game_details(rawg_id)
                    full_game_data.update(rawg_details)
                
                # Garante que todos os headers estejam preenchidos, mesmo que vazios
                row_to_append = [full_game_data.get(header, '') for header in headers]
                sheet.append_row(row_to_append)
                games_added += 1
                print(f"DEBUG: Jogo '{game_name}' adicionado à biblioteca.")
                _add_notification("Novo Jogo Adicionado", f"Você adicionou '{game_name}' à sua biblioteca via Steam Sync!")

        except Exception as e:
            errors.append(f"Erro ao processar jogo '{game_name}': {e}")
            print(f"ERRO ao sincronizar jogo '{game_name}': {e}")
            traceback.print_exc()

    _invalidate_cache('Jogos') # Invalida o cache de jogos após a sincronização
    
    message = f"{games_added} jogos adicionados e {games_updated} jogos atualizados."
    if errors:
        message += f" Com {len(errors)} erros."
        print("Erros durante a sincronização Steam:", errors)

    return {"success": True, "message": message, "added": games_added, "updated": games_updated, "errors": errors}
# --- FIM NOVAS FUNÇÕES PARA SINCRONIZAÇÃO STEAM ---

# --- NOVA FUNÇÃO PARA HISTÓRICO DE PREÇOS ---
def get_price_history(game_name):
    """
    Retorna o histórico de preços de um jogo da lista de desejos.
    Espera que exista uma aba 'Historico de Preços' com as colunas:
    'Nome do Jogo', 'Plataforma', 'Data', 'Preço'.
    """
    try:
        history_sheet_data = _get_data_from_sheet('Historico de Preços')
        if not history_sheet_data:
            return []

        # Filtra os registros para o jogo específico
        game_history = [
            record for record in history_sheet_data
            if record.get('Nome do Jogo', '').lower() == game_name.lower()
        ]
        
        # Opcional: ordenar por data, se não estiver garantido pela planilha
        game_history.sort(key=lambda x: datetime.strptime(x['Data'], "%Y-%m-%d %H:%M:%S"))

        return game_history
    except Exception as e:
        print(f"ERRO ao buscar histórico de preços para '{game_name}': {e}")
        traceback.print_exc()
        return []
# --- FIM NOVA FUNÇÃO PARA HISTÓRICO DE PREÇOS ---

# --- NOVA FUNÇÃO PARA SORTEAR JOGO ---
def get_random_game(platform=None, style=None, min_metacritic=None, max_metacritic=None):
    """
    Retorna um jogo aleatório da biblioteca, excluindo status 'Platinado', 'Abandonado', 'Finalizado'.
    Pode aplicar filtros opcionais: plataforma, estilo, min_metacritic, max_metacritic.
    """
    games_data = _get_data_from_sheet('Jogos')
    if not games_data:
        return None

    # Filtrar jogos que não estão em status de "jogando" ou "na fila"
    eligible_games = [
        game for game in games_data
        if game.get('Status') not in ['Platinado', 'Abandonado', 'Finalizado']
    ]

    # Aplicar filtros adicionais
    if platform and platform != 'all':
        eligible_games = [game for game in eligible_games if game.get('Plataforma', '').lower() == platform.lower()]
    
    if style and style != 'all':
        eligible_games = [
            game for game in eligible_games
            if game.get('Estilo') and style.lower() in game.get('Estilo', '').lower()
        ]
    
    if min_metacritic is not None:
        eligible_games = [
            game for game in eligible_games
            if game.get('Metacritic') and int(game.get('Metacritic')) >= min_metacritic
        ]
    
    if max_metacritic is not None:
        eligible_games = [
            game for game in eligible_games
            if game.get('Metacritic') and int(game.get('Metacritic')) <= max_metacritic
        ]

    if not eligible_games:
        return None

    # Sortear um jogo da lista filtrada
    random_game = random.choice(eligible_games)
    return random_game
# --- FIM NOVA FUNÇÃO PARA SORTEAR JOGO ---

# --- NOVA FUNÇÃO PARA JOGOS SIMILARES ---
def get_similar_games(rawg_id):
    """
    Busca jogos similares a um determinado RAWG_ID, excluindo jogos já possuídos.
    """
    if not Config.RAWG_API_KEY:
        return {"success": False, "message": "Chave da API da RAWG não configurada."}
    
    if not rawg_id: # Adiciona verificação para RAWG_ID nulo ou vazio
        return {"success": False, "message": "RAWG ID não fornecido para buscar jogos similares."}

    try:
        # Tenta buscar jogos da mesma série primeiro
        series_url = f"https://api.rawg.io/api/games/{rawg_id}/game-series?key={Config.RAWG_API_KEY}"
        response = requests.get(series_url)
        response.raise_for_status()
        series_data = response.json().get('results', [])
        
        # Se não houver jogos na série, busca jogos sugeridos
        if not series_data:
            suggested_url = f"https://api.rawg.io/api/games/{rawg_id}/suggested?key={Config.RAWG_API_KEY}"
            response = requests.get(suggested_url)
            response.raise_for_status()
            similar_rawg_games = response.json().get('results', [])
        else:
            similar_rawg_games = series_data

        # Obter jogos da biblioteca do usuário para filtrar
        games_data = _get_data_from_sheet('Jogos')
        owned_game_names_lower = {game.get('Nome', '').lower() for game in games_data}

        filtered_similar_games = []
        for game in similar_rawg_games:
            game_name = game.get('name')
            if not game_name:
                continue # Ignora jogos sem nome

            # Verifica se o jogo já está na biblioteca do usuário
            is_owned = game_name.lower() in owned_game_names_lower
            if is_owned:
                continue # Se já possui, não inclui na lista de similares a serem exibidos

            genres_pt = [g['name'] for g in game.get('genres', [])]
            game_tags = game.get('tags') or []
            for tag in game_tags:
                if tag.get('language') == 'eng' and tag.get('slug') == 'souls-like':
                    if "Soulslike" not in genres_pt:
                        genres_pt.append("Soulslike")
                    break

            filtered_similar_games.append({
                'id': game.get('id'),
                'name': game_name,
                'background_image': game.get('background_image'),
                'styles': ', '.join([GENRE_TRANSLATIONS.get(g, g) for g in genres_pt]),
                'is_owned': is_owned # Mantém a flag, embora não será usada para filtrar aqui
            })
        
        return {"success": True, "similar_games": filtered_similar_games[:5]} # Limita a 5 jogos similares
    except requests.exceptions.RequestException as e:
        print(f"ERRO DE CONEXÃO COM A API DA RAWG ao buscar jogos similares: {e}")
        return {"success": False, "message": "Falha ao se comunicar com a API da RAWG para jogos similares."}
    except Exception as e:
        print(f"ERRO INESPERADO ao buscar jogos similares: {e}")
        traceback.print_exc()
        return {"success": False, "message": "Ocorreu um erro interno no servidor."}
# --- FIM NOVA FUNÇÃO PARA JOGOS SIMILARES ---
