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
import pytz # Importar pytz

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
        'JOGOS_LONGOS': len([g for g in games_data if g.get('Tempo de Jogo') and int(str(g['Tempo de Jogo']).replace('h', '')) >= 50]),
        'SOULSLIKE_PLATINADOS': len([g for g in games_data if g.get('Platinado?') == 'Sim' and 'Soulslike' in g.get('Estilo', '')]),
        'INDIE_TOTAL': len([g for g in games_data if 'Indie' in g.get('Estilo', '')]),
        'JOGO_MAIS_JOGADO': stats.get('max_horas_um_jogo', 0),
        'FINALIZADOS_ACAO': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Ação' in g.get('Estilo', '')]),
        'FINALIZADOS_ESTRATEGIA': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Estratégia' in g.get('Estilo', '')]),
        'GENEROS_DIFERENTES': len(set(g for game in games_data if game.get('Estilo') for g in game.get('Estilo').split(','))),
        'NOTAS_10': len([n for n in [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')] if n == 100]),
        'NOTAS_BAIXAS': len([n for n in [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')] if n <= 30]),
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
    return {'nivel_gamer': nivel, 'rank_gamer': rank_name, 'exp_nivel_atual': exp_no_nivel_atual, 'exp_para_proximo_nivel': exp_per_level}

# --- Funções para gerenciar notificações ---
def _get_notifications_sheet():
    """Retorna a aba de notificações."""
    return _get_sheet('Notificações')

def _add_notification(notification_type, message):
    """Adiciona uma nova notificação à planilha, evitando duplicatas recentes."""
    sheet = _get_notifications_sheet()
    if not sheet:
        print("ERRO: Conexão com a planilha de notificações falhou ao tentar adicionar notificação.")
        return {"success": False, "message": "Conexão com a planilha de notificações falhou."}

    notifications = _get_data_from_sheet(sheet)
    for notif in notifications:
        # Considera uma notificação duplicada se for do mesmo tipo e mensagem
        # e não foi marcada como lida (ou seja, ainda está ativa ou foi lida mas queremos evitar repetição)
        if notif.get('Tipo') == notification_type and \
           notif.get('Mensagem') == message:
            print(f"Notificação duplicada evitada: Tipo='{notification_type}', Mensagem='{message}'")
            return {"success": False, "message": "Notificação duplicada evitada."}

    new_id = len(notifications) + 1
    
    # --- MODIFICAÇÃO: Usar fuso horário de Brasília ---
    brasilia_tz = pytz.timezone('America/Sao_Paulo')
    timestamp = datetime.now(brasilia_tz).strftime("%Y-%m-%d %H:%M:%S")
    # --- FIM MODIFICAÇÃO ---
    
    row_data = [new_id, notification_type, message, timestamp, 'Não']
    sheet.append_row(row_data)
    print(f"Notificação adicionada: ID={new_id}, Tipo='{notification_type}', Mensagem='{message}'")
    return {"success": True, "message": "Notificação adicionada com sucesso."}

def get_all_notifications_for_frontend():
    """Retorna TODAS as notificações (lidas e não lidas) para o frontend."""
    sheet = _get_notifications_sheet()
    if not sheet:
        print("ERRO: Conexão com a planilha de notificações falhou ao tentar buscar todas as notificações.")
        return []
    notifications = _get_data_from_sheet(sheet)
    
    # Processa as notificações para garantir tipos corretos para o frontend
    processed_notifications = []
    for notif in notifications:
        processed_notif = {
            'ID': int(notif.get('ID', 0)), # Garante que ID seja int
            'Tipo': notif.get('Tipo', ''),
            'Mensagem': notif.get('Mensagem', ''),
            'Data': notif.get('Data', ''),
            'Lida': str(notif.get('Lida', 'Não')) # Garante que Lida seja string 'Sim' ou 'Não'
        }
        processed_notifications.append(processed_notif)
    
    # Ordena as notificações pelas mais recentes primeiro
    processed_notifications.sort(key=lambda x: datetime.strptime(x['Data'], "%Y-%m-%d %H:%M:%S"), reverse=True)

    print(f"Total de notificações (lidas e não lidas) encontradas para o frontend: {len(processed_notifications)}")
    # --- NOVO LOG: Imprime o status 'Lida' de algumas notificações para depuração ---
    for i, notif in enumerate(processed_notifications[:5]): # Imprime as 5 primeiras
        print(f"  Notificação {notif['ID']} - Tipo: {notif['Tipo']}, Lida: '{notif['Lida']}'")
    # --- FIM NOVO LOG ---
    return processed_notifications

def mark_notification_as_read(notification_id):
    """Marca uma notificação específica como lida."""
    sheet = _get_notifications_sheet()
    if not sheet:
        print("ERRO: Conexão com a planilha de notificações falhou ao tentar marcar como lida.")
        return {"success": False, "message": "Conexão com a planilha de notificações falhou."}
    
    try:
        # Busca todas as linhas para encontrar a notificação pelo ID
        all_records = sheet.get_all_values()
        headers = all_records[0]
        data_rows = all_records[1:]

        id_col_index = headers.index('ID')
        lida_col_index = headers.index('Lida')

        found_row_index = -1
        for i, row in enumerate(data_rows):
            if str(row[id_col_index]) == str(notification_id):
                found_row_index = i + 2 # +2 porque headers é linha 1 e data_rows começa do 0
                break
        
        if found_row_index == -1:
            print(f"ERRO: Notificação com ID {notification_id} não encontrada na planilha.")
            return {"success": False, "message": "Notificação não encontrada."}

        # Atualiza a célula na planilha
        sheet.update_cell(found_row_index, lida_col_index + 1, 'Sim') # +1 para converter index 0-based para 1-based
        print(f"Notificação {notification_id} marcada como lida na planilha. Linha: {found_row_index}, Coluna Lida: {lida_col_index + 1}")
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
        game_sheet = _get_sheet('Jogos'); games_data = _get_data_from_sheet(game_sheet) if game_sheet else []
        wishlist_sheet = _get_sheet('Desejos')
        all_wishlist_data = _get_data_from_sheet(wishlist_sheet) if wishlist_sheet else []
        wishlist_data_filtered = [item for item in all_wishlist_data if item.get('Status') != 'Comprado']
        profile_sheet = _get_sheet('Perfil'); profile_records = _get_data_from_sheet(profile_sheet) if profile_sheet else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet = _get_sheet('Conquistas'); all_achievements = _get_data_from_sheet(achievements_sheet) if achievements_sheet else []
        
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
            'max_horas_um_jogo': max(tempos_de_jogo) if tempos_de_jogo else 0,
            'total_finalizados_acao': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Ação' in g.get('Estilo', '')]),
            'total_finalizados_estrategia': len([g for g in games_data if g.get('Status') in ['Finalizado', 'Platinado'] and 'Estratégia' in g.get('Estilo', '')]),
            'total_generos_diferentes': len(set(g for game in games_data if game.get('Estilo') for g in game.get('Estilo').split(','))),
            'total_notas_10': len([n for n in notas if n == 100]),
            'total_notas_baixas': len([n for n in notas if n <= 30]),
        }

        completed_achievements, pending_achievements = _check_achievements(games_data, base_stats, all_achievements, all_wishlist_data)
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        final_stats = {**base_stats, **gamer_stats}

        # --- MODIFICAÇÃO: Lógica para notificar conquistas do aplicativo ---
        for ach in completed_achievements:
            notification_message = f"Você desbloqueou a conquista: '{ach.get('Nome')}'!"
            # Verifica se já existe uma notificação para esta conquista específica
            if not any(n.get('Tipo') == "Conquista Desbloqueada" and n.get('Mensagem') == notification_message for n in existing_notifications):
                _add_notification("Conquista Desbloqueada", notification_message)
        # --- FIM MODIFICAÇÃO ---

        # --- NOVO: Lógica para notificar lançamentos próximos da lista de desejos ---
        brasilia_tz = pytz.timezone('America/Sao_Paulo')
        today = datetime.now(brasilia_tz).replace(hour=0, minute=0, second=0, microsecond=0) # Data de hoje em Brasília, sem hora
        for wish in all_wishlist_data:
            release_date_str = wish.get('Data Lançamento')
            if release_date_str:
                try:
                    # Tenta diferentes formatos de data
                    if '/' in release_date_str: # dd/mm/yyyy
                        release_date = datetime.strptime(release_date_str, "%d/%m/%Y")
                    elif '-' in release_date_str: # yyyy-mm-dd (formato comum de APIs)
                        release_date = datetime.strptime(release_date_str, "%Y-%m-%d")
                    else:
                        continue # Ignora datas em formato desconhecido

                    # Remove a parte da hora da data de lançamento para comparação justa
                    release_date = release_date.replace(hour=0, minute=0, second=0, microsecond=0)

                    time_to_release = release_date - today
                    
                    # Notifica se faltam 7 dias ou menos e a data ainda não passou
                    if timedelta(days=0) <= time_to_release <= timedelta(days=7):
                        notification_message = f"O jogo '{wish.get('Nome')}' será lançado em {time_to_release.days} dias!"
                        if time_to_release.days == 0:
                            notification_message = f"O jogo '{wish.get('Nome')}' foi lançado hoje!"
                        
                        # Evita notificações duplicadas para o mesmo jogo e tipo
                        if not any(n.get('Tipo') == "Lançamento Próximo" and n.get('Mensagem') == notification_message for n in existing_notifications):
                            _add_notification("Lançamento Próximo", notification_message)
                except ValueError:
                    print(f"AVISO: Data de lançamento inválida para '{wish.get('Nome')}': {release_date_str}")
                except Exception as e:
                    print(f"ERRO ao processar data de lançamento para '{wish.get('Nome')}': {e}")
        # --- FIM NOVO ---

        return {
            'estatisticas': final_stats, 'biblioteca': games_data, 'desejos': wishlist_data_filtered, 'perfil': profile_data,
            'conquistas_concluidas': completed_achievements,
            'conquistas_pendentes': pending_achievements
        }
    except Exception as e:
        print(f"Erro ao buscar dados: {e}"); traceback.print_exc()
        return { 'estatisticas': {}, 'biblioteca': [], 'desejos': [], 'perfil': {}, 'conquistas_concluidas': [], 'conquistas_pendentes': [] }

def get_public_profile_data():
    try:
        game_sheet = _get_sheet('Jogos'); games_data = _get_data_from_sheet(game_sheet) if game_sheet else []
        wishlist_sheet = _get_sheet('Desejos')
        all_wishlist_data = _get_data_from_sheet(wishlist_sheet) if wishlist_sheet else []
        profile_sheet = _get_sheet('Perfil'); profile_records = _get_data_from_sheet(profile_sheet) if profile_sheet else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet = _get_sheet('Conquistas'); all_achievements = _get_data_from_sheet(achievements_sheet) if achievements_sheet else []

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
            'max_horas_um_jogo': max(tempos_de_jogo) if tempos_de_jogo else 0,
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
        print(f"Erro ao buscar dados do perfil público: {e}"); traceback.print_exc()
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
        return {"success": True, "message": "Perfil atualizado com sucesso."}
    except Exception as e:
        print(f"Erro ao atualizar perfil: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar perfil."}

def add_game_to_sheet(game_data):
    try:
        # Pega o ID da RAWG dos dados recebidos
        rawg_id = game_data.get('RAWG_ID')

        if rawg_id and Config.RAWG_API_KEY:
            try:
                # Busca os detalhes completos na API da RAWG
                url = f"https://api.rawg.io/api/games/{rawg_id}?key={Config.RAWG_API_KEY}"
                response = requests.get(url)
                if response.ok:
                    details = response.json()
                    description = details.get('description_raw', '')
                    
                    # --- NOVO: Lógica de tradução com DeepL ---
                    translated_description = ""
                    if Config.DEEPL_API_KEY and description:
                        try:
                            translator = deepl.Translator(Config.DEEPL_API_KEY) #
                            # Traduz do idioma detectado para Português do Brasil
                            result = translator.translate_text(description, target_lang="PT-BR")
                            translated_description = result.text
                            print(f"Descrição traduzida com sucesso: {translated_description[:50]}...")
                        except Exception as deepl_e:
                            print(f"Erro ao traduzir com DeepL: {deepl_e}")
                            translated_description = description # Em caso de erro, usa a original
                    else:
                        translated_description = description

                    # Adiciona a descrição traduzida ao dicionário que será salvo
                    game_data['Descricao'] = translated_description
                    
                    game_data['Metacritic'] = details.get('metacritic', '')

                    # CORREÇÃO AQUI: Usando 'short_screenshots' para pegar as imagens
                    screenshots_list = [sc.get('image') for sc in details.get('short_screenshots', [])[:3]]
                    game_data['Screenshots'] = ', '.join(screenshots_list)
            except requests.exceptions.RequestException as e:
                print(f"Erro ao buscar detalhes da RAWG para o ID {rawg_id}: {e}")

        sheet = _get_sheet('Jogos')
        if not sheet:
            return {"success": False, "message": "Conexão com a planilha falhou."}

        # Lógica dinâmica para salvar todos os dados
        headers = sheet.row_values(1)
        row_data = [game_data.get(header, '') for header in headers]

        sheet.append_row(row_data)
        
        # --- NOVO: Geração de notificação ao adicionar jogo ---
        _add_notification("Novo Jogo Adicionado", f"Você adicionou '{game_data.get('Nome', 'Um novo jogo')}' à sua biblioteca!")
        # --- FIM NOVO ---

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

        # --- NOVO: Geração de notificação ao adicionar item à wishlist ---
        _add_notification("Novo Desejo Adicionado", f"Você adicionou '{wish_data.get('Nome', 'Um novo jogo')}' à sua lista de desejos!")
        # --- FIM NOVO ---

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
        
        # Pega os dados antigos do jogo para comparação
        old_game_data = sheet.row_values(cell.row)
        headers = sheet.row_values(1)
        old_game_dict = dict(zip(headers, old_game_data))

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
                # Conversão para float para Nota e Preço antes de salvar
                if key in ['Nota', 'Preço'] and value is not None:
                    new_row[col_index] = float(value)
                else:
                    new_row[col_index] = value

        sheet.update(f'A{cell.row}', [new_row])
        
        # --- NOVO: Geração de notificações com base em atualizações ---
        # Verifica se o jogo foi platinado
        if old_game_dict.get('Platinado?', 'Não') == 'Não' and updated_data.get('Platinado?') == 'Sim':
            _add_notification("Jogo Platinado", f"Parabéns! Você platinou '{updated_data.get('Nome', game_name)}'!")
        
        # Verifica se o status mudou para finalizado
        if old_game_dict.get('Status') not in ['Finalizado', 'Platinado'] and updated_data.get('Status') == 'Finalizado':
            _add_notification("Jogo Finalizado", f"Você finalizou '{updated_data.get('Nome', game_name)}'!")

        # --- REMOVIDO: Notificação de 'Conquistas Desbloqueadas' daqui, será tratada em get_all_game_data ---
        # old_conquistas = int(old_game_dict.get('Conquistas Obtidas', 0))
        # new_conquistas = int(updated_data.get('Conquistas Obtidas', 0))
        # if new_conquistas > old_conquistas:
        #     _add_notification("Conquistas Desbloqueadas", f"Você desbloqueou {new_conquistas - old_conquistas} novas conquistas em '{updated_data.get('Nome', game_name)}'!")
        # --- FIM REMOVIDO ---

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
        
        # --- NOVO: Geração de notificação ao deletar jogo ---
        _add_notification("Jogo Removido", f"O jogo '{game_name}' foi removido da sua biblioteca.")
        # --- FIM NOVO ---

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
        row_values = sheet.row_values(1)
        column_map = {'Nome': 0, 'Link': 1, 'Data Lançamento': 2, 'Status': 3, 'Preço': 4} # Adicionado 'Status'
        new_row = list(row_values)
        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                while len(new_row) <= col_index: new_row.append('')
                # Conversão para float para Preço antes de salvar
                if key == 'Preço' and value is not None:
                    new_row[col_index] = float(value)
                else:
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
        
        # --- NOVO: Geração de notificação ao deletar item da wishlist ---
        _add_notification("Desejo Removido", f"O item '{wish_name}' foi removido da sua lista de desejos.")
        # --- FIM NOVO ---

        return {"success": True, "message": "Item de desejo deletado com sucesso."}
    except Exception as e:
        print(f"Erro ao deletar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar item de desejo."}

def _invalidate_cache():
    pass

def purchase_wish_item_in_sheet(item_name):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}

        try:
            cell = sheet.find(item_name)
        except gspread.exceptions.CellNotFound:
            return {"success": False, "message": "Item de desejo não encontrado."}

        # Encontra a coluna "Status" e atualiza a célula
        headers = sheet.row_values(1)
        try:
            status_col_index = headers.index('Status') + 1
            sheet.update_cell(cell.row, status_col_index, 'Comprado')
            
            # --- NOVO: Geração de notificação ao comprar item da wishlist ---
            _add_notification("Desejo Comprado", f"Você marcou '{item_name}' como comprado! Aproveite o jogo!")
            # --- FIM NOVO ---

            return {"success": True, "message": "Item marcado como comprado!"}
        except ValueError:
            return {"success": False, "message": "Coluna 'Status' não encontrada na planilha de Desejos."}

    except Exception as e:
        print(f"Erro ao marcar item como comprado: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao processar a compra."}
