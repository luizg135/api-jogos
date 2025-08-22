import gspread
import pandas as pd
import json
import math
from oauth2client.service_account import ServiceAccountCredentials
from config import Config
from datetime import datetime, timedelta
import traceback
import requests
import uuid # Para gerar IDs únicos para as notificações

def _get_sheet(sheet_name):
    """
    Tenta obter uma planilha específica pelo nome.
    Adicionado log de erros detalhado.
    """
    try:
        creds_json = json.loads(Config.GOOGLE_SHEETS_CREDENTIALS_JSON)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(Config.GAME_SHEET_URL)
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"ERRO: Planilha com a URL '{Config.GAME_SHEET_URL}' não encontrada.")
        traceback.print_exc()
        return None
    except gspread.exceptions.WorksheetNotFound:
        print(f"ERRO: Aba '{sheet_name}' não encontrada na planilha.")
        print(f"Por favor, certifique-se de que a aba '{sheet_name}' existe e está com o nome correto.")
        traceback.print_exc()
        return None
    except json.JSONDecodeError:
        print("ERRO: GOOGLE_SHEETS_CREDENTIALS_JSON não é um JSON válido.")
        print("Verifique a formatação da sua variável de ambiente GOOGLE_SHEETS_CREDENTIALS.")
        traceback.print_exc()
        return None
    except Exception as e:
        print(f"ERRO CRÍTICO ao autenticar ou acessar a planilha: {e}")
        traceback.print_exc()
        return None

def _get_data_from_sheet(sheet):
    """
    Tenta obter todos os registros de uma planilha.
    Adicionado log de erros detalhado.
    """
    if sheet is None:
        print("AVISO: Tentativa de obter dados de uma planilha nula.")
        return []
    try:
        return sheet.get_all_records()
    except gspread.exceptions.APIError as e:
        if "unable to parse range" in str(e):
            print(f"AVISO: Erro de API ao ler dados da aba '{sheet.title}'. Pode ser uma aba vazia ou com formato inesperado. Erro: {e}")
            return []
        print(f"ERRO de API ao ler dados da aba '{sheet.title}': {e}")
        traceback.print_exc()
        return []
    except Exception as e:
        print(f"ERRO genérico ao ler dados da aba '{sheet.title}': {e}")
        traceback.print_exc()
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
    """
    Retorna todos os dados de jogos, desejos, perfil e conquistas,
    e agora também gera notificações de lançamento da wishlist.
    """
    try:
        game_sheet = _get_sheet('Jogos')
        games_data = _get_data_from_sheet(game_sheet) if game_sheet else []

        wishlist_sheet = _get_sheet('Desejos')
        all_wishlist_data = _get_data_from_sheet(wishlist_sheet) if wishlist_sheet else []
        wishlist_data_filtered = [item for item in all_wishlist_data if item.get('Status') != 'Comprado']

        profile_sheet = _get_sheet('Perfil')
        profile_records = _get_data_from_sheet(profile_sheet) if profile_sheet else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}

        achievements_sheet = _get_sheet('Conquistas')
        all_achievements = _get_data_from_sheet(achievements_sheet) if achievements_sheet else []
        
        # Obter notificações existentes para evitar duplicação
        notifications_sheet = _get_sheet('Notificações')
        all_notifications = _get_data_from_sheet(notifications_sheet) if notifications_sheet else []

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

        completed_achievements, pending_achievements = _check_achievements(games_data, base_stats, all_achievements, all_wishlist_data)
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        final_stats = {**base_stats, **gamer_stats}

        # Gerar notificações de lançamento de jogos da lista de desejos
        _check_wishlist_releases(all_wishlist_data, all_notifications)

        return {
            'estatisticas': final_stats, 'biblioteca': games_data, 'desejos': wishlist_data_filtered, 'perfil': profile_data,
            'conquistas_concluidas': completed_achievements,
            'conquistas_pendentes': pending_achievements
        }
    except Exception as e:
        print(f"ERRO ao buscar dados completos do jogo: {e}")
        traceback.print_exc()
        return { 'estatisticas': {}, 'biblioteca': [], 'desejos': [], 'perfil': {}, 'conquistas_concluidas': [], 'conquistas_pendentes': [] }

def get_public_profile_data():
    try:
        game_sheet = _get_sheet('Jogos'); games_data = _get_data_from_sheet(game_sheet) if game_sheet else []
        profile_sheet = _get_sheet('Perfil'); profile_records = _get_data_from_sheet(profile_sheet) if profile_sheet else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet = _get_sheet('Conquistas'); all_achievements = _get_data_from_sheet(achievements_sheet) if achievements_sheet else []

        # Calcula as estatísticas públicas
        tempos_de_jogo = [int(str(g.get('Tempo de Jogo', 0)).replace('h', '')) for g in games_data]
        notas = [float(str(g.get('Nota', 0)).replace(',', '.')) for g in games_data if g.get('Nota')]

        base_stats = {
            'total_jogos': len(games_data),
            'total_platinados': len([g for g in games_data if g.get('Platinado?') == 'Sim']),
            'total_horas_jogadas': sum(tempos_de_jogo),
            'media_notas': round(sum(notas) / len(notas), 2) if notas else 0,
            'total_conquistas': sum([int(g.get('Conquistas Obtidas', 0)) for g in games_data]),
        }

        # Conquistas desbloqueadas para o cálculo do nível e rank
        completed_achievements, _ = _check_achievements(games_data, base_stats, all_achievements, [])
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
        print(f"ERRO ao buscar dados do perfil público: {e}"); traceback.print_exc()
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
        _invalidate_cache()
        return {"success": True, "message": "Perfil atualizado com sucesso."}
    except Exception as e:
        print(f"ERRO ao atualizar perfil: {e}"); traceback.print_exc()
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
                    # Adiciona os novos dados ao dicionário que será salvo
                    game_data['Descricao'] = (description[:495] + '...') if len(description) > 500 else description
                    game_data['Metacritic'] = details.get('metacritic', '')

                    # CORREÇÃO AQUI: Usando 'short_screenshots' para pegar as imagens
                    screenshots_list = [sc.get('image') for sc in details.get('short_screenshots', [])[:3]]
                    game_data['Screenshots'] = ', '.join(screenshots_list)
            except requests.exceptions.RequestException as e:
                print(f"ERRO ao buscar detalhes da RAWG para o ID {rawg_id}: {e}")

        sheet = _get_sheet('Jogos')
        if not sheet:
            return {"success": False, "message": "Conexão com a planilha falhou."}

        # Lógica dinâmica para salvar todos os dados
        headers = sheet.row_values(1)
        row_data = [game_data.get(header, '') for header in headers]

        sheet.append_row(row_data)
        _invalidate_cache()
        return {"success": True, "message": "Jogo adicionado com sucesso."}
    except Exception as e:
        print(f"ERRO ao adicionar jogo: {e}"); traceback.print_exc()
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
        _invalidate_cache()
        return {"success": True, "message": "Item de desejo adicionado com sucesso."}
    except Exception as e:
        print(f"ERRO ao adicionar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar item de desejo."}
    
def update_game_in_sheet(game_name, updated_data):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        try: cell = sheet.find(game_name)
        except gspread.exceptions.CellNotFound: return {"success": False, "message": "Jogo não encontrado."}
        
        # Captura o estado anterior do jogo para verificar mudanças que podem gerar notificações
        previous_game_data = sheet.row_values(cell.row)
        headers = sheet.row_values(1)
        previous_game_dict = dict(zip(headers, previous_game_data))

        column_map = {
            'Nome': 0, 'Plataforma': 1, 'Status': 2, 'Nota': 3, 'Preço': 4,
            'Tempo de Jogo': 5, 'Conquistas Obtidas': 6, 'Platinado?': 7,
            'Estilo': 8, 'Link': 9, 'Adquirido em': 10, 'Início em': 11,
            'Terminado em': 12, 'Conclusão': 13, 'Abandonado?': 14
        }
        new_row = list(previous_game_data) # Começa com os valores existentes
        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                while len(new_row) <= col_index: new_row.append('')
                new_row[col_index] = value
        
        sheet.update(f'A{cell.row}', [new_row])
        
        # --- Lógica de Geração de Notificações de Conquistas ---
        # A lógica completa de conquistas é mais complexa, mas para este exemplo,
        # vamos simular uma notificação se o status mudar para 'Platinado' e não era antes.
        if previous_game_dict.get('Platinado?') != 'Sim' and updated_data.get('Platinado?') == 'Sim':
            _add_notification({
                'Tipo': 'Conquista',
                'Mensagem': f"Parabéns! Você platinou o jogo '{game_name}'!",
                'Data': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Status': 'Não Lida'
            })
        # Você pode adicionar mais lógica aqui para outros tipos de conquistas
        # Por exemplo, se 'Conquistas Obtidas' aumentar significativamente, etc.

        _invalidate_cache()
        return {"success": True, "message": "Jogo atualizado com sucesso."}
    except Exception as e:
        print(f"ERRO ao atualizar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar jogo."}
        
def delete_game_from_sheet(game_name):
    try:
        sheet = _get_sheet('Jogos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(game_name)
        if not cell: return {"success": False, "message": "Jogo não encontrado."}
        sheet.delete_rows(cell.row)
        _invalidate_cache()
        return {"success": True, "message": "Jogo deletado com sucesso."}
    except Exception as e:
        print(f"ERRO ao deletar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar jogo."}
    
def update_wish_in_sheet(wish_name, updated_data):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(wish_name)
        if not cell: return {"success": False, "message": "Item de desejo não encontrado."}
        row_values = sheet.row_values(1)
        column_map = {'Nome': 0, 'Link': 1, 'Data Lançamento': 2, 'Preço': 3}
        new_row = list(row_values)
        for key, value in updated_data.items():
            if key in column_map:
                col_index = column_map[key]
                while len(new_row) <= col_index: new_row.append('')
                new_row[col_index] = value
        sheet.update(f'A{cell.row}', [new_row])
        _invalidate_cache()
        return {"success": True, "message": "Item de desejo atualizado com sucesso."}
    except Exception as e:
        print(f"ERRO ao atualizar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao atualizar item de desejo."}

def delete_wish_from_sheet(wish_name):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        cell = sheet.find(wish_name)
        if not cell: return {"success": False, "message": "Item de desejo não encontrado."}
        sheet.delete_rows(cell.row)
        _invalidate_cache()
        return {"success": True, "message": "Item de desejo deletado com sucesso."}
    except Exception as e:
        print(f"ERRO ao deletar item de desejo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao deletar item de desejo."}

def _invalidate_cache():
    pass # No cache implementation yet

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
            _invalidate_cache()
            return {"success": True, "message": "Item marcado como comprado!"}
        except ValueError:
            return {"success": False, "message": "Coluna 'Status' não encontrada na planilha de Desejos."}

    except Exception as e:
        print(f"ERRO ao marcar item como comprado: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao processar a compra."}

# --- Funções de Notificação ---

def _add_notification(notification_data):
    """
    Adiciona uma nova notificação à planilha 'Notificações'.
    notification_data deve ser um dicionário com 'Tipo', 'Mensagem', 'Data', 'Status'.
    Um ID único será gerado automaticamente.
    """
    try:
        sheet = _get_sheet('Notificações')
        if not sheet: 
            print("ERRO: Conexão com a planilha de notificações falhou ao tentar adicionar notificação.")
            return {"success": False, "message": "Conexão com a planilha de notificações falhou."}

        headers = sheet.row_values(1)
        # Garante que o ID seja único
        notification_data['ID'] = str(uuid.uuid4())
        
        row_data = [notification_data.get(header, '') for header in headers]
        sheet.append_row(row_data)
        _invalidate_cache()
        return {"success": True, "message": "Notificação adicionada com sucesso."}
    except Exception as e:
        print(f"ERRO ao adicionar notificação: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar notificação."}

def _check_wishlist_releases(wishlist_data, existing_notifications):
    """
    Verifica a lista de desejos para jogos com lançamento próximo e gera notificações.
    Evita gerar notificações duplicadas para o mesmo evento.
    """
    today = datetime.now().date()
    
    # IDs de notificações de lançamento já existentes para evitar duplicação
    existing_release_notifications = set()
    for n in existing_notifications:
        data_string = n.get('Data')
        if data_string and n.get('Tipo') == 'Lançamento':
            try:
                # Usa um try-except para lidar com formatos de data inválidos
                data_parte = data_string.split(' ')[0]
                existing_release_notifications.add((n.get('Mensagem'), data_parte))
            except Exception as e:
                print(f"AVISO: Pulando notificação com data mal formatada: '{data_string}'. Erro: {e}")


    for item in wishlist_data:
        release_date_str = item.get('Data Lançamento')
        game_name = item.get('Nome')

        if not release_date_str or not game_name:
            continue

        release_date = None
        try:
            # Tenta parsear a data no formato DD/MM/YYYY
            release_date = datetime.strptime(release_date_str, '%d/%m/%Y').date()
        except ValueError:
            # Se não conseguir, tenta YYYY-MM-DD (formato de input date)
            try:
                release_date = datetime.strptime(release_date_str, '%Y-%m-%d').date()
            except ValueError:
                print(f"AVISO: Data de lançamento inválida para o jogo '{game_name}': '{release_date_str}'. Pulando notificação.")
                continue # Pula se a data não estiver em um formato reconhecido

        delta = release_date - today
        message = None
        notification_date = today.strftime('%Y-%m-%d')

        if message:
            # Verifica se a notificação já existe para hoje
            if (message, notification_date) not in existing_release_notifications:
                _add_notification({
                    'Tipo': 'Lançamento',
                    'Mensagem': message,
                    'Data': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Status': 'Não Lida'
                })
                existing_release_notifications.add((message, notification_date)) # Adiciona para evitar duplicação em chamadas futuras no mesmo dia

def get_notifications():
    """
    Retorna todas as notificações, separando-as em lidas e não lidas.
    """
    try:
        sheet = _get_sheet('Notificações')
        if not sheet: 
            print("AVISO: Planilha 'Notificações' não encontrada ou inacessível. Retornando notificações vazias.")
            return {"unread": [], "read": []}
        
        all_notifications = _get_data_from_sheet(sheet)
        
        unread_notifications = [n for n in all_notifications if n.get('Status') == 'Não Lida']
        read_notifications = [n for n in all_notifications if n.get('Status') == 'Lida']

        # Função de chave de ordenação segura que trata datas inválidas
        def safe_date_sort_key(x):
            try:
                # Tenta converter a data no formato esperado
                return datetime.strptime(x.get('Data', ''), '%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                # Se falhar, retorna uma data muito antiga para que a notificação
                # seja movida para o final da lista
                return datetime.min

        # Ordena as notificações usando a função de chave segura
        unread_notifications.sort(key=safe_date_sort_key, reverse=True)
        read_notifications.sort(key=safe_date_sort_key, reverse=True)

        return {"unread": unread_notifications, "read": read_notifications}
    except Exception as e:
        print(f"ERRO ao obter notificações: {e}"); traceback.print_exc()
        return {"unread": [], "read": []}

def mark_notifications_as_read(notification_ids):
    """
    Marca uma lista de notificações como 'Lida' na planilha.
    """
    try:
        sheet = _get_sheet('Notificações')
        if not sheet: 
            print("ERRO: Conexão com a planilha de notificações falhou ao tentar marcar como lida.")
            return {"success": False, "message": "Conexão com a planilha de notificações falhou."}

        # Busca todas as notificações para encontrar as linhas correspondentes
        all_records = sheet.get_all_records()
        headers = sheet.row_values(1)
        
        updates = []
        for i, record in enumerate(all_records):
            if record.get('ID') in notification_ids and record.get('Status') == 'Não Lida':
                row_index = i + 2 # +2 porque get_all_records é 0-index e a planilha é 1-index, e a primeira linha são os cabeçalhos
                status_col_index = headers.index('Status') + 1 # +1 porque gspread é 1-index para colunas
                updates.append({
                    'range': f"{gspread.utils.rowcol_to_a1(row_index, status_col_index)}",
                    'values': [['Lida']]
                })
        
        if updates:
            sheet.batch_update(updates)
            _invalidate_cache()
            return {"success": True, "message": f"{len(updates)} notificações marcadas como lidas."}
        else:
            return {"success": False, "message": "Nenhuma notificação para marcar como lida ou IDs inválidos."}

    except Exception as e:
        print(f"ERRO ao marcar notificações como lidas: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao marcar notificações como lidas."}