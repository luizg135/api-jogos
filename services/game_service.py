import gspread
import pandas as pd
import json
import math
from oauth2client.service_account import ServiceAccountCredentials
from config import Config
from datetime import datetime, timedelta
import traceback
import requests
from config import Config
import uuid

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
    
    # Obtém todas as notificações existentes para evitar duplicatas
    notifications = get_notifications()
    notified_achievements = {n.get('Jogo') for n in notifications.get('read', []) + notifications.get('unread', []) if n.get('Tipo') == 'Conquista'}
    
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
            # Verifica se já existe uma notificação para essa conquista antes de criá-la
            if ach.get('Nome') not in notified_achievements:
                create_notification("Conquista", f"Parabéns! Você desbloqueou a conquista '{ach.get('Nome')}'!", ach.get('Nome'))
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
            # Lida com valores vazios ou não numéricos
            nota_str = str(game.get('Nota', '0')).replace(',', '.')
            if nota_str.lower() != 'none' and nota_str.strip():
                nota = float(nota_str)
                if nota > 0: total_exp += int(nota * 10)
        except (ValueError, TypeError):
            pass
        try:
            total_exp += int(game.get('Conquistas Obtidas', 0))
        except (ValueError, TypeError):
            pass

    for ach in unlocked_achievements:
        try:
            total_exp += int(ach.get('EXP', 0))
        except (ValueError, TypeError):
            pass

    exp_per_level = 1000
    nivel = math.floor(total_exp / exp_per_level)
    exp_no_nivel_atual = total_exp % exp_per_level
    ranks = {0: "Bronze", 10: "Prata", 20: "Ouro", 30: "Platina", 40: "Diamante", 50: "Mestre"}
    rank_gamer = "Bronze"
    for level_req, rank_name in ranks.items():
        if nivel >= level_req: rank_gamer = rank_name
    return {'nivel_gamer': nivel, 'rank_gamer': rank_gamer, 'exp_nivel_atual': exp_no_nivel_atual, 'exp_para_proximo_nivel': exp_per_level}

def _check_release_dates(wishlist_data):
    try:
        notifications = get_notifications()
        notified_releases = {n.get('Jogo') for n in notifications.get('read', []) + notifications.get('unread', []) if n.get('Tipo') == 'Lançamento'}
        today = datetime.now().date()
        
        for item in wishlist_data:
            release_date_str = item.get('Data Lançamento')
            item_name = item.get('Nome')
            
            # Pula jogos sem uma data de lançamento válida
            if not release_date_str or not any(char.isdigit() for char in release_date_str):
                continue
            
            try:
                # Trata o formato de data "DD/MM/AAAA"
                release_date = datetime.strptime(release_date_str, '%d/%m/%Y').date()
            except ValueError:
                print(f"Formato de data inválido para {item_name}: {release_date_str}")
                continue

            # Calcula a diferença em dias
            days_until_release = (release_date - today).days

            # Intervalos de notificação
            notification_days = [30, 15, 10, 7, 3, 2, 1]
            
            # Verifica se o jogo deve ser notificado e se ainda não foi notificado para esse tipo
            if days_until_release in notification_days and item_name not in notified_releases:
                create_notification("Lançamento", f"Faltam {days_until_release} dias para o lançamento de '{item_name}'!", item_name)
                # Adiciona o nome do jogo ao set para evitar duplicatas na mesma execução
                notified_releases.add(item_name)
    except Exception as e:
        print(f"Erro ao verificar datas de lançamento: {e}"); traceback.print_exc()

def get_all_game_data():
    try:
        game_sheet = _get_sheet('Jogos'); games_data = _get_data_from_sheet(game_sheet) if game_sheet else []
        wishlist_sheet = _get_sheet('Desejos')
        all_wishlist_data = _get_data_from_sheet(wishlist_sheet) if wishlist_sheet else []
        wishlist_data_filtered = [item for item in all_wishlist_data if item.get('Status') != 'Comprado']
        profile_sheet = _get_sheet('Perfil'); profile_records = _get_data_from_sheet(profile_sheet) if profile_sheet else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet = _get_sheet('Conquistas'); all_achievements = _get_data_from_sheet(achievements_sheet) if achievements_sheet else []

        def sort_key(game):
            try: 
                nota_str = str(game.get('Nota', '-1')).replace(',', '.')
                nota = float(nota_str) if nota_str.lower() != 'none' else -1
            except (ValueError, TypeError): 
                nota = -1
            return (-nota, game.get('Nome', '').lower())
        games_data.sort(key=sort_key)

        notas = []
        for game in games_data:
            nota_str = str(game.get('Nota', '0')).replace(',', '.')
            if nota_str.lower() != 'none' and nota_str.strip():
                try:
                    notas.append(float(nota_str))
                except (ValueError, TypeError):
                    pass
                    
        tempos_de_jogo = [int(str(game.get('Tempo de Jogo', 0)).replace('h', '')) for game in games_data]
        
        base_stats = {
            'total_jogos': len(games_data),
            'total_finalizados': len([game for game in games_data if game.get('Status') in ['Finalizado', 'Platinado']]),
            'total_platinados': len([game for game in games_data if game.get('Platinado?') == 'Sim']),
            'total_avaliados': len([game for game in games_data if game.get('Nota') and str(game.get('Nota', '0')).replace(',', '.').strip().lower() != 'none']),
            'total_horas_jogadas': sum(tempos_de_jogo),
            'custo_total_biblioteca': sum([float(str(game.get('Preço', '0,00')).replace('R$', '').replace(',', '.')) for game in games_data]),
            'media_notas': round(sum(notas) / len(notas), 2) if notas else 0,
            'total_conquistas': sum([int(game.get('Conquistas Obtidas', 0)) for game in games_data if str(game.get('Conquistas Obtidas', '0')).strip().lower() != 'none']),
            'total_jogos_longos': len([t for t in tempos_de_jogo if t >= 50]),
            'total_soulslike_platinados': len([game for game in games_data if game.get('Platinado?') == 'Sim' and 'Soulslike' in game.get('Estilo', '')]),
            'total_indie': len([game for game in games_data if 'Indie' in game.get('Estilo', '')]),
            'max_horas_um_jogo': max(tempos_de_jogo) if tempos_de_jogo else 0,
            'total_finalizados_acao': len([game for game in games_data if game.get('Status') in ['Finalizado', 'Platinado'] and 'Ação' in game.get('Estilo', '')]),
            'total_finalizados_estrategia': len([game for game in games_data if game.get('Status') in ['Finalizado', 'Platinado'] and 'Estratégia' in game.get('Estilo', '')]),
            'total_generos_diferentes': len(set(g for game in games_data if game.get('Estilo') for g in game.get('Estilo').split(','))),
            'total_notas_10': len([n for n in notas if n == 10]),
            'total_notas_baixas': len([n for n in notas if n <= 3]),
        }

        completed_achievements, pending_achievements = _check_achievements(games_data, base_stats, all_achievements, all_wishlist_data)
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        final_stats = {**base_stats, **gamer_stats}
        
        _check_release_dates(all_wishlist_data)

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
        profile_sheet = _get_sheet('Perfil'); profile_records = _get_data_from_sheet(profile_sheet) if profile_sheet else []
        profile_data = {item['Chave']: item['Valor'] for item in profile_records}
        achievements_sheet = _get_sheet('Conquistas'); all_achievements = _get_data_from_sheet(achievements_sheet) if achievements_sheet else []

        # Calcula as estatísticas públicas
        tempos_de_jogo = [int(str(game.get('Tempo de Jogo', 0)).replace('h', '')) for game in games_data]
        notas = [float(str(game.get('Nota', 0)).replace(',', '.')) for game in games_data if game.get('Nota')]

        base_stats = {
            'total_jogos': len(games_data),
            'total_platinados': len([game for game in games_data if game.get('Platinado?') == 'Sim']),
            'total_horas_jogadas': sum(tempos_de_jogo),
            'media_notas': round(sum(notas) / len(notas), 2) if notas else 0,
            'total_conquistas': sum([int(game.get('Conquistas Obtidas', 0)) for game in games_data]),
        }

        # Conquistas desbloqueadas para o cálculo do nível e rank
        completed_achievements, _ = _check_achievements(games_data, base_stats, all_achievements, [])
        gamer_stats = _calculate_gamer_stats(games_data, completed_achievements)
        public_stats = {**base_stats, **gamer_stats}
        
        # Filtra os últimos 5 jogos platinados com imagens
        recent_platinums = [game for game in games_data if game.get('Platinado?') == 'Sim' and game.get('Link')]
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
        _invalidate_cache()
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
                    # Adiciona os novos dados ao dicionário que será salvo
                    game_data['Descricao'] = (description[:495] + '...') if len(description) > 500 else description
                    game_data['Metacritic'] = details.get('metacritic', '')
                    # Adiciona a RAWG_ID
                    game_data['RAWG_ID'] = rawg_id
                    
                    # Usa 'short_screenshots' para pegar as imagens
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
        create_notification("Adição", f"O jogo '{game_data.get('Nome')}' foi adicionado à sua biblioteca.", game_data.get('Nome'))
        _invalidate_cache()
        return {"success": True, "message": "Jogo adicionado com sucesso."}
    except Exception as e:
        print(f"Erro ao adicionar jogo: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao adicionar jogo."}
        
def add_wish_to_sheet(wish_data):
    try:
        sheet = _get_sheet('Desejos')
        if not sheet: return {"success": False, "message": "Conexão com a planilha falhou."}
        
        # Corrige a formatação do preço antes de salvar
        if isinstance(wish_data.get('Preço'), str):
            wish_data['Preço'] = float(wish_data['Preço'].replace(',', '.')) if wish_data['Preço'] else 0
            
        row_data = [
            wish_data.get('Nome', ''), wish_data.get('Link', ''),
            wish_data.get('Data Lançamento', ''), wish_data.get('Preço', '')
        ]
        sheet.append_row(row_data)
        create_notification("Desejo", f"O jogo '{wish_data.get('Nome')}' foi adicionado à sua lista de desejos.", wish_data.get('Nome'))
        _invalidate_cache()
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
        
        # Mapeamento de colunas para facilitar a atualização
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
                
                # Formata os valores numéricos antes de salvar
                if key in ['Nota', 'Preço']:
                    new_row[col_index] = str(value).replace('.', ',')
                else:
                    new_row[col_index] = value
                    
        sheet.update(f'A{cell.row}', [new_row])
        _invalidate_cache()
        create_notification("Atualização", f"O jogo '{game_name}' foi atualizado na sua biblioteca.", game_name)
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
        create_notification("Exclusão", f"O jogo '{game_name}' foi excluído da sua biblioteca.", game_name)
        _invalidate_cache()
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

                if key == 'Preço':
                    new_row[col_index] = str(value).replace('.', ',')
                else:
                    new_row[col_index] = value
        
        sheet.update(f'A{cell.row}', [new_row])
        create_notification("Atualização", f"O item de desejo '{wish_name}' foi atualizado.", wish_name)
        _invalidate_cache()
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
        create_notification("Exclusão", f"O item de desejo '{wish_name}' foi excluído.", wish_name)
        _invalidate_cache()
        return {"success": True, "message": "Item de desejo deletado com sucesso."}
    except Exception as e:
        print(f"Erro ao deletar item de desejo: {e}"); traceback.print_exc()
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
            create_notification("Compra", f"O jogo '{item_name}' foi marcado como comprado e removido da sua lista de desejos.", item_name)
            _invalidate_cache()
            return {"success": True, "message": "Item marcado como comprado!"}
        except ValueError:
            return {"success": False, "message": "Coluna 'Status' não encontrada na planilha de Desejos."}

    except Exception as e:
        print(f"Erro ao marcar item como comprado: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao processar a compra."}

# --- FUNÇÕES DE NOTIFICAÇÃO ---

def create_notification(notification_type, message, game_name=None):
    try:
        sheet = _get_sheet('Notificações')
        if not sheet:
            print("Erro: Planilha de Notificações não encontrada.")
            return

        # Verifica se o cabeçalho existe. Se não, cria.
        headers = sheet.row_values(1)
        required_headers = ['ID', 'Tipo', 'Mensagem', 'Data', 'Status', 'Jogo']
        if headers != required_headers:
            sheet.clear()
            sheet.append_row(required_headers)

        row = [
            str(uuid.uuid4()),
            notification_type,
            message,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "Não Lida",
            game_name
        ]
        
        # Adiciona a notificação na primeira linha para aparecer no topo
        sheet.insert_row(row, 2)

    except Exception as e:
        print(f"Erro ao criar notificação: {e}"); traceback.print_exc()
        
def get_notifications():
    try:
        sheet = _get_sheet('Notificações')
        if not sheet:
            return {"unread": [], "read": []}

        # get_all_records() lida melhor com dados inconsistentes, retorna uma lista de dicionários
        notifications = _get_data_from_sheet(sheet)
        
        # Filtra apenas as notificações válidas antes de ordenar
        valid_notifications = [n for n in notifications if n.get('ID') and n.get('Data') and n.get('Status')]

        unread = sorted([n for n in valid_notifications if n.get('Status') == 'Não Lida'], key=lambda x: datetime.strptime(x.get('Data'), '%Y-%m-%d %H:%M:%S') if isinstance(x.get('Data'), str) else datetime.min, reverse=True)
        read = sorted([n for n in valid_notifications if n.get('Status') == 'Lida'], key=lambda x: datetime.strptime(x.get('Data'), '%Y-%m-%d %H:%M:%S') if isinstance(x.get('Data'), str) else datetime.min, reverse=True)
        
        return {"unread": unread, "read": read}
        
    except Exception as e:
        print(f"Erro ao buscar notificações: {e}"); traceback.print_exc()
        return {"unread": [], "read": []}

def mark_notifications_as_read(notification_ids):
    try:
        sheet = _get_sheet('Notificações')
        if not sheet:
            return {"success": False, "message": "Conexão com a planilha de Notificações falhou."}

        # Obtém todos os dados da planilha
        notifications = sheet.get_all_values()
        header = notifications[0]
        data_rows = notifications[1:]
        
        id_col_index = header.index('ID')
        status_col_index = header.index('Status')

        updates = []
        for i, row in enumerate(data_rows):
            if row[id_col_index] in notification_ids and row[status_col_index] == 'Não Lida':
                cell_range = f'E{i + 2}' # i+2 porque a contagem de linhas do gspread começa em 1, e pulamos o cabeçalho
                updates.append({'range': cell_range, 'values': [['Lida']]})
        
        if updates:
            sheet.batch_update(updates)
            return {"success": True, "message": "Notificações marcadas como lidas."}
        else:
            return {"success": True, "message": "Nenhuma notificação para marcar como lida."}

    except Exception as e:
        print(f"Erro ao marcar notificações como lidas: {e}"); traceback.print_exc()
        return {"success": False, "message": "Erro ao marcar notificações como lidas."}
