# price_scraper_service.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
from fuzzywuzzy import fuzz
import re
import gspread
import os
import json
from oauth2client.service_account import ServiceAccountCredentials
import traceback
import math

# --- Configuração Global do Scraper ---
SIMILARITY_THRESHOLD = 70

class PriceTrackerConfig:
    """
    Configurações para o serviço de rastreamento de preços.
    As credenciais e URL da planilha devem ser configuradas como variáveis de ambiente.
    """
    GOOGLE_SHEETS_CREDENTIALS_JSON = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')
    GOOGLE_SHEET_URL = os.environ.get('GOOGLE_SHEET_URL')

    if not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("ERRO CRÍTICO (PriceTrackerConfig): 'GSPREAD_SERVICE_ACCOUNT_CREDENTIALS' não configurado!")
    if not GOOGLE_SHEET_URL:
        print("ERRO CRÍTICO (PriceTrackerConfig): 'GOOGLE_SHEET_URL' não configurado!")


# Cache global para planilhas e dados para evitar leituras repetidas e lentas
_sheet_cache = {}
_data_cache = {}
_cache_ttl_seconds = 300 # Tempo de vida do cache em segundos (5 minutos)
_last_cache_update = {}

def _col_to_char(col_num: int) -> str:
    """
    Converte um número de coluna (base 1) para sua representação em letra (A, B, AA, etc.).
    """
    string = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        string = chr(65 + remainder) + string
    return string

def _get_sheet_for_price_tracker(sheet_name):
    """
    Retorna o objeto da planilha (worksheet) para o Price Tracker, usando cache.
    Autentica com as credenciais da conta de serviço.
    """
    global _sheet_cache
    if sheet_name in _sheet_cache:
        return _sheet_cache[sheet_name]
    
    try:
        credentials_json = PriceTrackerConfig.GOOGLE_SHEETS_CREDENTIALS_JSON
        if not credentials_json:
            print("ERRO CRÍTICO (PriceTracker): Variável de ambiente 'GSPREAD_SERVICE_ACCOUNT_CREDENTIALS' não configurada.")
            return None
        
        google_sheet_url = PriceTrackerConfig.GOOGLE_SHEET_URL
        if not google_sheet_url:
            print("ERRO CRÍTICO (PriceTracker): Variável de ambiente 'GOOGLE_SHEET_URL' não configurada.")
            return None

        print(f"DEBUG (PriceTracker): URL da planilha Google sendo usada: {google_sheet_url}")

        creds_dict = json.loads(credentials_json)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open_by_url(google_sheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        _sheet_cache[sheet_name] = worksheet
        
        print(f"DEBUG (PriceTracker): Planilha aberta com sucesso pela URL e worksheet '{sheet_name}'.")
        return worksheet
    except Exception as e:
        print(f"Erro ao autenticar ou abrir planilha '{sheet_name}' no Price Tracker: {e}"); traceback.print_exc()
        return None

def _get_data_from_sheet_for_price_tracker(sheet_name):
    """Retorna os dados da planilha para o Price Tracker, usando cache com TTL."""
    global _data_cache, _last_cache_update
    current_time = datetime.now()
    if sheet_name in _data_cache and \
       (current_time - _last_cache_update.get(sheet_name, datetime.min)).total_seconds() < _cache_ttl_seconds:
        print(f"Dados da planilha '{sheet_name}' servidos do cache no Price Tracker.")
        return _data_cache[sheet_name]

    sheet = _get_sheet_for_price_tracker(sheet_name)
    if not sheet:
        return []

    try:
        data = sheet.get_all_records()
        _data_cache[sheet_name] = data
        _last_cache_update[sheet_name] = current_time
        print(f"Dados da planilha '{sheet_name}' atualizados do Google Sheets e armazenados em cache no Price Tracker.")
        return data
    except gspread.exceptions.APIError as e:
        if "unable to parse range" in str(e): 
            print(f"AVISO (PriceTracker): Planilha '{sheet_name}' vazia ou com erro de range, retornando lista vazia. Detalhes: {e}")
            return []
        print(f"Erro ao ler dados da planilha '{sheet_name}' no Price Tracker: {e}"); traceback.print_exc()
        return []
    except Exception as e:
        print(f"Erro genérico ao ler dados da planilha '{sheet_name}' no Price Tracker: {e}"); traceback.print_exc()
        return []

def _invalidate_cache(sheet_name):
    """Invalida o cache para uma planilha específica."""
    global _data_cache
    if sheet_name in _data_cache:
        del _data_cache[sheet_name]
        print(f"Cache para a planilha '{sheet_name}' invalidado.")

def clean_price_to_float(price_str: str) -> float:
    """
    Converte uma string de preço (ex: "R$ 199,90", "Gratuito") para um float.
    Retorna float('inf') para preços indisponíveis ou inválidos, e 0.0 para "Gratuito".
    Os preços numéricos são arredondados para o inteiro mais próximo (para cima).
    """
    if not isinstance(price_str, str):
        return float('inf')

    price_str_lower = price_str.lower().strip()
    if "gratuito" in price_str_lower or "free" in price_str_lower or "grátis" in price_str_lower:
        return 0.0
    if "não encontrado" in price_str_lower or "preço indisponível" in price_str_lower:
        return float('inf')

    cleaned_price = price_str.replace("r$", "").replace(".", "").replace(",", ".").strip()
    try:
        match = re.search(r'\d[\d\.]*', cleaned_price)
        if match:
            return math.ceil(float(match.group(0)))
        return float('inf')
    except ValueError:
        return float('inf')

def format_float_to_price_str(price_float: float) -> str:
    """
    Converte um float de preço de volta para uma string formatada (ex: "400").
    Retorna "Não encontrado" se o preço for float('inf').
    Retorna "0" para jogos gratuitos.
    """
    if price_float == 0.0:
        return "0"
    if price_float == float('inf'):
        return "Não encontrado"
    return str(int(price_float))

def _clean_game_title(title: str) -> str:
    """
    Remove plataforma, edição e outros sufixos comuns de um título de jogo
    para melhorar a correspondência fuzzy.
    """
    clean_title = title.lower()
    keywords_to_remove = [
        r'\bps4\b', r'\bps5\b', r'\bplaystation\b', r'\bdeluxe edition\b',
        r'\bspecial edition\b', r'\bstandard edition\b', r'\bultimate edition\b',
        r'\bremastered\b', r'\bgoty\b', r'\bgame of the year\b', r'\bedition\b',
        r'™', r'®'
    ]
    for keyword in keywords_to_remove:
        clean_title = re.sub(keyword, '', clean_title)
    clean_title = re.sub(r'\(.*?\)', '', clean_title)
    clean_title = re.sub(r'\[.*?\]', '', clean_title)
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()
    return clean_title

class SteamScraper:
    """
    Scraper para buscar informações de jogos e preços na Steam.
    """
    BASE_URL = "https://store.steampowered.com/search/"

    def search_game_price(self, game_name: str) -> dict:
        print(f"STEAM: Buscando por '{game_name}'...")
        params = {'term': game_name, 'cc': 'br'}
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        cookies = {'birthtime': '86400', 'wants_mature_content': '1', 'mature_content': '1'}

        try:
            response = requests.get(self.BASE_URL, params=params, headers=headers, cookies=cookies, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"ERRO STEAM: Falha de comunicação para '{game_name}': {e}")
            return self._format_error("Não encontrado.")

        soup = BeautifulSoup(response.text, 'html.parser')
        search_results = soup.select("#search_resultsRows a")[:5]

        best_match_element = None
        highest_score = 0
        
        if not search_results:
            return self._format_error("Não encontrado.")

        cleaned_game_name = _clean_game_title(game_name)

        for result_element in search_results:
            title_element = result_element.select_one("span.title")
            if title_element:
                result_title = title_element.text.strip()
                cleaned_result_title = _clean_game_title(result_title)
                score = fuzz.ratio(cleaned_game_name, cleaned_result_title)
                
                if score > highest_score:
                    highest_score = score
                    best_match_element = result_element
        
        if not best_match_element or highest_score < SIMILARITY_THRESHOLD:
            return self._format_error(f"Não encontrado (semelhança: {highest_score}%).")

        title = best_match_element.select_one("span.title").text.strip()
        game_url = best_match_element['href']

        final_price_str = "Não encontrado"
        discount_price_element = best_match_element.select_one(".search_price.discounted, .discount_final_price")
        if discount_price_element:
            price_text = discount_price_element.text.strip()
            if "gratuito" in price_text.lower() or "free" in price_text.lower() or "grátis" in price_text.lower():
                final_price_str = "Gratuito"
            else:
                price_text_value = price_text.split("R$")[-1].strip()
                final_price_str = f"R$ {price_text_value}" if price_text_value else "Não encontrado"
        else:
            regular_price_element = best_match_element.select_one(".search_price")
            if regular_price_element:
                price_text = regular_price_element.text.strip()
                if "gratuito" in price_text.lower() or "free" in price_text.lower() or "grátis" in price_text.lower():
                    final_price_str = "Gratuito"
                else:
                    price_text_value = price_text.split("R$")[-1].strip()
                    final_price_str = f"R$ {price_text_value}" if price_text_value else "Não encontrado"
            else:
                final_price_str = "Não encontrado"
            
        return {
            "found": True,
            "title": title,
            "price_str": final_price_str,
            "price_float": clean_price_to_float(final_price_str),
            "url": game_url,
            "similarity_score": highest_score
        }

    def _format_error(self, message: str) -> dict:
        return {
            "found": False,
            "title": None,
            "price_str": "Não encontrado",
            "price_float": float('inf'),
            "url": None,
            "similarity_score": 0
        }

class PsnScraper:
    """
    Scraper para buscar informações de jogos e preços na PlayStation Store.
    """
    BASE_URL = "https://store.playstation.com/pt-br/search/"

    def search_game_price(self, game_name: str) -> dict:
        print(f"PSN: Buscando por '{game_name}'...")
        formatted_game_name = game_name.replace(' ', '%20')
        search_url = f"{self.BASE_URL}{formatted_game_name}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            response = requests.get(search_url, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"ERRO PSN: Falha de comunicação para '{game_name}': {e}")
            return self._format_error("Não encontrado.")

        soup = BeautifulSoup(response.content, 'html.parser')

        all_product_tiles = soup.find_all('div', class_='psw-product-tile')[:5]
        
        best_match_tile = None
        highest_score = 0
        game_url = search_url

        cleaned_game_name = _clean_game_title(game_name)

        page_title_tag = soup.find('h1', class_='psw-m-t-2xs psw-t-title-l psw-l-line-break-m') or \
                         soup.find('h1', class_='psw-p-t-xs')
        if page_title_tag:
             page_title = page_title_tag.text.strip()
             cleaned_page_title = _clean_game_title(page_title)
             score = fuzz.ratio(cleaned_game_name, cleaned_page_title)
             if score >= SIMILARITY_THRESHOLD:
                best_match_tile = soup 
                highest_score = score
        
        if all_product_tiles:
            for tile in all_product_tiles:
                title_tag = tile.find('span', class_='psw-t-body') or tile.find('span', class_='psw-h5')
                if title_tag:
                    result_title = title_tag.text.strip()
                    cleaned_result_title = _clean_game_title(result_title)
                    score = fuzz.ratio(cleaned_game_name, cleaned_result_title)
                    
                    if score > highest_score:
                        highest_score = score
                        best_match_tile = tile
                        link_tag = tile.find('a', class_='psw-top-left psw-bottom-right psw-stretched-link')
                        if link_tag and 'href' in link_tag.attrs:
                            game_url = "https://store.playstation.com" + link_tag['href']
                        elif tile.name == 'a' and 'href' in tile.attrs:
                            game_url = "https://store.playstation.com" + tile['href']

        if not best_match_tile or highest_score < SIMILARITY_THRESHOLD:
            return self._format_error(f"Não encontrado (semelhança: {highest_score}%).")

        title = 'Nome não encontrado'
        price_str = 'Não encontrado'
        
        if best_match_tile == soup:
            temp_title_tag = soup.find('h1', class_='psw-m-t-2xs psw-t-title-l psw-l-line-break-m') or \
                             soup.find('h1', class_='psw-p-t-xs')
            if temp_title_tag:
                title = temp_title_tag.text.strip()
            temp_price_element = soup.find('span', class_='psw-m-r-3') or \
                                 soup.find('span', class_='psw-l-line-through') or \
                                 soup.find('span', class_='psw-h5')
            if temp_price_element:
                price_str_raw = temp_price_element.text.strip()
                if "gratuito" in price_str_raw.lower() or "free" in price_str_raw.lower() or "grátis" in price_str_raw.lower():
                    price_str = "Gratuito"
                else:
                    price_str = price_str_raw
        else:
            title_tag = best_match_tile.find('span', class_='psw-t-body') or best_match_tile.find('span', class_='psw-h5')
            if title_tag:
                title = title_tag.text.strip()

            price_element = best_match_tile.find('span', class_='psw-m-r-3')
            if not price_element:
                price_element = best_match_tile.find('span', class_='psw-l-line-through')
            if not price_element:
                price_element = best_match_tile.find('span', class_='psw-h5')
            if price_element:
                price_str_raw = price_element.text.strip()
                if "gratuito" in price_str_raw.lower() or "free" in price_str_raw.lower() or "grátis" in price_str_raw.lower():
                    price_str = "Gratuito"
                else:
                    price_str = price_str_raw
        
        return {
            "found": True,
            "title": title,
            "price_str": price_str,
            "price_float": clean_price_to_float(price_str),
            "url": game_url,
            "similarity_score": highest_score
        }

    def _format_error(self, message: str) -> dict:
        return {
            "found": False,
            "title": None,
            "price_str": "Não encontrado",
            "price_float": float('inf'),
            "url": None,
            "similarity_score": 0
        }

def run_scraper(worksheet_name: str = 'Desejos'):
    """
    Função principal que orquestra a leitura da planilha do Google Sheets,
    o scraping e a atualização.
    """
    steam_scraper = SteamScraper()
    psn_scraper = PsnScraper()
    current_date = datetime.now().strftime('%Y-%m-%d')

    try:
        data = _get_data_from_sheet_for_price_tracker(worksheet_name)
        if not data:
            return {"status": "error", "message": f"Não foi possível carregar dados da planilha '{worksheet_name}'."}

        df = pd.DataFrame(data)

        if 'Nome' not in df.columns:
            return {"status": "error", "message": f"A planilha '{worksheet_name}' não possui a coluna 'Nome'."}

        target_gsheet_columns = [
            'Steam Preco Atual',
            'Steam Menor Preco Historico',
            'PSN Preco Atual',
            'PSN Menor Preco Historico',
            'Ultima Atualizacao'
        ]
        
        for col in target_gsheet_columns:
            if col not in df.columns:
                df[col] = 'Não encontrado'

        gsheet_worksheet = _get_sheet_for_price_tracker(worksheet_name)
        if not gsheet_worksheet:
            return {"status": "error", "message": f"Não foi possível obter o objeto da planilha para {worksheet_name}."}

        gsheet_headers = gsheet_worksheet.row_values(1)
        col_indices = {}
        for col_name in target_gsheet_columns:
            if col_name not in gsheet_headers:
                print(f"Adicionando coluna '{col_name}' à planilha do Google Sheets.")
                gsheet_headers.append(col_name)
                gsheet_worksheet.update_cell(1, len(gsheet_headers), col_name)
            col_indices[col_name] = gsheet_headers.index(col_name) + 1

        for index, row in df.iterrows():
            game_name = row['Nome']
            if pd.isna(game_name) or str(game_name).strip() == '':
                print(f"\nPulando linha {index + 2}: Nome do jogo vazio.")
                continue

            print(f"\nProcessando jogo: {game_name}")

            # --- Busca na Steam ---
            steam_result = steam_scraper.search_game_price(game_name)
            df.at[index, 'Steam Preco Atual'] = format_float_to_price_str(steam_result['price_float'])
            
            current_steam_price_float = steam_result['price_float']
            historical_steam_price_str = df.at[index, 'Steam Menor Preco Historico']
            historical_steam_price_float = clean_price_to_float(historical_steam_price_str)

            if current_steam_price_float < historical_steam_price_float:
                df.at[index, 'Steam Menor Preco Historico'] = format_float_to_price_str(steam_result['price_float'])
                print(f"  STEAM: Novo menor preço histórico para '{game_name}': {format_float_to_price_str(steam_result['price_float'])} (Semelhança: {steam_result['similarity_score']}%)")
            elif historical_steam_price_float == float('inf') and steam_result['found']:
                 df.at[index, 'Steam Menor Preco Historico'] = format_float_to_price_str(steam_result['price_float'])
                 print(f"  STEAM: Primeiro preço registrado para '{game_name}': {format_float_to_price_str(steam_result['price_float'])} (Semelhança: {steam_result['similarity_score']}%)")
            else:
                 print(f"  STEAM: Preço atual para '{game_name}': {format_float_to_price_str(steam_result['price_float'])} (Semelhança: {steam_result['similarity_score']}%)")


            # --- Busca na PSN ---
            psn_result = psn_scraper.search_game_price(game_name)
            df.at[index, 'PSN Preco Atual'] = format_float_to_price_str(psn_result['price_float'])

            current_psn_price_float = psn_result['price_float']
            historical_psn_price_str = df.at[index, 'PSN Menor Preco Historico']
            historical_psn_price_float = clean_price_to_float(historical_psn_price_str)

            if current_psn_price_float < historical_psn_price_float:
                df.at[index, 'PSN Menor Preco Historico'] = format_float_to_price_str(psn_result['price_float'])
                print(f"  PSN: Novo menor preço histórico para '{game_name}': {format_float_to_price_str(psn_result['price_float'])} (Semelhança: {psn_result['similarity_score']}%)")
            elif historical_psn_price_float == float('inf') and psn_result['found']:
                 df.at[index, 'PSN Menor Preco Historico'] = format_float_to_price_str(psn_result['price_float'])
                 print(f"  PSN: Primeiro preço registrado para '{game_name}': {format_float_to_price_str(psn_result['price_float'])} (Semelhança: {psn_result['similarity_score']}%)")
            else:
                 print(f"  PSN: Preço atual para '{game_name}': {format_float_to_price_str(psn_result['price_float'])} (Semelhança: {psn_result['similarity_score']}%)")
            
            df.at[index, 'Ultima Atualizacao'] = current_date

            time.sleep(0.5) # Pequeno atraso para evitar sobrecarregar os servidores

        # --- Atualiza o Google Sheet ---
        start_row = 2
        updates = []
        for r_idx, row_df in df.iterrows():
            row_data = []
            for col_name in target_gsheet_columns:
                row_data.append(row_df[col_name])
            updates.append(row_data)

        start_col_letter = _col_to_char(col_indices[target_gsheet_columns[0]])
        end_col_letter = _col_to_char(col_indices[target_gsheet_columns[-1]])
        end_row = start_row + len(df) - 1

        range_to_update = f"{start_col_letter}{start_row}:{end_col_letter}{end_row}"
        
        print(f"\nAtualizando Google Sheet no range: {range_to_update}")
        gsheet_worksheet.update(values=updates, range_name=range_to_update)

        _invalidate_cache(worksheet_name) # Invalida o cache após a atualização

        print(f"\nPlanilha do Google Sheets '{worksheet_name}' atualizada com sucesso!")
        return {"status": "success", "message": f"Preços da planilha '{worksheet_name}' atualizados com sucesso!"}

    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a execução do script: {e}")
        traceback.print_exc()
        return {"status": "error", "message": f"Erro interno ao executar o scraper: {str(e)}"}

if __name__ == "__main__":
    # Este bloco é para testar o serviço de forma independente
    # Certifique-se de que as variáveis de ambiente estão definidas para o teste
    # Ex: export GOOGLE_SHEET_URL="SUA_URL"
    # Ex: export GSPREAD_SERVICE_ACCOUNT_CREDENTIALS='{"type": "service_account", ...}'
    result = run_scraper(worksheet_name='Desejos')
    print(result)
