# services/price_tracker_service.py

import requests
from bs4 import BeautifulSoup
import time
import urllib.parse
from fuzzywuzzy import fuzz
import traceback

# Helper para converter "R$ X.XX" ou "R$ X,XX" para float e arredondar
def price_to_float(price_str: str) -> int | None:
    if not isinstance(price_str, str) or "indisponível" in price_str.lower() or "gratuito" in price_str.lower():
        return None
    price_str = price_str.replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return round(float(price_str))
    except ValueError:
        return None

# Helper para converter float para "R$ X"
def float_to_price_str(price_value: int | None) -> str:
    return f"R$ {price_value}" if price_value is not None else "Preço indisponível"

# --- SteamScraper ---
class SteamScraper:
    BASE_URL = "https://store.steampowered.com/search/"
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 2

    def search_game_price(self, game_name: str, num_results: int = 3) -> list:
        print(f"STEAM: Buscando por '{game_name}'...")
        params = {'term': game_name, 'l': 'brazilian', 'cc': 'br'}
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        cookies = {'birthtime': '315532800', 'wants_mature_content': '1'}

        response = None
        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.get(self.BASE_URL, params=params, headers=headers, cookies=cookies, timeout=15)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                print(f"STEAM: Erro na tentativa {attempt + 1} para '{game_name}': {e}")
                time.sleep(self.RETRY_DELAY_SECONDS * (attempt + 1))
        else:
            return [self._format_error("Número máximo de tentativas excedido.")]

        soup = BeautifulSoup(response.text, 'html.parser')
        search_results = soup.select("#search_resultsRows a")
        
        if not search_results:
            return [self._format_error("Nenhum jogo encontrado.")]
            
        parsed_results = []
        for result in search_results[:num_results]:
            title_element = result.select_one("span.title")
            title = title_element.text.strip() if title_element else "Título indisponível"
            
            url = result['href'] if 'href' in result.attrs else None

            final_price = "Preço indisponível"
            original_price = None

            discount_block = result.select_one(".search_price.discounted")
            if discount_block:
                final_price_element = result.select_one(".discount_final_price")
                final_price = self._parse_price(final_price_element.text.strip()) if final_price_element else "Preço indisponível"
                original_price_element = result.select_one("span strike")
                original_price = self._parse_price(original_price_element.text.strip()) if original_price_element else None
            else:
                regular_price_element = result.select_one(".search_price")
                if regular_price_element:
                    final_price = self._parse_price(regular_price_element.text.strip())
            
            parsed_results.append({
                "found": True, "title": title, "final_price": final_price, 
                "original_price": original_price, "url": url, "platform": "Steam"
            })
            
        return parsed_results

    def _parse_price(self, price_str: str) -> str:
        if "gratuito" in price_str.lower(): return "Gratuito"
        try:
            price_value = price_to_float(price_str)
            return float_to_price_str(price_value)
        except (ValueError, TypeError):
            return price_str

    def _format_error(self, message: str) -> dict:
        return {"found": False, "title": None, "final_price": message, "original_price": None, "url": None, "platform": "Steam"}

# --- PsnScraper ---
class PsnScraper:
    BASE_SEARCH_URL = "https://store.playstation.com/pt-br/search/"
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 2

    def search_game_price(self, game_name: str, num_results: int = 3) -> list:
        print(f"PSN: Buscando por '{game_name}'...")
        encoded_game_name = urllib.parse.quote(game_name, safe='')
        search_url = f"{self.BASE_SEARCH_URL}{encoded_game_name}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

        response = None
        for attempt in range(self.MAX_RETRIES):
            try:
                response = requests.get(search_url, headers=headers, timeout=15)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                print(f"PSN: Erro na tentativa {attempt + 1} para '{game_name}': {e}")
                time.sleep(self.RETRY_DELAY_SECONDS * (attempt + 1))
        else:
            return [self._format_error("Número máximo de tentativas excedido.")]

        soup = BeautifulSoup(response.text, 'html.parser')
        search_results = soup.select("div.psw-l-w-2\\/3.psw-l-w-full\\@mobile a.psw-link")

        if not search_results:
            return [self._format_error("Nenhum resultado de jogo encontrado.")]
            
        parsed_results = []
        for result_link in search_results[:num_results]:
            title_tag = result_link.select_one("span.psw-t-body.psw-c-font-weight-medium.psw-product-tile__product-title")
            title = title_tag.text.strip() if title_tag else "Título indisponível"

            final_price = "Preço indisponível"
            original_price = None

            price_container = result_link.select_one(".psw-product-tile__price")
            if price_container:
                final_price_span = price_container.select_one("span.psw-m-r-3")
                if final_price_span:
                    final_price = self._parse_price(final_price_span.text.strip())
                
                original_price_span = price_container.select_one("span.psw-text--line-through")
                if original_price_span:
                    original_price = self._parse_price(original_price_span.text.strip())

            if final_price == "Preço indisponível":
                free_game_span = result_link.select_one("span.psw-product-tile__badge-label")
                if free_game_span and "gratuito" in free_game_span.text.lower():
                    final_price = "Gratuito"

            url = result_link['href'] if 'href' in result_link.attrs else None
            if url and not url.startswith("https://store.playstation.com"):
                url = "https://store.playstation.com" + url

            parsed_results.append({
                "found": True, "title": title, "final_price": final_price, 
                "original_price": original_price, "url": url, "platform": "PSN"
            })
            
        return parsed_results

    def _parse_price(self, price_str: str) -> str:
        if not price_str or "gratuito" in price_str.lower(): return "Gratuito"
        try:
            price_value = price_to_float(price_str)
            return float_to_price_str(price_value)
        except (ValueError, TypeError):
            return price_str

    def _format_error(self, message: str) -> dict:
        return {"found": False, "title": None, "final_price": message, "original_price": None, "url": None, "platform": "PSN"}

# --- GamePriceAggregator ---
class GamePriceAggregator:
    IGNORE_KEYWORDS = [
        'moeda', 'pacote de', 'stubs', 'créditos', 'coins', 'points', 'vc', 'bundle',
        'dlc', 'passe de temporada', 'season pass', 'expansão', 'upgrade', 'demo', 'beta',
        'soundtrack', 'artbook', 'trilha sonora'
    ]
    SIMILARITY_THRESHOLD = 85 # Aumentado para mais precisão

    def __init__(self):
        self.steam_scraper = SteamScraper()
        self.psn_scraper = PsnScraper()

    def _is_relevant(self, title: str, original_game_name: str) -> bool:
        if not title: return False
        
        lower_title = title.lower()
        # Verifica se contém palavras-chave a serem ignoradas
        if any(keyword in lower_title for keyword in self.IGNORE_KEYWORDS):
            return False
        
        # Verifica se o título contém "edição" mas o nome original não
        if 'edição' in lower_title and 'edição' not in original_game_name.lower():
            # Permite edições como "Game of the Year Edition" se for similar o suficiente
            similarity = fuzz.token_sort_ratio(original_game_name.lower(), lower_title)
            if similarity < self.SIMILARITY_THRESHOLD:
                return False
        
        return True

    def get_best_match_for_game(self, game_name: str) -> dict:
        """
        Busca em ambas as plataformas e retorna o melhor resultado para cada uma.
        """
        print(f"\n----- Buscando por '{game_name}' em todas as plataformas -----")
        steam_results = self.steam_scraper.search_game_price(game_name, 5)
        psn_results = self.psn_scraper.search_game_price(game_name, 5)
        
        all_results = steam_results + psn_results
        
        best_steam_match = None
        best_psn_match = None
        
        highest_steam_similarity = 0
        highest_psn_similarity = 0

        for result in all_results:
            if not result.get('found') or not self._is_relevant(result.get('title'), game_name):
                continue

            similarity = fuzz.token_sort_ratio(game_name.lower(), result['title'].lower())
            
            if result['platform'] == 'Steam' and similarity > highest_steam_similarity:
                highest_steam_similarity = similarity
                best_steam_match = result
            
            if result['platform'] == 'PSN' and similarity > highest_psn_similarity:
                highest_psn_similarity = similarity
                best_psn_match = result
        
        # Monta o dicionário de retorno
        final_results = {}
        if best_steam_match and highest_steam_similarity >= self.SIMILARITY_THRESHOLD:
            final_results['Steam'] = best_steam_match
        
        if best_psn_match and highest_psn_similarity >= self.SIMILARITY_THRESHOLD:
            final_results['PSN'] = best_psn_match
            
        return final_results
