import os
from werkzeug.security import generate_password_hash

class Config:
    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'seu-segredo-de-desenvolvimento')

    # A variável de ambiente GOOGLE_SHEETS_CREDENTIALS será lida como uma string JSON
    GOOGLE_SHEETS_CREDENTIALS_JSON = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
    
    if not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("CRITICAL ERROR: GOOGLE_SHEETS_CREDENTIALS environment variable is not set!")

    # A URL da sua planilha.
    GAME_SHEET_URL = os.environ.get(
        'GAME_SHEET_URL',
        'https://docs.google.com/spreadsheets/d/1xWeZ0p6v_wgp4WquyEJNRxSl-sijD3G7vKFZH8NaXLc/edit?usp=sharing'
    )

    ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'admin')
    ADMIN_PASSWORD_HASH = os.environ.get('ADMIN_PASSWORD_HASH', generate_password_hash('suasenha_padrao'))

    RAWG_API_KEY = os.environ.get('RAWG_API_KEY')
    
    DEEPL_API_KEY = os.environ.get('DEEPL_API_KEY')

    # --- NOVAS VARIÁVEIS PARA ACIONAR A GITHUB ACTION ---
    GITHUB_PAT = os.environ.get('GITHUB_PAT') # Seu Personal Access Token do GitHub
    GITHUB_OWNER = os.environ.get('GITHUB_OWNER') # Seu nome de usuário do GitHub
    GITHUB_REPO = os.environ.get('GITHUB_REPO') # O nome do repositório do scraper
    GITHUB_WORKFLOW_FILE_NAME = os.environ.get('GITHUB_WORKFLOW_FILE_NAME') # Ex: scraper.yml
