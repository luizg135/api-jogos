import os
from werkzeug.security import generate_password_hash

class Config:
    # Obtém a chave secreta do Render ou usa uma de fallback (NUNCA use o fallback em produção)
    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'seu-segredo-de-desenvolvimento')

    # Obtém o conteúdo do arquivo de credenciais da variável de ambiente do Render
    GOOGLE_SHEETS_CREDENTIALS_JSON = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
    if not GOOGLE_SHEETS_CREDENTIALS_JSON:
        raise ValueError("A variável de ambiente 'GOOGLE_SHEETS_CREDENTIALS' não está definida.")

    # A URL da sua planilha. Altere 'SUA_PLANILHA_ID'
    GAME_SHEET_URL = os.environ.get('GAME_SHEET_URL', 'https://docs.google.com/spreadsheets/d/SUA_PLANILHA_ID/edit?usp=sharing')

    # Credenciais de login (obtenha do Render)
    ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'admin')
    ADMIN_PASSWORD_HASH = os.environ.get('ADMIN_PASSWORD_HASH', generate_password_hash('suasenha_padrao'))
