import os
from werkzeug.security import generate_password_hash

class Config:
    JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'seu-segredo-de-desenvolvimento')

    # A variável de ambiente GOOGLE_SHEETS_CREDENTIALS será lida como uma string JSON
    GOOGLE_SHEETS_CREDENTIALS_JSON = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
    
    if not GOOGLE_SHEETS_CREDENTIALS_JSON:
        # Se a variável de ambiente não estiver definida, é um erro crítico em produção.
        # Caso esteja rodando localmente, você pode definir um caminho para um arquivo JSON
        # Mas para o Render, esta variável deve ser preenchida.
        print("CRITICAL ERROR: GOOGLE_SHEETS_CREDENTIALS environment variable is not set!")
        # Para evitar que o Render trave, vamos deixar o erro ser tratado mais abaixo.

    # A URL da sua planilha. Altere 'SUA_PLANILHA_ID'
    GAME_SHEET_URL = os.environ.get(
        'GAME_SHEET_URL',
        'https://docs.google.com/spreadsheets/d/1xWeZ0p6v_wgp4WquyEJNRxSl-sijD3G7vKFZH8NaXLc/edit?usp=sharing'
    )

    ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'admin')
    ADMIN_PASSWORD_HASH = os.environ.get('ADMIN_PASSWORD_HASH', generate_password_hash('suasenha_padrao'))

    RAWG_API_KEY = os.environ.get('RAWG_API_KEY')
