import os
import time
from services.game_service import get_all_game_data

def run_worker():
    """
    Função principal do worker que roda a verificação em um loop.
    """
    print("Worker de verificação de datas de lançamento iniciado.")
    # Roda a verificação imediatamente
    get_all_game_data()

    # Loop principal que roda a cada 24 horas (86400 segundos)
    # Você pode ajustar este valor conforme a sua necessidade
    while True:
        time.sleep(86400) # 24 horas
        print("Executando verificação de datas de lançamento...")
        get_all_game_data()

if __name__ == '__main__':
    run_worker()
