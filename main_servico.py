from __future__ import annotations

import time
import traceback
from datetime import datetime

# Ordem: API (POSTOS) -> Sybase -> Refinador -> Orquestrador.
# O extrator API devolve codigo de saida (0 ok / 1 falha geral); falhas por
# entidade ja sao contidas dentro dele (nao abortam o pipeline).
from extrator_pombo_api import executar as executar_extrator_api
from Pombo_Extrator import executar as executar_extrator
from Pombo_Refinador import executar as executar_refinador
from Pombo_Orquestrador import executar as executar_orquestrador


INTERVALO_MINUTOS = 20
INTERVALO_SEGUNDOS = INTERVALO_MINUTOS * 60


def executar_pipeline() -> None:
    print("\n========================================")
    print(f"POMBO - INÍCIO DO CICLO: {datetime.now():%d/%m/%Y %H:%M:%S}")
    print("========================================\n")

    print("========================================")
    print("Executando Extrator API (POSTOS)")
    print("========================================\n")
    rc_api = executar_extrator_api()
    if rc_api != 0:
        print(f"[AVISO] Extrator API finalizou com código {rc_api}; seguindo o pipeline.")

    print("\n========================================")
    print("Executando Extrator (Sybase)")
    print("========================================\n")
    executar_extrator()

    print("\n========================================")
    print("Executando Refinador")
    print("========================================\n")
    executar_refinador()

    print("\n========================================")
    print("Executando Orquestrador")
    print("========================================\n")
    executar_orquestrador()

    print("\n========================================")
    print(f"POMBO - CICLO FINALIZADO: {datetime.now():%d/%m/%Y %H:%M:%S}")
    print("========================================\n")


def main() -> None:
    print("===========================================")
    print("       POMBO DATA LAKE - SERVIÇO")
    print("===========================================")
    print(f"Intervalo: {INTERVALO_MINUTOS} minutos")
    print("===========================================\n")

    while True:
        try:
            executar_pipeline()

        except KeyboardInterrupt:
            print("\nPombo Serviço encerrado pelo usuário.")
            break

        except Exception:
            print("\nERRO DURANTE O PIPELINE:\n")
            traceback.print_exc()

        print(
            f"\nPróxima execução em {INTERVALO_MINUTOS} minutos..."
        )

        try:
            time.sleep(INTERVALO_SEGUNDOS)

        except KeyboardInterrupt:
            print("\nPombo Serviço encerrado pelo usuário.")
            break


if __name__ == "__main__":
    main()