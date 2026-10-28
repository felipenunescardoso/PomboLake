from __future__ import annotations

import sys
import traceback

from Pombo_Extrator import executar as executar_extrator
from Pombo_Refinador import executar as executar_refinador
from Pombo_Orquestrador import executar as executar_orquestrador


def executar_pipeline_completo() -> None:
    print("\n========== PIPELINE COMPLETO ==========\n")

    try:
        print("========================================")
        print("Executando Extrator")
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

        print("\n========== PIPELINE FINALIZADO ==========\n")

    finally:
        pass


def menu() -> str:
    print(
        """
===========================================
            POMBO DATA LAKE
===========================================

1 - Executar Extrator

2 - Executar Refinador

3 - Executar Orquestrador

4 - Executar Pipeline Completo

0 - Sair

===========================================
"""
    )

    return input("Escolha uma opção: ").strip()


def main() -> None:
    while True:
        opcao = menu()

        try:
            if opcao == "1":
                executar_extrator()

            elif opcao == "2":
                executar_refinador()

            elif opcao == "3":
                executar_orquestrador()

            elif opcao == "4":
                executar_pipeline_completo()

            elif opcao == "0":
                print("\nEncerrando Pombo...")
                break

            else:
                print("\nOpção inválida.\n")

        except KeyboardInterrupt:
            print("\nExecução interrompida pelo usuário.\n")

        except Exception:
            print("\nErro durante a execução:\n")
            traceback.print_exc()

        input("\nPressione ENTER para continuar...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)