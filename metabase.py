# -*- coding: utf-8 -*-
"""
metabase_service.py
Sobe o Metabase (java -jar) como processo filho e grava
stdout/stderr em D:\\Metabase\\logs\\metabase_YYYYMMDD.log

Uso manual:    python D:\\Metabase\\metabase_service.py
Uso servico:   registrado via NSSM (comandos no final)
"""

import os
import sys
import signal
import threading
import subprocess
from pathlib import Path
from datetime import datetime

# ===================== CONFIG =====================
JAVA_BIN = r"D:\Java\bin\java.exe"
JAR      = r"D:\Metabase\metabase.jar"
WORKDIR  = r"D:\Java\bin"

LOG_DIR      = Path(r"D:\Metabase\logs")
RETENCAO_DIAS = 30          # apaga logs mais velhos que isso

ENV = {
    "MB_DB_TYPE":   "postgres",
    "MB_DB_HOST":   "localhost",
    "MB_DB_PORT":   "54320",
    "MB_DB_DBNAME": "metabase_app",
    "MB_DB_USER":   "metabase",
    "MB_DB_PASS":   "Refit@2026",
}
# ==================================================


_lock     = threading.Lock()
_log_date = None
_log_file = None


def abrir_log():
    """Abre (ou gira) o arquivo de log do dia."""
    global _log_date, _log_file
    hoje = datetime.now().strftime("%Y%m%d")
    if _log_date != hoje:
        if _log_file:
            _log_file.close()
        LOG_DIR.mkdir(parents=True, exist_ok=True)   # cria a pasta se nao existir
        _log_file = open(
            LOG_DIR / f"metabase_{hoje}.log",
            "a", encoding="utf-8", buffering=1,     # line-buffered
        )
        _log_date = hoje
    return _log_file


def log(msg):
    linha = f"{datetime.now():%d/%m/%Y %H:%M:%S} | {msg}"
    with _lock:
        f = abrir_log()
        f.write(linha + "\n")
    print(linha, flush=True)   # tambem espelha no console


def ler_saida(pipe, prefixo):
    """Le stdout/stderr do Java linha a linha e grava no log."""
    for raw in iter(pipe.readline, b""):
        texto = raw.decode("utf-8", errors="replace").rstrip()
        log(f"{prefixo} | {texto}")


def limpar_logs_antigos():
    try:
        corte = datetime.now().timestamp() - (RETENCAO_DIAS * 86400)
        for arq in LOG_DIR.glob("metabase_*.log"):
            if arq.stat().st_mtime < corte:
                arq.unlink()
                log(f"Log antigo removido: {arq.name}")
    except Exception as e:
        log(f"Falha ao limpar logs antigos: {e}")


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    limpar_logs_antigos()

    env = os.environ.copy()
    env.update(ENV)

    log("=" * 60)
    log("Metabase Service iniciando")
    log(f"Jar : {JAR}")
    log(f"Cwd : {WORKDIR}")
    log(f"Destino do log: {LOG_DIR}")

    proc = subprocess.Popen(
        [JAVA_BIN, "-jar", JAR],
        cwd=WORKDIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    t_out = threading.Thread(target=ler_saida, args=(proc.stdout, "OUT"), daemon=True)
    t_err = threading.Thread(target=ler_saida, args=(proc.stderr, "ERR"), daemon=True)
    t_out.start()
    t_err.start()

    def parar(sig, frame):
        log(f"Sinal de parada recebido ({sig}). Encerrando Metabase...")
        proc.terminate()
        try:
            proc.wait(timeout=30)
            log("Metabase encerrado com grace.")
        except subprocess.TimeoutExpired:
            log("Metabase nao respondeu em 30s. Forcando kill.")
            proc.kill()
        sys.exit(0)

    signal.signal(signal.SIGTERM, parar)
    signal.signal(signal.SIGINT, parar)

    rc = proc.wait()          # fica aqui enquanto o Metabase vive
    log(f"Processo Java encerrado (codigo={rc}). Reiniciar via servico/NSSM.")
    sys.exit(rc)


if __name__ == "__main__":
    main()