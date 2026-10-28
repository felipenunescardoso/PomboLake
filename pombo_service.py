# -*- coding: utf-8 -*-
"""
pombo_service.py
Envolve o pipeline do Pombo (main_servico.py) e grava stdout/stderr
em <projeto>\logs\pombo_YYYYMMDD.log (cria a pasta se nao existir).

Uso manual:   python pombo_service.py
Servico:      buildar com PyInstaller e registrar via NSSM
"""

import io
import sys
import threading
from pathlib import Path
from datetime import datetime

# ===================== CONFIG =====================
PROJETO    = Path(r"C:\Users\svc-qlikv\PycharmProjects\WelcomeScreen\Pombo")
LOG_DIR    = PROJETO / "logs"
NOME_LOG   = "pombo"
RETENCAO_DIAS = 30
# ==================================================


class ArquivoLogDiario:
    """Arquivo de log que gira por dia (pombo_YYYYMMDD.log)."""

    def __init__(self, diretorio: Path, prefixo: str):
        self.diretorio = diretorio
        self.prefixo = prefixo
        self._data = None
        self._arq = None
        self._lock = threading.Lock()

    def _garantir(self):
        hoje = datetime.now().strftime("%Y%m%d")
        if self._data != hoje:
            if self._arq:
                self._arq.close()
            self.diretorio.mkdir(parents=True, exist_ok=True)  # cria se nao existir
            self._arq = open(
                self.diretorio / f"{self.prefixo}_{hoje}.log",
                "a", encoding="utf-8", buffering=1,
            )
            self._data = hoje

    def write(self, s: str) -> int:
        with self._lock:
            self._garantir()
            self._arq.write(s)
            self._arq.flush()
        return len(s)

    def flush(self):
        if self._arq:
            self._arq.flush()


class ComTimestamp(io.TextIOBase):
    """Prefixa cada linha com data/hora."""

    def __init__(self, destino):
        self.destino = destino
        self._buf = ""

    def write(self, data: str) -> int:
        self._buf += data
        while "\n" in self._buf:
            linha, self._buf = self._buf.split("\n", 1)
            if linha.strip():
                ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                self.destino.write(f"{ts} | {linha}\n")
        return len(data)

    def flush(self):
        self.destino.flush()


class Duplex(io.TextIOBase):
    """Espelha tudo para dois destinos (console + arquivo)."""

    def __init__(self, a, b):
        self.a, self.b = a, b

    def write(self, s: str) -> int:
        self.a.write(s)
        self.b.write(s)
        return len(s)

    def flush(self):
        self.a.flush()
        self.b.flush()


def limpar_logs_antigos():
    try:
        corte = datetime.now().timestamp() - RETENCAO_DIAS * 86400
        for arq in LOG_DIR.glob(f"{NOME_LOG}_*.log"):
            if arq.stat().st_mtime < corte:
                arq.unlink()
    except Exception:
        pass  # nunca derruba o pipeline por causa de limpeza


def configurar_logs():
    arquivo = ArquivoLogDiario(LOG_DIR, NOME_LOG)

    sys.stdout = Duplex(sys.__stdout__, ComTimestamp(arquivo))
    sys.stderr = Duplex(sys.__stderr__, ComTimestamp(arquivo))


def main():
    configurar_logs()
    limpar_logs_antigos()

    # Garante que o projeto esteja no sys.path,
    # independente de onde o script/exe foi chamado
    if str(PROJETO) not in sys.path:
        sys.path.insert(0, str(PROJETO))

    import main_servico

    main_servico.main()


if __name__ == "__main__":
    main()