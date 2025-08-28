from pathlib import Path
import csv

# Configurações internas (ajuste conforme necessário)
INPUT_FILE = "produtos-copafer-2.csv"
COUNT = 200  # quantidade de registros (linhas lógicas) de dados após o cabeçalho
OUTPUT_FILE = "resumido_200.csv"  # se None, gera automaticamente

# Dialeto do CSV de entrada/saída
DELIMITER = ";"
QUOTECHAR = '"'
INCLUDE_HEADER = True  # sempre escreve o cabeçalho

inp = Path(INPUT_FILE)
out = Path(OUTPUT_FILE) if OUTPUT_FILE else inp.with_name(f"{inp.stem}-head{COUNT}{inp.suffix}")

# Abrir com newline='' para o csv lidar corretamente com quebras dentro de campos
with inp.open("r", encoding="utf-8", errors="ignore", newline='') as f_in, \
     out.open("w", encoding="utf-8", newline='') as f_out:
    reader = csv.reader(f_in, delimiter=DELIMITER, quotechar=QUOTECHAR)
    writer = csv.writer(f_out, delimiter=DELIMITER, quotechar=QUOTECHAR, quoting=csv.QUOTE_MINIMAL)

    # Cabeçalho
    try:
        header = next(reader)
    except StopIteration:
        header = None

    if INCLUDE_HEADER and header is not None:
        writer.writerow(header)

    # Escrever os primeiros COUNT registros de dados
    written = 0
    for row in reader:
        writer.writerow(row)
        written += 1
        if written >= COUNT:
            break

print(f"Criado: {out}")