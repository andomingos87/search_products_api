from pathlib import Path
import csv

# Configurações internas (ajuste conforme necessário)
INPUT_FILE = "produtos-copafer-2.csv"
DELIMITER = ";"
QUOTECHAR = '"'
HAS_HEADER = True  # se True, considera a primeira linha como cabeçalho


def count_csv_rows(csv_path: Path, delimiter: str = ";", quotechar: str = '"', has_header: bool = True) -> tuple[int, int, int]:
    """
    Conta registros lógicos de um CSV respeitando delimitador/aspas.

    Retorna: (total_linhas_arquivo, total_registros_dados, linhas_cabecalho)
    - total_linhas_arquivo: cabeçalho (se houver) + linhas de dados
    - total_registros_dados: somente linhas de dados (sem cabeçalho)
    - linhas_cabecalho: 1 se houver cabeçalho e existir, senão 0
    """
    total_data = 0
    header_lines = 0

    # newline='' para o csv lidar com quebras internas de linha
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
        # consumir cabeçalho se aplicável
        if has_header:
            try:
                next(reader)
                header_lines = 1
            except StopIteration:
                header_lines = 0
                return (0, 0, 0)
        for _ in reader:
            total_data += 1

    return (header_lines + total_data, total_data, header_lines)


if __name__ == "__main__":
    inp = Path(INPUT_FILE)
    if not inp.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {inp}")

    total, dados, cab = count_csv_rows(inp, delimiter=DELIMITER, quotechar=QUOTECHAR, has_header=HAS_HEADER)

    print(f"Arquivo: {inp}")
    if HAS_HEADER:
        print(f"Cabeçalho: {cab}")
    print(f"Registros (dados): {dados}")
    print(f"Total de linhas (arquivo): {total}")
