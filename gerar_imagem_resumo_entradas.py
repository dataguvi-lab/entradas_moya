from dotenv import load_dotenv
import fdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from datetime import datetime
import locale
from git import Repo
from matplotlib.offsetbox import OffsetImage, AnnotationBbox

load_dotenv()

# ============ CONFIGURAÇÃO DA CONEXÃO ============
FB_HOST = os.getenv("DT_HOST")
FB_DATABASE = os.getenv("DT_DATABASE")
FB_USER = os.getenv("DT_USER")
FB_PASSWORD = os.getenv("DT_PASSWORD")
FB_CHARSET = "WIN1252"

locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
mes_atual = datetime.now().strftime("%B").upper()
ano_atual = datetime.now().year

# ============ CONFIG DA IMAGEM ============
TITULO = f"ENTRADAS DE AGOSTO 2025"#{mes_atual} {ano_atual}"          # ajuste conforme seu filtro
ARQUIVO_SAIDA = "entradas_moya.png"  # caminho/arquivo de saída
FIGSIZE = (8, 3)                           # largura x altura (polegadas) – ajuste se quiser
FONTE_TABELA = 9                             # tamanho da fonte da tabela
logo_path = 'logo_moya.png'

# CORES
HEADER_BG = "#a51a19"
HEADER_FG = "white"
META_BG = "#1E3A8A"

# ============ SUA QUERY ============
SQL = """
WITH base AS (
    SELECT
        p.linha,
        TRIM(REPLACE(l.descricao, 'REMESSA RETORNO - ', '')) AS descricao,
        o.abertura AS dt,
        e.produto,
        o.filial,
        t.nome
    FROM osordem o
    JOIN cadastro t ON t.codigo = o.cadastro
    JOIN osequipamentos e ON e.equipamento = o.equipamento
    JOIN ceprodutos p ON p.produto = e.produto
    LEFT JOIN celinhas l ON p.linha = l.linha
    LEFT JOIN ossituacao s ON s.situacao = o.situacao
    LEFT JOIN vendedores v ON v.vendedor = o.vendedor
    LEFT JOIN vendedores v2 ON v2.vendedor = t.vendedortmk
    WHERE EXTRACT(YEAR FROM o.abertura) = EXTRACT(YEAR FROM CURRENT_DATE)
      AND EXTRACT(MONTH FROM o.abertura) = EXTRACT(MONTH FROM CURRENT_DATE)
      AND t.nome NOT LIKE '%JCC%'
      AND t.nome NOT LIKE 'LOG P%'
),
agg AS (
    SELECT
        linha,
        descricao,
        SUM(CASE WHEN EXTRACT(DAY FROM dt) BETWEEN 1 AND 7 THEN 1 ELSE 0 END) AS sem01,
        SUM(CASE WHEN EXTRACT(DAY FROM dt) BETWEEN 8 AND 14 THEN 1 ELSE 0 END) AS sem02,
        SUM(CASE WHEN EXTRACT(DAY FROM dt) BETWEEN 15 AND 21 THEN 1 ELSE 0 END) AS sem03,
        SUM(CASE WHEN EXTRACT(DAY FROM dt) BETWEEN 22 AND 28 THEN 1 ELSE 0 END) AS sem04,
        SUM(CASE WHEN EXTRACT(DAY FROM dt) >= 29 THEN 1 ELSE 0 END) AS sem05,
        COUNT(produto) AS total
    FROM base
    GROUP BY linha, descricao
)
SELECT * FROM (
    SELECT
        'ZZZZF' as linha,
        'Equip. de grande porte' as descricao,
        SUM(a.sem01) as sem01,
        SUM(a.sem02) as sem02,
        SUM(a.sem03) as sem03,
        SUM(a.sem04) as sem04,
        SUM(a.sem05) as sem05,
        SUM(a.total) as total,
        null as meta,
        null as perc
    FROM agg a
    WHERE linha IN ('RR0139','RR0140','RR0108','RR0131','RR0141','RR0137','RR0148','RR0117')
    UNION ALL
    SELECT
        'ZZZZB' as linha,
        'Motores Part / Alt.' as descricao,
        SUM(a.sem01) as sem01,
        SUM(a.sem02) as sem02,
        SUM(a.sem03) as sem03,
        SUM(a.sem04) as sem04,
        SUM(a.sem05) as sem05,
        SUM(a.total) as total,
        470 as meta,
        CASE WHEN 960 = 0 THEN NULL ELSE CAST((SUM(a.total) * 100.0) / 960 AS NUMERIC(9,2)) END AS perc
    FROM agg a
    WHERE linha IN ('RR0100', 'RR0103')
    UNION ALL
    SELECT
        'ZZZZA' as linha,
        'MOTOR EMP CC' as descricao,
        SUM(a.sem01) as sem01,
        SUM(a.sem02) as sem02,
        SUM(a.sem03) as sem03,
        SUM(a.sem04) as sem04,
        SUM(a.sem05) as sem05,
        SUM(a.total) as total,
        330 as meta,
        CASE WHEN 960 = 0 THEN NULL ELSE CAST((SUM(a.total) * 100.0) / 960 AS NUMERIC(9,2)) END AS perc
    FROM agg a
    WHERE linha IN ('RR0101')
    UNION ALL
    SELECT
        'ZZZZC' as linha,
        'MOTOR EMP CA' as descricao,
        SUM(a.sem01) as sem01,
        SUM(a.sem02) as sem02,
        SUM(a.sem03) as sem03,
        SUM(a.sem04) as sem04,
        SUM(a.sem05) as sem05,
        SUM(a.total) as total,
        160 as meta,
        CASE WHEN 960 = 0 THEN NULL ELSE CAST((SUM(a.total) * 100.0) / 960 AS NUMERIC(9,2)) END AS perc
    FROM agg a
    WHERE linha IN ('RR0102')
    UNION ALL
    SELECT
        'ZZZZD' as linha,
        'TRANSMISSÃO' as descricao,
        SUM(a.sem01) as sem01,
        SUM(a.sem02) as sem02,
        SUM(a.sem03) as sem03,
        SUM(a.sem04) as sem04,
        SUM(a.sem05) as sem05,
        SUM(a.total) as total,
        70 as meta,
        CASE WHEN 960 = 0 THEN NULL ELSE CAST((SUM(a.total) * 100.0) / 960 AS NUMERIC(9,2)) END AS perc
    FROM agg a
    WHERE linha IN ('RR0128')
    UNION ALL
    SELECT
        'ZZZZE' as linha,
        'POLIA DE FREIO' as descricao,
        SUM(a.sem01) as sem01,
        SUM(a.sem02) as sem02,
        SUM(a.sem03) as sem03,
        SUM(a.sem04) as sem04,
        SUM(a.sem05) as sem05,
        SUM(a.total) as total,
        150 as meta,
        CASE WHEN 960 = 0 THEN NULL ELSE CAST((SUM(a.total) * 100.0) / 960 AS NUMERIC(9,2)) END AS perc
    FROM agg a
    WHERE linha IN ('RR0115')
    UNION ALL
    SELECT
        'ZZZZG' as linha,
        'PLACAS ELETRÔNICAS' as descricao,
        SUM(a.sem01) as sem01,
        SUM(a.sem02) as sem02,
        SUM(a.sem03) as sem03,
        SUM(a.sem04) as sem04,
        SUM(a.sem05) as sem05,
        SUM(a.total) as total,
        161 as meta,
        CASE WHEN 960 = 0 THEN NULL ELSE CAST((SUM(a.total) * 100.0) / 960 AS NUMERIC(9,2)) END AS perc
    FROM agg a
    WHERE linha IN ('RR0105')
) resultado
ORDER BY linha, descricao;
"""

COL_WIDTHS = [0.372, 0.103, 0.100, 0.100, 0.100, 0.100, 0.098, 0.098, 0.098]

# ============ FUNÇÕES AUXILIARES ============
def _fmt_int(x):
    try:
        if pd.isna(x):
            return ""
        return f"{int(x)}"
    except Exception:
        return ""

def _fmt_pct(x):
    if x is None or (isinstance(x, float) and (np.isnan(x))):
        return ""
    return f"{round(float(x))}%"

def render_entradas_table(
    df: pd.DataFrame,
    title: str,
    outfile: str,
    figsize=(12, 6),
    header_bg=HEADER_BG,
    meta_bg=META_BG,
    header_fg=HEADER_FG,
    col_widths=COL_WIDTHS,
    font_size=FONTE_TABELA,
    logo_path=logo_path
):
    """
    Espera df com colunas:
    ['descricao','sem01','sem02','sem03','sem04','sem05','total','meta','perc']
    """
    cols = ["descricao","sem01","sem02","sem03","sem04","sem05","total","meta","perc"]
    df = df[cols].copy()

    # Linha de totais
    total_row = {
        "descricao": "",
        "sem01": df["sem01"].sum(),
        "sem02": df["sem02"].sum(),
        "sem03": df["sem03"].sum(),
        "sem04": df["sem04"].sum(),
        "sem05": df["sem05"].sum(),
        "total": df["total"].sum(),
        "meta": df["meta"].sum(),
        "perc": (df["total"].sum() / df["meta"].sum() * 100.0) if df["meta"].sum() else np.nan,
    }

    df_display = df.copy()
    df_display.loc[:, "descricao"] = "ENTRADA - " + df_display["descricao"]
    df_display = pd.concat([df_display, pd.DataFrame([total_row])], ignore_index=True)

    col_labels = ["", "SEM 01","SEM 02","SEM 03","SEM 04","SEM 05","TOTAL","META","%"]

    cell_text = []
    for _, r in df_display.iterrows():
        cell_text.append([
            r["descricao"],
            _fmt_int(r["sem01"]),
            _fmt_int(r["sem02"]),
            _fmt_int(r["sem03"]),
            _fmt_int(r["sem04"]),
            _fmt_int(r["sem05"]),
            _fmt_int(r["total"]),
            _fmt_int(r["meta"]),
            _fmt_pct(r["perc"]),
        ])

    n_rows = len(df_display) + 1  # header + corpo
    n_cols = len(col_labels)

    fig, ax = plt.subplots(figsize=figsize)
    ax.axis("off")

    table = ax.table(
        cellText=cell_text,
        colLabels=col_labels,
        cellLoc="center",
        colLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)
    table.scale(1, 1.3)

    # === Cabeçalho ===
    for j in range(n_cols):
        cell = table[0, j]
        cell.set_facecolor(header_bg)
        cell.get_text().set_color(header_fg)
        cell.set_edgecolor("black")
        cell.set_linewidth(1.0)

    # === Borda/cores do corpo + lógica da coluna % ===
    last_row_idx = len(df_display) - 1
    for i in range(1, n_rows):
        for j in range(n_cols):
            cell = table[i, j]
            cell.set_edgecolor("black")
            cell.set_linewidth(1.0)

            # Coluna META
            if j == 7:
                cell.set_facecolor(meta_bg)
                cell.get_text().set_color("white")

            # Coluna % com cor condicional (linhas de categoria)
            if j == 8 and i <= last_row_idx:
                text = cell.get_text().get_text().replace("%", "")
                try:
                    pv = float(text) if text else np.nan
                except Exception:
                    pv = np.nan
                if not np.isnan(pv):
                    if pv < 100:
                        cell.set_facecolor("#FAD0D0")  # vermelho claro
                        cell.get_text().set_color("#B91C1C")
                    elif 100 <= pv < 120:
                        cell.set_facecolor("#E7F6E7")  # verde claro
                        cell.get_text().set_color("#065F46")
                    else:
                        cell.set_facecolor("#DDEEFF")  # azul claro
                        cell.get_text().set_color("#1E3A8A")

    # === LARGURAS POR COLUNA ===
    # Aplica a largura definida em col_widths para TODAS as células daquela coluna.
    if col_widths and len(col_widths) == n_cols:
        for j, w in enumerate(col_widths):
            for i in range(n_rows):  # header + linhas
                table[i, j].set_width(w)
    else:
        # fallback suave: nada a fazer, mantém proporções default
        pass

    # Título com faixa
    ax.set_title(
        title,
        fontsize=16, fontweight="bold", pad=16, color="white",
        bbox=dict(facecolor=HEADER_BG, edgecolor="black", boxstyle="round,pad=0.4")
    )

    if logo_path:
        try:
            logo = plt.imread(logo_path)
            imagebox = OffsetImage(logo, zoom=0.3)  # ajuste o zoom conforme necessário
            ab = AnnotationBbox(
                imagebox,
                (0.03, 1.08),  # posição relativa ao eixo (x>1 joga à direita do título)
                xycoords="axes fraction",
                frameon=False
            )
            ax.add_artist(ab)
        except Exception as e:
            print(f"Erro ao carregar logo: {e}")

    plt.tight_layout()
    fig.savefig(outfile, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return outfile

# ============ EXECUÇÃO ============
def main():
    con = fdb.connect(
        host=FB_HOST,
        database=FB_DATABASE,
        user=FB_USER,
        password=FB_PASSWORD,
        charset=FB_CHARSET,
    )
    try:
        cur = con.cursor()
        cur.execute(SQL)
        rows = cur.fetchall()
        cols = [d[0].lower() for d in cur.description]

        df = pd.DataFrame(rows, columns=cols)

        # Garante tipos e colunas esperadas
        needed = ["descricao","sem01","sem02","sem03","sem04","sem05","total","meta","perc"]
        for c in needed:
            if c not in df.columns:
                raise ValueError(f"Coluna esperada não encontrada: {c}")

        for c in ["sem01","sem02","sem03","sem04","sem05","total","meta","perc"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # Caso perc venha NULL, calcula
        if df["perc"].isna().any():
            df["perc"] = np.where(df["meta"] > 0, (df["total"] / df["meta"] * 100.0), np.nan)

        # Renderiza
        outfile = render_entradas_table(
            df,
            title=TITULO,
            outfile=ARQUIVO_SAIDA,
            figsize=FIGSIZE,
            col_widths=COL_WIDTHS,
            font_size=FONTE_TABELA,
            logo_path="logo_moya.png"
        )
        print(f"Imagem gerada: {outfile}")

        repo_dir = '/home/ubuntu/repositorios/moya_ordens'

        # Bloco do Git (mantido como no original)
        try:
            # Descomente as linhas abaixo para usar o Git
            repo = Repo(repo_dir)
            repo.git.add('entradas_moya.png')
            repo.index.commit('Atualização entradas MOYA')
            origin = repo.remote(name='origin')
            origin.push()
            print("Arquivo enviado para o GitHub com sucesso!")
        except Exception as e:
            print(f"Erro ao enviar para o GitHub: {e}")

    finally:
        try:
            cur.close()
        except Exception:
            pass
        con.close()

if __name__ == "__main__":
    main()