from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
from reportlab.platypus import KeepTogether
from dotenv import load_dotenv
import fdb
import os
import re
from pathlib import Path

from svglib.svglib import svg2rlg
from reportlab.graphics import renderPDF

load_dotenv()


def make_header_footer(title: str, filter_text: str):
    def header_footer(canvas, doc):
        canvas.saveState()
        page_w, page_h = A4

        left_x = doc.leftMargin
        right_x = page_w - doc.rightMargin
        top_y = page_h - 0.80 * inch

        # ===== Logo PNG =====
        try:
            logo = ImageReader("logo_moya.png")
            logo_w = 60   # largura em pontos
            logo_h = 22   # altura em pontos
            logo_x = left_x + 10
            logo_y = top_y - logo_h + 45
            canvas.drawImage(logo, logo_x, logo_y, width=logo_w, height=logo_h, mask="auto")
        except Exception as e:
            print("Erro ao carregar logo PNG:", e)
            logo_w = 0

        # ===== Título ao lado da logo =====
        title_x = left_x + logo_w - 50
        canvas.setFont("Helvetica-Bold", 11)
        canvas.setFillColor(colors.black)
        canvas.drawString(title_x, top_y, title)

        # ===== Data/hora e paginação =====
        canvas.setFont("Helvetica", 8.5)
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        page_no = canvas.getPageNumber()
        page_text = f"Gerado em: {now}  |  Página {page_no}"
        text_width = canvas.stringWidth(page_text, "Helvetica", 9)
        canvas.drawString(right_x - text_width, top_y, page_text)

        # ===== Filtro =====
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(colors.HexColor("#333333"))
        canvas.drawString(title_x, top_y - 12, filter_text)

        # ===== Linha separadora =====
        canvas.setStrokeColor(colors.HexColor("#DDDDDD"))
        canvas.setLineWidth(0.5)
        canvas.line(left_x, top_y - 18, right_x, top_y - 18)

        canvas.restoreState()
    return header_footer


# ------------ NOVO: abrir conexão única ------------
def get_conn(db_config):
    return fdb.connect(
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
        charset="UTF8"
    )


# ------------ NOVO: listar vendedores no período ------------
def list_vendedores(conn, dt_ini, dt_fim):
    """
    Retorna lista de tuplas (vendedor_id, vendedor_nomeReduzidoOuVazio),
    apenas dos vendedores que têm OS no período.
    """
    sql = """
        SELECT DISTINCT o.vendedor, COALESCE(v.nomered, '')
          FROM osordem o
          LEFT JOIN vendedores v ON v.vendedor = o.vendedor
         WHERE o.Abertura BETWEEN ? AND ?
         ORDER BY 1
    """
    cur = conn.cursor()
    cur.execute(sql, (dt_ini, dt_fim))
    return cur.fetchall()  # [(id, nomeReduzido), ...]


def get_data_from_firebird(conn, dt_ini, dt_fim, vendedor=None):
    """
    Busca dados. Se vendedor for informado, aplica AND o.vendedor = ?
    """
    data = []
    try:
        cursor = conn.cursor()
        base_query = """
        SELECT
               o.situacao,
               CASE o.situacao
                 WHEN '90' THEN 'Cancelado'
                 WHEN '99' THEN 'Encerrado'
                 ELSE s.descricao
               END AS desc_situacao,
               REPLACE(L.descricao, 'REMESSA RETORNO - ', '') AS descricao_linha,
               o.ordem, o.abertura, o.cadastro,  t.nome,
               e.produto AS RR, e.descricao AS desc_equipamento,
               CAST(o.Ent_Prev AS DATE) AS Prev_Conclusao,
               o.vendedor || ' - ' || v.nomered AS nome_vendedor,
               t.vendedortmk || ' - ' || v2.nomered AS nome_vend_interno
          FROM osordem o
          INNER JOIN cadastro t ON t.codigo = o.cadastro
          INNER JOIN osequipamentos e ON e.equipamento = o.equipamento
          INNER JOIN ceprodutos p ON p.produto = e.produto
          LEFT JOIN celinhas L ON p.linha = L.linha
          LEFT JOIN ossituacao s ON s.situacao = o.situacao
          LEFT JOIN vendedores v ON v.vendedor = o.vendedor
          LEFT JOIN vendedores v2 ON v2.vendedor = t.vendedortmk
         WHERE o.Abertura BETWEEN ? AND ?
         AND t.nome NOT LIKE '%JCC%'
         AND t.nome NOT LIKE 'LOG %'
        """
        params = [dt_ini, dt_fim]
        if vendedor is not None:
            base_query += " AND o.vendedor = ?"
            params.append(vendedor)

        base_query += " ORDER BY o.situacao, o.ordem"
        cursor.execute(base_query, tuple(params))
        data = cursor.fetchall()
    except Exception as e:
        print(f"Erro ao buscar dados do Firebird: {e}")
    return data

def get_resumo_linha(conn, dt_ini, dt_fim, vendedor=None):
    """
    Retorna lista de tuplas (linha, descricao, quantidade) do resumo por linha.
    """
    sql = """
        SELECT P.linha, REPLACE(L.descricao, 'REMESSA RETORNO - ', '') AS descricao, COUNT(E.produto) AS qtde
          FROM osordem o
          INNER JOIN cadastro t ON t.codigo = o.cadastro
          INNER JOIN osequipamentos e ON e.equipamento = o.equipamento
          INNER JOIN ceprodutos p ON p.produto = e.produto
          LEFT JOIN celinhas L ON p.linha = L.linha
          LEFT JOIN ossituacao s ON s.situacao = o.situacao
          LEFT JOIN vendedores v ON v.vendedor = o.vendedor
          LEFT JOIN vendedores v2 ON v2.vendedor = t.vendedortmk
         WHERE o.Abertura BETWEEN ? AND ?
    """
    params = [dt_ini, dt_fim]
    if vendedor is not None:
        sql += " AND o.vendedor = ?"
        params.append(vendedor)
    sql += " GROUP BY P.linha, L.descricao ORDER BY P.linha, L.descricao"
    cur = conn.cursor()
    cur.execute(sql, tuple(params))
    return cur.fetchall()  # [(linha, descricao, qtde), ...]


def generate_pdf(filename, data, filter_text, resumo_linha=None):
    # Margens maiores no topo/rodapé para não colidir com cabeçalho/rodapé
    doc = SimpleDocTemplate(
        filename,
        pagesize=A4,
        leftMargin=0.0001 * inch,
        rightMargin=0.0001 * inch,
        topMargin=1.05 * inch,
        bottomMargin=0.45 * inch,
    )

    styles = getSampleStyleSheet()

    # Estilos customizados (minimalista, moderno, com boa legibilidade)
    body_style = ParagraphStyle(
        name="TableBody",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=5,
        leading=9.2,
        spaceAfter=0
    )
    header_style = ParagraphStyle(
        name="TableHeader",
        parent=styles["BodyText"],
        fontName="Helvetica-Bold",
        fontSize=5,
        leading=9.6,
        textColor=colors.white,
        alignment=1
    )

    title_style = ParagraphStyle(
        name="ReportTitle", fontName="Helvetica-Bold", fontSize=12, spaceAfter=6, alignment=1
    )

    story = []

    headers = [
        "SITUAÇÃO",
        "DESC. SITUAÇÃO",
        "LINHA",
        "ORDEM",
        "ABERTURA",
        "CADASTRO",
        "NOME",
        "RR",
        "DESC. EQUIP.",
        "PREV. CONCLUSÃO",
        "VEND.",
        "VEND. INTERNO",
    ]

    # Envolver todos os campos em Paragraph para quebra de linha automática
    table_data = [[Paragraph(h, header_style) for h in headers]]
    for row in data:
        processed_row = [Paragraph(str(item), body_style) for item in row]
        table_data.append(processed_row)

    # Largura útil da página
    usable_width = A4[0] - (doc.leftMargin + doc.rightMargin)

    # Distribuição de colunas (soma = 1.0)
    col_widths = [
        usable_width * 0.06,   # SITUAÇÃO
        usable_width * 0.06,   # DESC. SITUAÇÃO
        usable_width * 0.08,   # LINHA
        usable_width * 0.045,  # ORDEM
        usable_width * 0.06,   # ABERTURA
        usable_width * 0.06,   # CADASTRO
        usable_width * 0.14,   # NOME
        usable_width * 0.065,  # RR
        usable_width * 0.18,   # DESC. EQUIPAMENTO
        usable_width * 0.07,   # PREV. CONCLUSÃO
        usable_width * 0.08,   # VEND.
        usable_width * 0.07,   # VEND. INTERNO
    ]

    table_style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#a51a19")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),

        ("ALIGN", (0, 1), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),

        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#FFFFFF"), colors.HexColor("#F7F7F7")]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#DDDDDD")),

        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ])

    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    table.setStyle(table_style)

    story.append(table)
    story.append(Spacer(1, 8))
    story.append(Paragraph(f"Sub-Total (Quantidade de motores): {len(data)}",
                           ParagraphStyle(name="SubTotal", fontName="Helvetica-Bold", fontSize=9, alignment=2)))
    if resumo_linha:
        story.append(Spacer(1, 16))
        # Agrupe o título e a tabela juntos no KeepTogether
        resumo_title = Paragraph(
            "Resumo por Linha",
            ParagraphStyle(name="ResumoTitle", fontName="Helvetica-Bold", fontSize=10, alignment=0)
        )
        resumo_headers = ["Linha", "Descrição", "Qtde"]
        resumo_data = [[Paragraph(h, styles["Heading6"]) for h in resumo_headers]]
        for linha, descricao, qtde in resumo_linha:
            resumo_data.append([
                Paragraph(str(linha), styles["BodyText"]),
                Paragraph(str(descricao), styles["BodyText"]),
                Paragraph(str(qtde), styles["BodyText"]),
            ])
        resumo_table = Table(resumo_data, colWidths=[60, 200, 40])
        resumo_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e0e0e0")),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (2, 1), (2, -1), "RIGHT"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CCCCCC")),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        # Inclua o título e a tabela juntos no KeepTogether
        story.append(KeepTogether([resumo_title, resumo_table]))

    # Cabeçalho/rodapé em todas as páginas
    hf = make_header_footer("Relatórios de Entradas de Motores", filter_text)
    doc.build(story, onFirstPage=hf, onLaterPages=hf)


# ------------ util para nome de arquivo ------------
def sanitize_filename(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s)
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:150] if len(s) > 150 else s


if __name__ == "__main__":
    # Configurações do banco de dados Firebird
    db_config = {
        "host": os.getenv("DT_HOST"),
        "port": 3050,
        "database": os.getenv("DT_DATABASE"),
        "user": os.getenv("DT_USER"),
        "password": os.getenv("DT_PASSWORD")
    }

    # Datas para a consulta
    start_date = datetime(2025, 8, 3).date()  # YYYY, M, D
    end_date   = datetime(2025, 8, 9).date()  # YYYY, M, D

    # Corrige aspas internas no f-string
    filter_text_base = (
        f"Filtro: Abertura de {start_date.strftime('%d/%m/%Y')} até {end_date.strftime('%d/%m/%Y')}, "
        f"Cadastro de 0 até 999999999, Produto (RR Motor) de até zz, Linha de até zz, "
        f"Situação de até 99, Tipo = Detalhado"
    )

    # Pasta de saída
    out_dir = Path("rel")
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        conn = get_conn(db_config)

        # (Opcional) Gera o PDF geral (tudo no período)
        data_all = get_data_from_firebird(conn, start_date, end_date, vendedor=None)
        resumo_all = get_resumo_linha(conn, start_date, end_date, vendedor=None)
        if data_all:
            generate_pdf(str(out_dir / "rel_GERAL.pdf"), data_all, filter_text_base, resumo_linha=resumo_all)
            print("PDF geral gerado:", out_dir / "rel_GERAL.pdf")
        else:
            print("Nenhum dado encontrado para o PDF GERAL.")

        # Lista vendedores ativos no período
        vendedores = list_vendedores(conn, start_date, end_date)
        filtro_ids = {17, 29}
        if not vendedores:
            print("Nenhum vendedor com registros no período.")
        else:
            for vend_id, vend_nome in vendedores:
                if vend_id not in filtro_ids:
                    continue

                dados_vend = get_data_from_firebird(conn, start_date, end_date, vendedor=vend_id)
                resumo_vend = get_resumo_linha(conn, start_date, end_date, vendedor=vend_id)
                if not dados_vend:
                    print(f"Sem dados para vendedor {vend_id} ({vend_nome}).")
                    continue

                nome_legivel = f"{vend_id} - {vend_nome}".strip(" -")
                filtro_vend = f"{filter_text_base} | Vendedor: {nome_legivel}"

                safe_nome = f"{vend_id}"
                file_out = out_dir / f"rel_{safe_nome}.pdf"

                generate_pdf(str(file_out), dados_vend, filtro_vend, resumo_linha=resumo_vend)
                print("PDF gerado:", file_out)

    except Exception as e:
        print("Erro no processo:", e)
    finally:
        try:
            conn.close()
        except:
            pass
