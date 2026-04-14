"""
Agente Auxiliar Contable — Metrika Group
FastAPI + Claude API + Twilio WhatsApp + openpyxl
"""
import os, io, re, json, logging, tempfile
from datetime import datetime, date, timedelta
from typing import Optional

import httpx
import pdfplumber
import openpyxl
from openpyxl.styles import (Font, PatternFill, Alignment, Border, Side,
                              numbers as xl_numbers)
from openpyxl.utils import get_column_letter

from fastapi import FastAPI, Form, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
import anthropic
from twilio.rest import Client as TwilioClient

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("agente_contable")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WA_NUMBER   = os.getenv("TWILIO_WA_NUMBER", "whatsapp:+14155238886")

claude  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
twilio  = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

app = FastAPI(title="Agente Auxiliar Contable", version="1.0.0")

REGISTROS_CONTABLES = [
    {"doc": "TRF-0012341", "fecha": "2025-01-02", "concepto": "Ingreso - PROVEEDORA TEXTIL S.A.",       "tipo": "CR", "valor": 5200000,  "cuenta": "1305", "conciliado": False},
    {"doc": "CHQ-0056782", "fecha": "2025-01-03", "concepto": "Gasto papeleria - SUMINISTROS LTDA",     "tipo": "DB", "valor": 380000,   "cuenta": "5105", "conciliado": False},
    {"doc": "PSE-0098431", "fecha": "2025-01-05", "concepto": "Seguro - BOLIVAR S.A.",                  "tipo": "DB", "valor": 1250000,  "cuenta": "5135", "conciliado": False},
    {"doc": "TRF-0012398", "fecha": "2025-01-07", "concepto": "Ingreso - CLIENTE OMEGA SAS",            "tipo": "CR", "valor": 3800000,  "cuenta": "1305", "conciliado": False},
    {"doc": "DEB-0034521", "fecha": "2025-01-09", "concepto": "Energia electrica - ENDESA",             "tipo": "DB", "valor": 890000,   "cuenta": "5115", "conciliado": False},
    {"doc": "TRF-0012501", "fecha": "2025-01-12", "concepto": "Ingreso - ALMACEN EL EXITO SA",          "tipo": "CR", "valor": 2100000,  "cuenta": "1305", "conciliado": False},
    {"doc": "CHQ-0056830", "fecha": "2025-01-14", "concepto": "Pago proveedor - CENTRAL LTDA",          "tipo": "DB", "valor": 1500000,  "cuenta": "2205", "conciliado": False},
    {"doc": "PSE-0098512", "fecha": "2025-01-15", "concepto": "Telefonia - ETB S.A.",                   "tipo": "DB", "valor": 245000,   "cuenta": "5120", "conciliado": False},
    {"doc": "TRF-0012560", "fecha": "2025-01-17", "concepto": "Ingreso - COMERCIAL LOS ANDES",          "tipo": "CR", "valor": 4500000,  "cuenta": "1305", "conciliado": False},
    {"doc": "DEB-0034678", "fecha": "2025-01-19", "concepto": "Leasing vehiculo - BANCOLOMBIA",         "tipo": "DB", "valor": 1800000,  "cuenta": "5210", "conciliado": False},
    {"doc": "TRF-0012621", "fecha": "2025-01-21", "concepto": "Pago proveedor - MAPLOCA S.A.",          "tipo": "DB", "valor": 2300000,  "cuenta": "2205", "conciliado": False},
    {"doc": "TRF-0012655", "fecha": "2025-01-22", "concepto": "Ingreso - GRUPO EMPRESARIAL XYZ",        "tipo": "CR", "valor": 5000000,  "cuenta": "1305", "conciliado": False},
    {"doc": "CHQ-0056891", "fecha": "2025-01-24", "concepto": "Ferreteria - EL MARTILLO",               "tipo": "DB", "valor": 420000,   "cuenta": "5105", "conciliado": False},
    {"doc": "TRF-0012700", "fecha": "2025-01-24", "concepto": "Cobro comision servicio - sin banco",    "tipo": "CR", "valor": 950000,   "cuenta": "1305", "conciliado": False},
    {"doc": "PSE-0098601", "fecha": "2025-01-25", "concepto": "Acueducto - TRIPLE A",                   "tipo": "DB", "valor": 185000,   "cuenta": "5115", "conciliado": False},
    {"doc": "TRF-0012710", "fecha": "2025-01-27", "concepto": "ICA enero - Distrito Barranquilla",      "tipo": "DB", "valor": 620000,   "cuenta": "2404", "conciliado": False},
    {"doc": "TRF-0012745", "fecha": "2025-01-28", "concepto": "Ingreso - CLIENTE BETA LTDA",            "tipo": "CR", "valor": 1800000,  "cuenta": "1305", "conciliado": False},
    {"doc": "TRF-0012790", "fecha": "2025-01-30", "concepto": "Devolucion IVA - DIAN",                  "tipo": "CR", "valor": 800000,   "cuenta": "2408", "conciliado": False},
    {"doc": "TRF-0012801", "fecha": "2025-01-31", "concepto": "Retencion fuente - DIAN ene/2025",       "tipo": "DB", "valor": 3000000,  "cuenta": "2365", "conciliado": False},
]

PAGOS_PENDIENTES = [
    {"proveedor": "INDUSTRIAS MAPLOCA S.A.",    "concepto": "Factura 2024-892 - Materia prima",   "valor": 3800000,  "vencimiento": "2025-02-05", "estado": "VENCIDO"},
    {"proveedor": "PAPELERIA UNIVERSAL LTDA",   "concepto": "Factura 2025-011 - Suministros",      "valor": 450000,   "vencimiento": "2025-02-10", "estado": "PROXIMO"},
    {"proveedor": "LEASING BANCOLOMBIA",        "concepto": "Cuota febrero - Leasing vehiculo",    "valor": 1800000,  "vencimiento": "2025-02-19", "estado": "PENDIENTE"},
    {"proveedor": "SEGUROS BOLIVAR S.A.",       "concepto": "Prima trimestral seguro todo riesgo", "valor": 1250000,  "vencimiento": "2025-02-28", "estado": "PENDIENTE"},
    {"proveedor": "DISTRIBUIDORA CENTRAL LTDA", "concepto": "Factura 2024-445 - Mercancia",        "valor": 2100000,  "vencimiento": "2025-01-31", "estado": "VENCIDO"},
]

OBLIGACIONES_FISCALES = [
    {"obligacion": "Retencion en la fuente",  "periodo": "Enero 2025",  "vencimiento": "2025-02-17", "estado": "PENDIENTE", "valor_aprox": 3000000},
    {"obligacion": "IVA bimestral",           "periodo": "Nov-Dic 2024","vencimiento": "2025-02-12", "estado": "PENDIENTE", "valor_aprox": 2800000},
    {"obligacion": "ICA Barranquilla",        "periodo": "Enero 2025",  "vencimiento": "2025-02-20", "estado": "PENDIENTE", "valor_aprox": 620000},
]

SYSTEM_PROMPT = """Eres CONTA, el Auxiliar Contable Inteligente de Distribuidora El Progreso S.A.S., desarrollado por Metrika Group.

Tu función es asistir al contador y al equipo administrativo en las tareas contables del día a día.

CONOCIMIENTO CONTABLE:
- Manejas el Plan Único de Cuentas (PUC) colombiano
- Conoces las obligaciones tributarias: IVA, Retención en la Fuente, ICA, Renta
- Entiendes conciliación bancaria: cruzar extracto bancario vs. registros contables
- Identificas partidas conciliadas (match exacto doc+valor) y partidas abiertas (diferencias)
- Conoces los plazos de la DIAN y los calendarios tributarios colombianos

EMPRESA:
- Nombre: Distribuidora El Progreso S.A.S.
- NIT: 901.234.567-1
- Ciudad: Barranquilla, Colombia
- Banco principal: Bancolombia - Cta. Cte. 63-000123456-78
- Régimen: Responsable de IVA, Gran contribuyente retención

TONO:
- Profesional pero cercano
- Respuestas concisas por WhatsApp (máximo 3-4 párrafos)
- Usa emojis contables con moderación: ✅ ❌ ⚠️ 📊 💰
- Cuando generes un Excel, avisa que lo estás preparando"""

async def parsear_extracto_bancolombia(pdf_bytes: bytes) -> list[dict]:
    log.info(f"Iniciando parser — PDF recibido: {len(pdf_bytes):,} bytes")

    # NIVEL 1: pdfplumber — tablas estructuradas
    log.info("Parser nivel 1: intentando pdfplumber (tablas)...")
    try:
        movimientos = _parsear_con_pdfplumber(pdf_bytes)
        if movimientos:
            log.info(f"Parser nivel 1 OK — {len(movimientos)} movimientos extraídos")
            return movimientos
        log.info("Parser nivel 1: sin resultados, pasando al nivel 2")
    except Exception as e:
        log.error(f"Parser nivel 1 FALLÓ — {type(e).__name__}: {e}", exc_info=True)

    # NIVEL 2: pdfplumber texto plano con regex
    log.info("Parser nivel 2: intentando texto plano (regex Bancolombia)...")
    try:
        movimientos = _parsear_con_texto_plano(pdf_bytes)
        if movimientos:
            log.info(f"Parser nivel 2 OK — {len(movimientos)} movimientos extraídos")
            return movimientos
        log.info("Parser nivel 2: sin resultados, pasando al nivel 3")
    except Exception as e:
        log.error(f"Parser nivel 2 FALLÓ — {type(e).__name__}: {e}", exc_info=True)

    # NIVEL 3: Claude Vision — funciona con cualquier PDF
    log.info("Parser nivel 3: intentando Claude Vision...")
    try:
        movimientos = await _parsear_con_claude_vision(pdf_bytes)
        if movimientos:
            log.info(f"Parser nivel 3 OK — {len(movimientos)} movimientos extraídos")
            return movimientos
        log.info("Parser nivel 3: sin resultados — el PDF no pudo ser procesado")
    except Exception as e:
        log.error(f"Parser nivel 3 FALLÓ — {type(e).__name__}: {e}", exc_info=True)

    log.error("Todos los niveles del parser fallaron o devolvieron 0 movimientos")
    return []


def _parsear_con_pdfplumber(pdf_bytes: bytes) -> list[dict]:
    movimientos = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) < 4:
                        continue
                    fecha_str = str(row[0]).strip() if row[0] else ""
                    if not re.match(r"\d{2}/\d{2}/\d{4}", fecha_str):
                        continue
                    try:
                        fecha = datetime.strptime(fecha_str, "%d/%m/%Y").date()
                        movimientos.append({
                            "fecha": str(fecha),
                            "doc": str(row[1]).strip() if row[1] else "",
                            "concepto": str(row[2]).strip() if row[2] else "",
                            "debito": _parse_valor(str(row[3]) if row[3] else ""),
                            "credito": _parse_valor(str(row[4]) if len(row) > 4 and row[4] else ""),
                            "saldo": _parse_valor(str(row[5]) if len(row) > 5 and row[5] else ""),
                        })
                    except Exception:
                        continue
    return movimientos


def _parsear_con_texto_plano(pdf_bytes: bytes) -> list[dict]:
    """
    Formato real Bancolombia:
    FECHA | DESCRIPCIÓN | SUCURSAL | DCTO. | VALOR | SALDO
    VALOR es positivo (abono/crédito) o negativo (cargo/débito).
    """
    movimientos = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.split("\n"):
                # Formato Bancolombia: fecha  descripcion  sucursal  dcto  valor  saldo
                # VALOR puede ser -1.234.567,89 o 1.234.567,89
                match = re.match(
                    r"(\d{2}/\d{2}/\d{4})\s+"   # fecha
                    r"(.+?)\s+"                  # descripcion
                    r"(\S+)\s+"                  # sucursal
                    r"(\S*)\s+"                  # dcto (puede estar vacío)
                    r"(-?[\d.,]+)\s+"            # valor (positivo o negativo)
                    r"(-?[\d.,]+)$",             # saldo
                    line.strip()
                )
                if match:
                    try:
                        fecha = datetime.strptime(match.group(1), "%d/%m/%Y").date()
                        dcto  = match.group(4).strip()
                        valor = _parse_valor(match.group(5))
                        saldo = _parse_valor(match.group(6))
                        debito  = abs(valor) if valor < 0 else 0.0
                        credito = valor       if valor > 0 else 0.0
                        movimientos.append({
                            "fecha":    str(fecha),
                            "doc":      dcto,
                            "concepto": match.group(2).strip(),
                            "debito":   debito,
                            "credito":  credito,
                            "saldo":    abs(saldo),
                        })
                    except Exception:
                        continue
    return movimientos


async def _parsear_con_claude_vision(pdf_bytes: bytes) -> list[dict]:
    import base64
    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    prompt = """Analiza este extracto bancario de Bancolombia y extrae TODOS los movimientos.

FORMATO DEL EXTRACTO:
Las columnas son: FECHA | DESCRIPCIÓN | SUCURSAL | DCTO. | VALOR | SALDO

REGLAS DE INTERPRETACIÓN:
- FECHA viene en formato DD/MM sin año. Infiere el año desde el encabezado del extracto.
- DESCRIPCIÓN es el concepto del movimiento (ej. "TRANSFERENCIAS A NEQUI").
- DCTO. es el número de documento de referencia; puede estar vacío, en ese caso usa "".
- VALOR negativo (con signo - o entre paréntesis) = cargo → va en "debito", "credito" = 0.
- VALOR positivo = abono → va en "credito", "debito" = 0.
- SALDO es el saldo después del movimiento.
- Todos los valores numéricos deben ir sin puntos de miles ni comas decimales (solo número entero o con punto decimal si aplica).

Devuelve ÚNICAMENTE un JSON válido con esta estructura, sin texto adicional ni explicaciones:
{
  "movimientos": [
    {
      "fecha": "2026-01-01",
      "doc": "",
      "concepto": "TRANSFERENCIAS A NEQUI",
      "debito": 280000,
      "credito": 0,
      "saldo": 1553685.25
    }
  ]
}

Si un campo numérico no aplica usa 0. No incluyas texto fuera del JSON."""

    response = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=4000,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": pdf_b64
                    }
                },
                {
                    "type": "text",
                    "text": prompt
                }
            ]
        }]
    )

    texto = response.content[0].text.strip()
    if "```" in texto:
        texto = texto.split("```")[1]
        if texto.startswith("json"):
            texto = texto[4:]

    data = json.loads(texto.strip())
    return data.get("movimientos", [])

def _parse_valor(s: str) -> float:
    s = s.strip().replace("$", "").replace(".", "").replace(",", "").replace(" ", "")
    try:
        return float(s)
    except Exception:
        return 0.0

def conciliar(mov_banco: list[dict]) -> dict:
    registros = [r.copy() for r in REGISTROS_CONTABLES]
    conciliados = []
    solo_banco  = []
    solo_conta  = []

    for mb in mov_banco:
        match = None
        mb_valor = mb["debito"] or mb["credito"]

        # Intento 1: match por número de documento
        if mb["doc"]:
            for rc in registros:
                if rc["conciliado"]:
                    continue
                if mb["doc"] == rc["doc"]:
                    match = rc
                    break

        # Intento 2 (doc vacío): match por valor + fecha (±1 día de tolerancia)
        if match is None:
            mb_fecha = datetime.strptime(mb["fecha"], "%Y-%m-%d").date()
            for rc in registros:
                if rc["conciliado"]:
                    continue
                rc_fecha = datetime.strptime(rc["fecha"], "%Y-%m-%d").date()
                fecha_ok = abs((mb_fecha - rc_fecha).days) <= 1
                valor_ok = mb_valor and abs(mb_valor - rc["valor"]) < 1
                if fecha_ok and valor_ok:
                    match = rc
                    break

        if match:
            match["conciliado"] = True
            conciliados.append({
                "fecha_banco": mb["fecha"],
                "fecha_conta": match["fecha"],
                "doc": mb["doc"],
                "concepto_banco": mb["concepto"],
                "concepto_conta": match["concepto"],
                "valor": mb["debito"] or mb["credito"],
                "tipo": "DB" if mb["debito"] > 0 else "CR",
                "cuenta_puc": match["cuenta"],
                "estado": "CONCILIADO",
            })
        else:
            solo_banco.append({
                "fecha": mb["fecha"],
                "doc": mb["doc"],
                "concepto": mb["concepto"],
                "valor": mb["debito"] or mb["credito"],
                "tipo": "DB" if mb["debito"] > 0 else "CR",
                "estado": "SOLO EN BANCO",
            })

    for rc in registros:
        if not rc["conciliado"]:
            solo_conta.append({
                "fecha": rc["fecha"],
                "doc": rc["doc"],
                "concepto": rc["concepto"],
                "valor": rc["valor"],
                "tipo": rc["tipo"],
                "cuenta_puc": rc["cuenta"],
                "estado": "SOLO EN CONTABILIDAD",
            })

    total_banco_db = sum(m["debito"]  for m in mov_banco)
    total_banco_cr = sum(m["credito"] for m in mov_banco)
    total_conta_db = sum(r["valor"] for r in REGISTROS_CONTABLES if r["tipo"] == "DB")
    total_conta_cr = sum(r["valor"] for r in REGISTROS_CONTABLES if r["tipo"] == "CR")

    return {
        "conciliados": conciliados,
        "solo_banco":  solo_banco,
        "solo_conta":  solo_conta,
        "resumen": {
            "total_conciliados":   len(conciliados),
            "total_solo_banco":    len(solo_banco),
            "total_solo_conta":    len(solo_conta),
            "debitos_banco":       total_banco_db,
            "creditos_banco":      total_banco_cr,
            "debitos_conta":       total_conta_db,
            "creditos_conta":      total_conta_cr,
            "diferencia_debitos":  total_banco_db - total_conta_db,
            "diferencia_creditos": total_banco_cr - total_conta_cr,
        }
    }

def generar_excel_conciliacion(resultado: dict) -> bytes:
    wb = openpyxl.Workbook()
    AMARILLO  = "FFCC00"
    NEGRO     = "1A1A1A"
    VERDE_OSC = "1E6B3C"
    ROJO_OSC  = "9B1C1C"
    AZUL_OSC  = "1E3A5F"
    GRIS_CLARO  = "F5F5F5"
    VERDE_CLA   = "D4EDDA"
    ROJO_CLA    = "F8D7DA"
    AMARILLO_CL = "FFF3CD"

    def hdr_fill(color): return PatternFill("solid", fgColor=color)
    def font(bold=False, color="000000", size=10):
        return Font(bold=bold, color=color, size=size, name="Calibri")
    def border_thin():
        s = Side(style="thin", color="CCCCCC")
        return Border(left=s, right=s, top=s, bottom=s)
    def fmt_col(ws, col, w):
        ws.column_dimensions[get_column_letter(col)].width = w

    def escribir_encabezado(ws, titulo, subtitulo):
        ws.merge_cells("A1:H1")
        ws["A1"] = "DISTRIBUIDORA EL PROGRESO S.A.S."
        ws["A1"].font = Font(bold=True, size=14, color="FFFFFF", name="Calibri")
        ws["A1"].fill = hdr_fill(NEGRO)
        ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
        ws.row_dimensions[1].height = 30
        ws.merge_cells("A2:H2")
        ws["A2"] = titulo
        ws["A2"].font = Font(bold=True, size=12, color=NEGRO, name="Calibri")
        ws["A2"].fill = hdr_fill(AMARILLO)
        ws["A2"].alignment = Alignment(horizontal="center", vertical="center")
        ws.row_dimensions[2].height = 24
        ws.merge_cells("A3:H3")
        ws["A3"] = subtitulo
        ws["A3"].font = Font(size=9, color="555555", name="Calibri")
        ws["A3"].alignment = Alignment(horizontal="center")

    # Hoja 1 - Conciliados
    ws1 = wb.active
    ws1.title = "Conciliados"
    escribir_encabezado(ws1, "CONCILIACION BANCARIA ENERO 2025",
                        f"Movimientos conciliados | {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    cols1 = ["Fecha Banco","Fecha Conta","No. Documento","Concepto Banco",
             "Concepto Contabilidad","Valor ($)","Tipo","Cuenta PUC"]
    for j, c in enumerate(cols1, 1):
        cell = ws1.cell(row=5, column=j, value=c)
        cell.font  = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill  = hdr_fill(VERDE_OSC)
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = border_thin()
    ws1.row_dimensions[5].height = 28
    for i, row in enumerate(resultado["conciliados"], 6):
        vals = [row["fecha_banco"], row["fecha_conta"], row["doc"],
                row["concepto_banco"], row["concepto_conta"],
                row["valor"], row["tipo"], row["cuenta_puc"]]
        for j, v in enumerate(vals, 1):
            cell = ws1.cell(row=i, column=j, value=v)
            cell.font   = font(size=9)
            cell.border = border_thin()
            cell.fill   = hdr_fill(VERDE_CLA) if i % 2 == 0 else hdr_fill("FFFFFF")
            if j == 6:
                cell.number_format = "#,##0"
                cell.alignment = Alignment(horizontal="right")
            elif j in (1,2,3,7,8):
                cell.alignment = Alignment(horizontal="center")
    widths1 = [13,13,15,35,35,14,7,12]
    for j, w in enumerate(widths1, 1): fmt_col(ws1, j, w)
    last1 = 5 + len(resultado["conciliados"])
    ws1.cell(row=last1+1, column=5, value="TOTAL CONCILIADO:").font = font(bold=True)
    tc = ws1.cell(row=last1+1, column=6, value=sum(r["valor"] for r in resultado["conciliados"]))
    tc.font = Font(bold=True, size=10, color=VERDE_OSC, name="Calibri")
    tc.number_format = "#,##0"
    tc.alignment = Alignment(horizontal="right")

    # Hoja 2 - Partidas Abiertas
    ws2 = wb.create_sheet("Partidas Abiertas")
    escribir_encabezado(ws2, "PARTIDAS ABIERTAS - DIFERENCIAS",
                        "Movimientos sin match entre banco y contabilidad")
    ws2.cell(row=5, column=1, value="SOLO EN EXTRACTO BANCARIO").font = Font(bold=True, color=ROJO_OSC, size=10, name="Calibri")
    ws2.merge_cells("A5:G5")
    cols2 = ["Fecha","No. Documento","Concepto","Valor ($)","Tipo","Estado","Accion Requerida"]
    for j, c in enumerate(cols2, 1):
        cell = ws2.cell(row=6, column=j, value=c)
        cell.font = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill = hdr_fill(ROJO_OSC)
        cell.alignment = Alignment(horizontal="center")
        cell.border = border_thin()
    r = 7
    for row in resultado["solo_banco"]:
        datos = [row["fecha"], row["doc"], row["concepto"],
                 row["valor"], row["tipo"], row["estado"], "Registrar en contabilidad"]
        for j, v in enumerate(datos, 1):
            cell = ws2.cell(row=r, column=j, value=v)
            cell.font = font(size=9)
            cell.border = border_thin()
            cell.fill = hdr_fill(ROJO_CLA)
            if j == 4:
                cell.number_format = "#,##0"
                cell.alignment = Alignment(horizontal="right")
        r += 1
    r += 1
    ws2.cell(row=r, column=1, value="SOLO EN CONTABILIDAD (NO EN BANCO)").font = Font(bold=True, color=AZUL_OSC, size=10, name="Calibri")
    ws2.merge_cells(f"A{r}:G{r}")
    r += 1
    cols2b = ["Fecha","No. Documento","Concepto","Valor ($)","Tipo","Cuenta PUC","Estado"]
    for j, c in enumerate(cols2b, 1):
        cell = ws2.cell(row=r, column=j, value=c)
        cell.font = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill = hdr_fill(AZUL_OSC)
        cell.alignment = Alignment(horizontal="center")
        cell.border = border_thin()
    r += 1
    for row in resultado["solo_conta"]:
        datos = [row["fecha"], row["doc"], row["concepto"],
                 row["valor"], row["tipo"], row.get("cuenta_puc",""), row["estado"]]
        for j, v in enumerate(datos, 1):
            cell = ws2.cell(row=r, column=j, value=v)
            cell.font = font(size=9)
            cell.border = border_thin()
            cell.fill = hdr_fill(AMARILLO_CL)
            if j == 4:
                cell.number_format = "#,##0"
                cell.alignment = Alignment(horizontal="right")
        r += 1
    widths2 = [13,15,40,14,7,12,30]
    for j, w in enumerate(widths2, 1): fmt_col(ws2, j, w)

    # Hoja 3 - Pagos Pendientes
    ws3 = wb.create_sheet("Pagos Pendientes")
    escribir_encabezado(ws3, "CONTROL DE PAGOS PENDIENTES",
                        f"Corte: {datetime.now().strftime('%d/%m/%Y')}")
    cols3 = ["Proveedor","Concepto","Valor ($)","Fecha Vencimiento","Dias","Estado","Prioridad"]
    for j, c in enumerate(cols3, 1):
        cell = ws3.cell(row=5, column=j, value=c)
        cell.font = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill = hdr_fill(NEGRO)
        cell.alignment = Alignment(horizontal="center")
        cell.border = border_thin()
    hoy = date.today()
    for i, pago in enumerate(PAGOS_PENDIENTES, 6):
        venc = datetime.strptime(pago["vencimiento"], "%Y-%m-%d").date()
        dias = (venc - hoy).days
        prioridad = "URGENTE" if dias < 0 else ("ESTA SEMANA" if dias <= 7 else "PROXIMO")
        color_fila = ROJO_CLA if dias < 0 else (AMARILLO_CL if dias <= 7 else VERDE_CLA)
        datos = [pago["proveedor"], pago["concepto"], pago["valor"],
                 pago["vencimiento"], dias, pago["estado"], prioridad]
        for j, v in enumerate(datos, 1):
            cell = ws3.cell(row=i, column=j, value=v)
            cell.font = font(size=9)
            cell.border = border_thin()
            cell.fill = hdr_fill(color_fila)
            if j == 3:
                cell.number_format = "#,##0"
                cell.alignment = Alignment(horizontal="right")
    widths3 = [28,35,14,18,8,12,14]
    for j, w in enumerate(widths3, 1): fmt_col(ws3, j, w)
    last3 = 5 + len(PAGOS_PENDIENTES)
    ws3.cell(row=last3+1, column=2, value="TOTAL POR PAGAR:").font = font(bold=True)
    tc3 = ws3.cell(row=last3+1, column=3, value=sum(p["valor"] for p in PAGOS_PENDIENTES))
    tc3.number_format = "#,##0"
    tc3.font = Font(bold=True, color=ROJO_OSC, size=11, name="Calibri")
    tc3.alignment = Alignment(horizontal="right")

    # Hoja 4 - Obligaciones Fiscales
    ws4 = wb.create_sheet("Obligaciones Fiscales")
    escribir_encabezado(ws4, "CALENDARIO TRIBUTARIO FEBRERO 2025",
                        "Obligaciones fiscales proximas a vencer")
    cols4 = ["Obligacion","Periodo","Vencimiento","Dias Restantes","Estado","Valor Aprox ($)"]
    for j, c in enumerate(cols4, 1):
        cell = ws4.cell(row=5, column=j, value=c)
        cell.font = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill = hdr_fill(AZUL_OSC)
        cell.alignment = Alignment(horizontal="center")
        cell.border = border_thin()
    for i, ob in enumerate(OBLIGACIONES_FISCALES, 6):
        venc = datetime.strptime(ob["vencimiento"], "%Y-%m-%d").date()
        dias = (venc - hoy).days
        color_fila = ROJO_CLA if dias < 5 else (AMARILLO_CL if dias <= 15 else VERDE_CLA)
        datos = [ob["obligacion"], ob["periodo"], ob["vencimiento"],
                 dias, ob["estado"], ob["valor_aprox"]]
        for j, v in enumerate(datos, 1):
            cell = ws4.cell(row=i, column=j, value=v)
            cell.font = font(size=9)
            cell.border = border_thin()
            cell.fill = hdr_fill(color_fila)
            if j == 6:
                cell.number_format = "#,##0"
                cell.alignment = Alignment(horizontal="right")
    widths4 = [28,18,15,15,12,18]
    for j, w in enumerate(widths4, 1): fmt_col(ws4, j, w)

    # Hoja 5 - Resumen
    ws5 = wb.create_sheet("Resumen Ejecutivo")
    ws5.sheet_view.showGridLines = False
    escribir_encabezado(ws5, "RESUMEN EJECUTIVO CONCILIACION ENERO 2025",
                        "Distribuidora El Progreso S.A.S. | NIT: 901.234.567-1")
    res = resultado["resumen"]
    metricas = [
        ("", ""),
        ("CONCILIACION BANCARIA", ""),
        ("Movimientos conciliados",        res["total_conciliados"]),
        ("Partidas solo en banco",         res["total_solo_banco"]),
        ("Partidas solo en contabilidad",  res["total_solo_conta"]),
        ("Debitos banco",                  res["debitos_banco"]),
        ("Creditos banco",                 res["creditos_banco"]),
        ("Diferencia debitos",             res["diferencia_debitos"]),
        ("Diferencia creditos",            res["diferencia_creditos"]),
        ("", ""),
        ("PAGOS PENDIENTES", ""),
        ("Total por pagar proveedores",    sum(p["valor"] for p in PAGOS_PENDIENTES)),
        ("Pagos vencidos",                 sum(p["valor"] for p in PAGOS_PENDIENTES if p["estado"] == "VENCIDO")),
        ("Pagos proximos a vencer",        sum(p["valor"] for p in PAGOS_PENDIENTES if p["estado"] == "PROXIMO")),
    ]
    for i, (etiqueta, valor) in enumerate(metricas, 5):
        cell_e = ws5.cell(row=i, column=2, value=etiqueta)
        cell_v = ws5.cell(row=i, column=4, value=valor if valor != "" else "")
        if etiqueta in ("CONCILIACION BANCARIA", "PAGOS PENDIENTES"):
            cell_e.font = Font(bold=True, color="FFFFFF", size=10, name="Calibri")
            cell_e.fill = hdr_fill(NEGRO)
            ws5.merge_cells(f"B{i}:E{i}")
            ws5.row_dimensions[i].height = 22
        elif etiqueta:
            cell_e.font = font(size=10)
            cell_e.fill = hdr_fill(GRIS_CLARO)
            if isinstance(valor, (int, float)) and valor > 100:
                cell_v.number_format = "#,##0"
                cell_v.alignment = Alignment(horizontal="right")
            cell_e.border = border_thin()
            cell_v.border = border_thin()
    fmt_col(ws5, 2, 35)
    fmt_col(ws5, 3, 5)
    fmt_col(ws5, 4, 18)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()

def consultar_agente(mensaje: str, contexto_extra: str = "") -> str:
    prompt = mensaje
    if contexto_extra:
        prompt = f"{contexto_extra}\n\nMensaje del usuario: {mensaje}"
    resp = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}]
    )
    return resp.content[0].text

def enviar_whatsapp(to: str, body: str):
    twilio.messages.create(
        from_=TWILIO_WA_NUMBER,
        to=f"whatsapp:{to}" if not to.startswith("whatsapp:") else to,
        body=body
    )

def enviar_whatsapp_media(to: str, body: str, media_url: str):
    twilio.messages.create(
        from_=TWILIO_WA_NUMBER,
        to=f"whatsapp:{to}" if not to.startswith("whatsapp:") else to,
        body=body,
        media_url=[media_url]
    )

def subir_excel_twilio(excel_bytes: bytes, filename: str) -> str:
    path = f"/tmp/{filename}"
    with open(path, "wb") as f:
        f.write(excel_bytes)
    base = os.getenv("RAILWAY_PUBLIC_DOMAIN", "http://localhost:8000")
    return f"{base}/descargar/{filename}"

@app.get("/")
def root():
    return {"status": "ok", "agente": "Auxiliar Contable Metrika Group", "version": "1.0"}

@app.get("/descargar/{filename}")
def descargar_archivo(filename: str):
    from fastapi.responses import FileResponse
    path = f"/tmp/{filename}"
    if not os.path.exists(path):
        raise HTTPException(404, "Archivo no encontrado")
    return FileResponse(path, filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.post("/webhook/whatsapp")
async def webhook_whatsapp(
    background_tasks: BackgroundTasks,
    From: str = Form(...),
    Body: str = Form(default=""),
    NumMedia: str = Form(default="0"),
    MediaUrl0: str = Form(default=""),
    MediaContentType0: str = Form(default=""),
):
    numero = From.replace("whatsapp:", "")
    mensaje = Body.strip().lower()
    log.info(f"Mensaje de {numero}: '{Body}'")

    if int(NumMedia) > 0 and "pdf" in MediaContentType0.lower():
        background_tasks.add_task(procesar_extracto_pdf, numero, MediaUrl0)
        enviar_whatsapp(numero,
            "Recibi tu extracto bancario. Estoy procesando la conciliacion... en unos segundos te envio el Excel con el resultado.")
        return {"status": "procesando"}

    if any(k in mensaje for k in ["conciliar","conciliacion","extracto"]):
        enviar_whatsapp(numero,
            "Para hacer la conciliacion bancaria, enviame el extracto en PDF por este chat. Acepto extractos de Bancolombia.")
        return {"status": "ok"}

    if any(k in mensaje for k in ["pagos","pendientes","vencimientos","proveedores"]):
        background_tasks.add_task(reporte_pagos_pendientes, numero)
        return {"status": "ok"}

    if any(k in mensaje for k in ["impuestos","fiscal","tributario","dian","obligaciones"]):
        background_tasks.add_task(reporte_fiscal, numero)
        return {"status": "ok"}

    if any(k in mensaje for k in ["ayuda","help","hola","buenas","buenos"]):
        enviar_whatsapp(numero,
            "Hola! Soy CONTA, tu Auxiliar Contable Inteligente de Metrika Group.\n\n"
            "Puedo ayudarte con:\n"
            "- Conciliacion bancaria: enviame el extracto en PDF\n"
            "- Pagos pendientes: escribe 'pagos pendientes'\n"
            "- Obligaciones fiscales: escribe 'impuestos'\n"
            "- Consultas contables: preguntame lo que necesites")
        return {"status": "ok"}

    background_tasks.add_task(responder_consulta_libre, numero, Body)
    return {"status": "ok"}

async def procesar_extracto_pdf(numero: str, media_url: str):
    try:
        log.info(f"Descargando PDF desde Twilio — URL: {media_url}")
        async with httpx.AsyncClient() as client:
            resp = await client.get(media_url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
        log.info(f"PDF descargado — HTTP {resp.status_code}, {len(resp.content):,} bytes, Content-Type: {resp.headers.get('content-type', 'desconocido')}")
        if resp.status_code != 200:
            log.error(f"Error descargando PDF — HTTP {resp.status_code}: {resp.text[:200]}")
            enviar_whatsapp(numero, f"No pude descargar el archivo (HTTP {resp.status_code}). Intenta enviarlo de nuevo.")
            return
        pdf_bytes = resp.content
        mov_banco = await parsear_extracto_bancolombia(pdf_bytes)
        if not mov_banco:
            enviar_whatsapp(numero, "No pude leer los movimientos del PDF. Asegurate que sea un extracto de Bancolombia en texto.")
            return
        resultado = conciliar(mov_banco)
        res = resultado["resumen"]
        excel_bytes = generar_excel_conciliacion(resultado)
        filename = f"Conciliacion_Enero2025_{datetime.now().strftime('%H%M%S')}.xlsx"
        url_excel = subir_excel_twilio(excel_bytes, filename)
        vencidos = sum(1 for p in PAGOS_PENDIENTES if p["estado"] == "VENCIDO")
        proximos = sum(1 for p in PAGOS_PENDIENTES if p["estado"] == "PROXIMO")
        msg = (
            f"Conciliacion Enero 2025 lista!\n\n"
            f"Resultado:\n"
            f"- Movimientos conciliados: {res['total_conciliados']}\n"
            f"- Solo en extracto banco: {res['total_solo_banco']}\n"
            f"- Solo en contabilidad: {res['total_solo_conta']}\n\n"
        )
        if res["total_solo_banco"] + res["total_solo_conta"] > 0:
            msg += f"Hay {res['total_solo_banco'] + res['total_solo_conta']} partidas abiertas que requieren revision.\n\n"
        if vencidos > 0:
            msg += f"{vencidos} pago(s) a proveedores VENCIDOS\n"
        if proximos > 0:
            msg += f"{proximos} pago(s) proximo(s) a vencer\n"
        msg += "\nTe adjunto el Excel con el detalle completo (5 hojas)."
        enviar_whatsapp_media(numero, msg, url_excel)
    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
        enviar_whatsapp(numero, "Ocurrio un error procesando el extracto. Intenta de nuevo.")

def reporte_pagos_pendientes(numero: str):
    hoy = date.today()
    msg = "Pagos Pendientes a Proveedores\n\n"
    for p in PAGOS_PENDIENTES:
        venc = datetime.strptime(p["vencimiento"], "%Y-%m-%d").date()
        dias = (venc - hoy).days
        estado = "VENCIDO hace " + str(abs(dias)) + " dias" if dias < 0 else "Vence en " + str(dias) + " dias"
        msg += f"{p['proveedor']}\n{p['concepto']}\n${p['valor']:,.0f} | {estado}\n\n"
    msg += f"Total por pagar: ${sum(p['valor'] for p in PAGOS_PENDIENTES):,.0f}"
    enviar_whatsapp(numero, msg)

def reporte_fiscal(numero: str):
    hoy = date.today()
    msg = "Obligaciones Fiscales - Febrero 2025\n\n"
    for ob in OBLIGACIONES_FISCALES:
        venc = datetime.strptime(ob["vencimiento"], "%Y-%m-%d").date()
        dias = (venc - hoy).days
        msg += f"{ob['obligacion']}\nPeriodo: {ob['periodo']}\nVence: {ob['vencimiento']} ({dias} dias)\nValor aprox: ${ob['valor_aprox']:,.0f}\n\n"
    msg += "Recuerda verificar las fechas exactas segun tu NIT en el calendario DIAN."
    enviar_whatsapp(numero, msg)

def responder_consulta_libre(numero: str, mensaje: str):
    respuesta = consultar_agente(mensaje)
    enviar_whatsapp(numero, respuesta)
