"""
Agente Auxiliar Contable — Metrika Group
FastAPI + Claude API + Twilio WhatsApp + openpyxl
"""
import os, io, re, logging, math
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Optional

import httpx
import pdfplumber
import openpyxl
import google.generativeai as genai
from openpyxl.styles import (Font, PatternFill, Alignment, Border, Side)
from openpyxl.utils import get_column_letter

from fastapi import FastAPI, Form, BackgroundTasks, HTTPException, UploadFile, File, Request
from fastapi.responses import JSONResponse, FileResponse
import anthropic
from twilio.rest import Client as TwilioClient

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("agente_contable")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WA_NUMBER   = os.getenv("TWILIO_WA_NUMBER", "whatsapp:+14155238886")

claude  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
twilio  = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
genai.configure(api_key=GEMINI_API_KEY)
gemini  = genai.GenerativeModel("gemini-2.0-flash")

app = FastAPI(title="Agente Auxiliar Contable", version="1.0.0")

SYSTEM_PROMPT = """Eres CONTA, auxiliar contable de Metrika Group.
Respondes en español, eres preciso y conciso."""

# ─── Sesiones por número de teléfono (TTL 24 h) ──────────────────────────────
from dataclasses import dataclass, field as dc_field

@dataclass
class SesionExtracto:
    mov_banco:    list[dict]
    resumen:      dict
    clasificados: list[dict]
    timestamp:    datetime = dc_field(default_factory=datetime.now)

SESIONES: dict[str, SesionExtracto] = {}
SESION_TTL_HORAS = 24

def limpiar_sesiones_expiradas():
    ahora = datetime.now()
    expiradas = [n for n, s in SESIONES.items()
                 if (ahora - s.timestamp).total_seconds() > SESION_TTL_HORAS * 3600]
    for n in expiradas:
        del SESIONES[n]
        log.info(f"Sesión expirada eliminada: {n}")

# ─── Reglas locales de clasificación PUC ─────────────────────────────────────
REGLAS_PUC_LOCAL = [
    (["intereses ahorros", "abono intereses", "intereses"],  "1110", "Bancos"),
    (["nequi", "transferencia nequi"],                        "1305", "Clientes"),
    (["nomina", "nomi ", "salario", "empleado"],              "5105", "Gastos de personal"),
    (["farmatodo", "farmacia", "drogueria", "drogas"],        "5305", "Otros gastos operacionales"),
    (["mercadopago", "mercado pago"],                         "2205", "Proveedores nacionales"),
    (["claude.ai", "subscri", "suscripci"],                   "5305", "Otros gastos operacionales"),
    (["gmf", "4x1000", "gravamen movimiento"],                "5305", "Otros gastos operacionales"),
    (["combustible", "gasolina", "combustibl"],               "5210", "Gastos de viaje"),
    (["retenci", "retencion", "ret. fte"],                    "2365", "Retención en la fuente a pagar"),
    (["iva ", "impuesto ventas"],                             "2408", "IVA generado"),
    (["energia", "electrica", "epm ", "codensa"],             "5115", "Servicios públicos"),
    (["acueducto", "agua ", "triple a"],                      "5115", "Servicios públicos"),
    (["telefon", "celular", "internet", "claro", "movistar"], "5120", "Comunicaciones"),
    (["arrend", "alquiler", "canon"],                         "5135", "Arrendamientos"),
    (["seguro", "prima ", "bolivar", "sura "],                "5165", "Seguros"),
    (["pago nomi", "pago de nomi"],                           "5105", "Gastos de personal"),
    (["transferencia"],                                        "1110", "Bancos"),
]

def clasificar_con_reglas_locales(mov_banco: list[dict]) -> list[dict]:
    resultado = []
    for m in mov_banco:
        desc = m.get("concepto", "").lower()
        cuenta, nombre = None, None
        for keywords, cod, nom in REGLAS_PUC_LOCAL:
            if any(k in desc for k in keywords):
                cuenta, nombre = cod, nom
                break
        resultado.append({**m, "cuenta_puc": cuenta, "nombre_puc": nombre})
    return resultado

def computar_resumen_wa(mov_banco: list[dict]) -> dict:
    ingresos = sum(m["credito"] for m in mov_banco)
    egresos  = sum(m["debito"]  for m in mov_banco)
    top10 = sorted(mov_banco, key=lambda m: m["credito"] + m["debito"], reverse=True)[:10]
    dist: dict[str, dict] = {}
    for m in mov_banco:
        cod = m.get("cuenta_puc") or "Sin clasificar"
        nom = m.get("nombre_puc") or "Sin clasificar"
        if cod not in dist:
            dist[cod] = {"nombre": nom, "n": 0, "total": 0.0}
        dist[cod]["n"] += 1
        dist[cod]["total"] += m["credito"] + m["debito"]
    return {
        "total_tx":   len(mov_banco),
        "ingresos":   ingresos,
        "egresos":    egresos,
        "saldo":      ingresos - egresos,
        "top10":      top10,
        "por_cuenta": dist,
    }

def fmt_cop(n: float) -> str:
    return f"${n:,.0f}".replace(",", ".")

# ─── Parsers de extracto Bancolombia ─────────────────────────────────────────

async def parsear_extracto_bancolombia(pdf_bytes: bytes) -> list[dict]:
    log.info(f"Iniciando parser — PDF recibido: {len(pdf_bytes):,} bytes")

    log.info("Parser nivel 1: texto plano + regex Bancolombia ahorros...")
    try:
        movimientos = _parsear_bancolombia_ahorros(pdf_bytes)
        if len(movimientos) > 10:
            log.info(f"Parser nivel 1 OK — {len(movimientos)} movimientos extraídos")
            return movimientos
        log.info(f"Parser nivel 1: {len(movimientos)} mov (umbral 10), pasando al nivel 2")
    except Exception as e:
        log.error(f"Parser nivel 1 FALLÓ — {type(e).__name__}: {e}", exc_info=True)

    log.info("Parser nivel 2: pdfplumber tablas (fecha completa)...")
    try:
        movimientos = _parsear_con_tablas(pdf_bytes)
        if movimientos:
            log.info(f"Parser nivel 2 OK — {len(movimientos)} movimientos extraídos")
            return movimientos
        log.info("Parser nivel 2: sin resultados — PDF no compatible")
    except Exception as e:
        log.error(f"Parser nivel 2 FALLÓ — {type(e).__name__}: {e}", exc_info=True)

    log.error("Ambos parsers fallaron o devolvieron 0 movimientos")
    return []


# Formato Bancolombia Ahorros: D/MM  DESCRIPCION  VALOR  SALDO
_RX_BANC_TX = re.compile(
    r"^(\d{1,2}/\d{2})"
    r"\s+(.+?)"
    r"\s+(-?[\d,]*\.\d{2})"
    r"\s+([\d,]*\.\d{2})\s*$"
)
_RX_DESDE = re.compile(r"DESDE:\s*(\d{4})/(\d{2})/\d{2}")


def _parsear_bancolombia_ahorros(pdf_bytes: bytes) -> list[dict]:
    movimientos: list[dict] = []
    desde_year  = datetime.now().year
    desde_month = 1

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages_text: list[str] = []
        all_text = ""
        for page in pdf.pages:
            t = page.extract_text() or ""
            pages_text.append(t)
            all_text += t + "\n"

        m = _RX_DESDE.search(all_text)
        if m:
            desde_year  = int(m.group(1))
            desde_month = int(m.group(2))

        current_year = desde_year
        prev_month   = desde_month

        for text in pages_text:
            for line in text.split("\n"):
                line = line.strip()
                mx = _RX_BANC_TX.match(line)
                if not mx:
                    continue

                fecha_str, concepto, valor_str, saldo_str = mx.groups()
                day_s, month_s = fecha_str.split("/")
                tx_month = int(month_s)
                tx_day   = int(day_s)

                if tx_month < prev_month:
                    current_year += 1
                prev_month = tx_month

                try:
                    fecha   = datetime(current_year, tx_month, tx_day).date()
                    valor   = _parse_valor_banc(valor_str)
                    saldo   = _parse_valor_banc(saldo_str)
                    debito  = abs(valor) if valor < 0 else 0.0
                    credito = valor      if valor > 0 else 0.0
                    movimientos.append({
                        "fecha":    str(fecha),
                        "doc":      "",
                        "concepto": concepto.strip(),
                        "debito":   debito,
                        "credito":  credito,
                        "saldo":    saldo,
                    })
                except (ValueError, Exception):
                    continue

    return movimientos


def _parsear_con_tablas(pdf_bytes: bytes) -> list[dict]:
    movimientos: list[dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            for table in (page.extract_tables() or []):
                for row in (table or []):
                    if not row or len(row) < 4:
                        continue
                    fecha_str = str(row[0]).strip() if row[0] else ""
                    if not re.match(r"\d{2}/\d{2}/\d{4}", fecha_str):
                        continue
                    try:
                        fecha = datetime.strptime(fecha_str, "%d/%m/%Y").date()
                        movimientos.append({
                            "fecha":    str(fecha),
                            "doc":      str(row[1]).strip() if row[1] else "",
                            "concepto": str(row[2]).strip() if row[2] else "",
                            "debito":   _parse_valor_co(str(row[3]) if row[3] else ""),
                            "credito":  _parse_valor_co(str(row[4]) if len(row) > 4 and row[4] else ""),
                            "saldo":    _parse_valor_co(str(row[5]) if len(row) > 5 and row[5] else ""),
                        })
                    except Exception:
                        continue
    return movimientos


def _parse_valor_banc(s: str) -> float:
    s = s.strip().replace("$", "").replace(",", "").replace(" ", "")
    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_valor_co(s: str) -> float:
    s = s.strip().replace("$", "").replace(".", "").replace(",", ".").replace(" ", "")
    try:
        return float(s)
    except Exception:
        return 0.0

# ─── Conciliación: solo datos reales del banco ───────────────────────────────

def conciliar(mov_banco: list[dict]) -> dict:
    """
    Clasifica y agrupa los movimientos reales del banco por cuenta PUC.
    Sin registros contables propios del usuario: no hay cruce inventado.
    Para conciliación completa, el usuario debe importar sus registros de OSSADO.
    """
    if mov_banco and "cuenta_puc" not in mov_banco[0]:
        mov_banco = clasificar_con_reglas_locales(mov_banco)

    clasificados   = [m for m in mov_banco if m.get("cuenta_puc")]
    sin_clasificar = [m for m in mov_banco if not m.get("cuenta_puc")]

    top_egresos = sorted(
        [m for m in mov_banco if m["debito"] > 0],
        key=lambda m: m["debito"],
        reverse=True
    )[:20]

    total_db = sum(m["debito"]  for m in mov_banco)
    total_cr = sum(m["credito"] for m in mov_banco)

    return {
        "mov_banco":      mov_banco,
        "clasificados":   clasificados,
        "sin_clasificar": sin_clasificar,
        "top_egresos":    top_egresos,
        # Claves de compatibilidad con la interfaz web
        "conciliados": clasificados,
        "solo_banco":  sin_clasificar,
        "solo_conta":  [],
        "resumen": {
            "total_tx":             len(mov_banco),
            "total_clasificados":   len(clasificados),
            "total_sin_clasificar": len(sin_clasificar),
            "debitos_banco":        total_db,
            "creditos_banco":       total_cr,
            "saldo_neto":           total_cr - total_db,
            # Legacy keys
            "total_conciliados":    len(clasificados),
            "total_solo_banco":     len(sin_clasificar),
            "total_solo_conta":     0,
            "diferencia_debitos":   0,
            "diferencia_creditos":  0,
        },
        "nota": "Para conciliación completa necesitas importar tus registros contables de OSSADO.",
    }

# ─── Calendario DIAN real (calculado dinámicamente) ──────────────────────────

_MESES = ["", "Ene", "Feb", "Mar", "Abr", "May", "Jun",
          "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

def _calendario_dian_proximas() -> list[dict]:
    """Próximas obligaciones DIAN colombianas calculadas desde date.today()."""
    hoy = date.today()
    obligaciones: list[dict] = []

    # Retención en la fuente: mensual, vence día 12 del mes siguiente
    for delta in range(4):
        mes_venc = hoy.month + delta
        anio_venc = hoy.year + (mes_venc - 1) // 12
        mes_venc  = ((mes_venc - 1) % 12) + 1
        try:
            venc = date(anio_venc, mes_venc, 12)
        except ValueError:
            continue
        if venc < hoy:
            continue
        mes_per  = (mes_venc - 2) % 12 + 1
        anio_per = anio_venc if mes_venc > 1 else anio_venc - 1
        obligaciones.append({
            "obligacion": "Retención en la fuente",
            "periodo":    f"{_MESES[mes_per]} {anio_per}",
            "vencimiento": str(venc),
        })

    # IVA bimestral: vence día 12 del mes siguiente al bimestre
    # Bimestres: (mes_inicio, mes_fin, mes_vencimiento)
    for m1, m2, m_venc in [(1,2,3),(3,4,5),(5,6,7),(7,8,9),(9,10,11),(11,12,1)]:
        anio_venc = hoy.year + (1 if m_venc < m1 else 0)
        try:
            venc = date(anio_venc, m_venc, 12)
        except ValueError:
            continue
        if venc < hoy:
            continue
        anio_per = anio_venc - (1 if m_venc < m1 else 0)
        obligaciones.append({
            "obligacion": "IVA bimestral",
            "periodo":    f"{_MESES[m1]}-{_MESES[m2]} {anio_per}",
            "vencimiento": str(venc),
        })

    # ICA Barranquilla: trimestral, vence día 20 del mes siguiente
    for m_ini, m_fin, m_venc in [(1,3,4),(4,6,7),(7,9,10),(10,12,1)]:
        anio_venc = hoy.year + (1 if m_venc < m_ini else 0)
        try:
            venc = date(anio_venc, m_venc, 20)
        except ValueError:
            continue
        if venc < hoy:
            continue
        anio_per = anio_venc - (1 if m_venc < m_ini else 0)
        obligaciones.append({
            "obligacion": "ICA Barranquilla",
            "periodo":    f"T {_MESES[m_ini]}-{_MESES[m_fin]} {anio_per}",
            "vencimiento": str(venc),
        })

    obligaciones.sort(key=lambda o: o["vencimiento"])
    return obligaciones[:7]

# ─── Generación Excel ─────────────────────────────────────────────────────────

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
        ws["A1"] = "EXTRACTO BANCOLOMBIA — ANÁLISIS CONTABLE"
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

    mov_banco    = resultado.get("mov_banco", [])
    clasificados = resultado.get("clasificados", [])
    sin_clasi    = resultado.get("sin_clasificar", [])
    top_egresos  = resultado.get("top_egresos", [])
    res          = resultado["resumen"]

    # ── Hoja 1: Movimientos clasificados ─────────────────────────────────────
    ws1 = wb.active
    ws1.title = "Movimientos"
    escribir_encabezado(ws1, "MOVIMIENTOS CLASIFICADOS POR PUC",
                        f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    cols1 = ["Fecha", "Concepto", "Débito ($)", "Crédito ($)", "Saldo ($)", "Cuenta PUC", "Nombre PUC"]
    for j, c in enumerate(cols1, 1):
        cell = ws1.cell(row=5, column=j, value=c)
        cell.font  = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill  = hdr_fill(VERDE_OSC)
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = border_thin()
    ws1.row_dimensions[5].height = 28
    for i, m in enumerate(clasificados, 6):
        concepto = m["concepto"]
        vals = [m["fecha"], concepto, m["debito"] or None,
                m["credito"] or None, m.get("saldo") or None,
                m.get("cuenta_puc", ""), m.get("nombre_puc", "")]
        for j, v in enumerate(vals, 1):
            cell = ws1.cell(row=i, column=j, value=v)
            cell.font   = font(size=9)
            cell.border = border_thin()
            cell.fill   = hdr_fill(VERDE_CLA) if i % 2 == 0 else hdr_fill("FFFFFF")
            if j == 2:
                cell.alignment = Alignment(wrap_text=True, vertical="top")
            elif j in (3, 4, 5) and v:
                cell.number_format = "#,##0"
                cell.alignment = Alignment(horizontal="right", vertical="top")
            else:
                cell.alignment = Alignment(vertical="top")
        descripcion = str(ws1.cell(row=i, column=2).value or "")
        lineas = math.ceil(len(descripcion) / 45)
        ws1.row_dimensions[i].height = max(18, lineas * 15)
    widths1 = [12, 45, 14, 14, 14, 12, 28]
    for j, w in enumerate(widths1, 1): fmt_col(ws1, j, w)

    # ── Hoja 2: Sin clasificar ────────────────────────────────────────────────
    ws2 = wb.create_sheet("Sin Clasificar")
    escribir_encabezado(ws2, "MOVIMIENTOS SIN CUENTA PUC",
                        "Requieren revisión manual — asignar cuenta contable")
    ws2.merge_cells("A4:G4")
    nota = ws2.cell(row=4, column=1,
        value="⚠️  Para conciliación completa, importar registros contables de OSSADO")
    nota.font = Font(bold=True, color=ROJO_OSC, size=10, name="Calibri")
    nota.alignment = Alignment(horizontal="center")
    cols2 = ["Fecha", "Concepto", "Débito ($)", "Crédito ($)", "Saldo ($)", "Tipo", "Acción"]
    for j, c in enumerate(cols2, 1):
        cell = ws2.cell(row=5, column=j, value=c)
        cell.font = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill = hdr_fill(ROJO_OSC)
        cell.alignment = Alignment(horizontal="center")
        cell.border = border_thin()
    for i, m in enumerate(sin_clasi, 6):
        tipo = "Crédito" if m["credito"] > 0 else "Débito"
        vals = [m["fecha"], m["concepto"], m["debito"] or None,
                m["credito"] or None, m.get("saldo") or None,
                tipo, "Asignar cuenta PUC manualmente"]
        for j, v in enumerate(vals, 1):
            cell = ws2.cell(row=i, column=j, value=v)
            cell.font = font(size=9)
            cell.border = border_thin()
            cell.fill = hdr_fill(AMARILLO_CL)
            if j == 2:
                cell.alignment = Alignment(wrap_text=True, vertical="top")
            elif j in (3, 4, 5) and v:
                cell.number_format = "#,##0"
                cell.alignment = Alignment(horizontal="right", vertical="top")
            else:
                cell.alignment = Alignment(vertical="top")
        descripcion = str(ws2.cell(row=i, column=2).value or "")
        lineas = math.ceil(len(descripcion) / 45)
        ws2.row_dimensions[i].height = max(18, lineas * 15)
    widths2 = [12, 45, 14, 14, 14, 10, 35]
    for j, w in enumerate(widths2, 1): fmt_col(ws2, j, w)

    # ── Hoja 3: Top Egresos ───────────────────────────────────────────────────
    ws3 = wb.create_sheet("Top Egresos")
    escribir_encabezado(ws3, "TOP EGRESOS DEL PERÍODO",
                        f"Corte: {date.today().strftime('%d/%m/%Y')}")
    cols3 = ["Fecha", "Concepto", "Débito ($)", "Cuenta PUC", "Nombre PUC"]
    for j, c in enumerate(cols3, 1):
        cell = ws3.cell(row=5, column=j, value=c)
        cell.font = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill = hdr_fill(NEGRO)
        cell.alignment = Alignment(horizontal="center")
        cell.border = border_thin()
    for i, m in enumerate(top_egresos, 6):
        vals = [m["fecha"], m["concepto"], m["debito"],
                m.get("cuenta_puc", "—"), m.get("nombre_puc", "Sin clasificar")]
        for j, v in enumerate(vals, 1):
            cell = ws3.cell(row=i, column=j, value=v)
            cell.font = font(size=9)
            cell.border = border_thin()
            cell.fill = hdr_fill(ROJO_CLA) if i % 2 == 0 else hdr_fill("FFFFFF")
            if j == 2:
                cell.alignment = Alignment(wrap_text=True, vertical="top")
            elif j == 3:
                cell.number_format = "#,##0"
                cell.alignment = Alignment(horizontal="right", vertical="top")
            else:
                cell.alignment = Alignment(vertical="top")
        descripcion = str(ws3.cell(row=i, column=2).value or "")
        lineas = math.ceil(len(descripcion) / 45)
        ws3.row_dimensions[i].height = max(18, lineas * 15)
    widths3 = [12, 45, 14, 12, 28]
    for j, w in enumerate(widths3, 1): fmt_col(ws3, j, w)
    last3 = 5 + len(top_egresos)
    ws3.cell(row=last3+1, column=2, value="TOTAL EGRESOS:").font = font(bold=True)
    tc3 = ws3.cell(row=last3+1, column=3, value=res["debitos_banco"])
    tc3.number_format = "#,##0"
    tc3.font = Font(bold=True, size=10, color=ROJO_OSC, name="Calibri")
    tc3.alignment = Alignment(horizontal="right")

    # ── Hoja 4: Calendario DIAN ───────────────────────────────────────────────
    ws4 = wb.create_sheet("Calendario DIAN")
    escribir_encabezado(ws4, "PRÓXIMAS OBLIGACIONES FISCALES",
                        f"Calculado desde {date.today().strftime('%d/%m/%Y')} — verificar NIT en DIAN")
    cols4 = ["Obligación", "Período", "Fecha Vencimiento", "Días Restantes", "Estado"]
    for j, c in enumerate(cols4, 1):
        cell = ws4.cell(row=5, column=j, value=c)
        cell.font = Font(bold=True, color="FFFFFF", size=9, name="Calibri")
        cell.fill = hdr_fill(AZUL_OSC)
        cell.alignment = Alignment(horizontal="center")
        cell.border = border_thin()
    hoy = date.today()
    for i, ob in enumerate(_calendario_dian_proximas(), 6):
        venc = datetime.strptime(ob["vencimiento"], "%Y-%m-%d").date()
        dias = (venc - hoy).days
        color = ROJO_CLA if dias < 5 else (AMARILLO_CL if dias <= 15 else VERDE_CLA)
        vals = [ob["obligacion"], ob["periodo"], ob["vencimiento"], dias, "PENDIENTE"]
        for j, v in enumerate(vals, 1):
            cell = ws4.cell(row=i, column=j, value=v)
            cell.font = font(size=9)
            cell.border = border_thin()
            cell.fill = hdr_fill(color)
            if j in (3, 4):
                cell.alignment = Alignment(horizontal="center")
    widths4 = [28, 20, 18, 15, 12]
    for j, w in enumerate(widths4, 1): fmt_col(ws4, j, w)
    # Nota al pie
    last4 = 5 + len(_calendario_dian_proximas())
    nota4 = ws4.cell(row=last4+2, column=1,
        value="* Fechas basadas en calendario general colombiano. Confirmar según dígito verificador del NIT.")
    nota4.font = Font(italic=True, size=8, color="888888", name="Calibri")
    ws4.merge_cells(f"A{last4+2}:E{last4+2}")

    # ── Hoja 5: Resumen Ejecutivo ─────────────────────────────────────────────
    ws5 = wb.create_sheet("Resumen Ejecutivo")
    ws5.sheet_view.showGridLines = False
    escribir_encabezado(ws5, "RESUMEN EJECUTIVO",
                        f"Metrika Group | {date.today().strftime('%d/%m/%Y')}")
    metricas = [
        ("", ""),
        ("MOVIMIENTOS DEL EXTRACTO", ""),
        ("Total transacciones",          res["total_tx"]),
        ("Clasificados con PUC",         res["total_clasificados"]),
        ("Sin clasificar",               res["total_sin_clasificar"]),
        ("Total débitos",                res["debitos_banco"]),
        ("Total créditos",               res["creditos_banco"]),
        ("Saldo neto del período",       res["saldo_neto"]),
        ("", ""),
        ("NOTA", "Para conciliación completa importar registros de OSSADO"),
    ]
    for i, (etiqueta, valor) in enumerate(metricas, 5):
        cell_e = ws5.cell(row=i, column=2, value=etiqueta)
        cell_v = ws5.cell(row=i, column=4, value=valor if valor != "" else "")
        if etiqueta in ("MOVIMIENTOS DEL EXTRACTO",):
            cell_e.font = Font(bold=True, color="FFFFFF", size=10, name="Calibri")
            cell_e.fill = hdr_fill(NEGRO)
            ws5.merge_cells(f"B{i}:E{i}")
            ws5.row_dimensions[i].height = 22
        elif etiqueta == "NOTA":
            cell_e.font = Font(bold=True, color=AZUL_OSC, size=9, name="Calibri")
            cell_e.fill = hdr_fill(AMARILLO_CL)
            cell_v.font = Font(italic=True, color=AZUL_OSC, size=9, name="Calibri")
            cell_v.fill = hdr_fill(AMARILLO_CL)
            cell_e.border = border_thin()
            cell_v.border = border_thin()
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

# ─── Claude + Twilio ──────────────────────────────────────────────────────────

def consultar_agente(mensaje: str, contexto_extra: str = "") -> str:
    prompt = f"{SYSTEM_PROMPT}\n\n"
    if contexto_extra:
        prompt += f"{contexto_extra}\n\n"
    prompt += f"Usuario: {mensaje}"
    try:
        response = gemini.generate_content(prompt)
        return response.text
    except Exception as e:
        log.error(f"[consultar_agente] Gemini error — {type(e).__name__}: {e}", exc_info=True)
        return f"❌ Error al consultar el asistente: {type(e).__name__}: {e}"

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
    size = os.path.getsize(path)
    if size == 0:
        raise RuntimeError(f"El archivo {filename} se escribió vacío en disco")
    log.info(f"Excel guardado en disco: {path} — {size:,} bytes")
    raw_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "localhost:8000")
    if raw_domain.startswith("http"):
        base = raw_domain.rstrip("/")
    else:
        base = f"https://{raw_domain.rstrip('/')}"
    url = f"{base}/descargar/{filename}"
    log.info(f"URL de descarga generada: {url}")
    return url

# ─── Endpoints HTTP ───────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "agente": "Auxiliar Contable Metrika Group", "version": "1.0"}

@app.get("/descargar/{filename}")
def descargar_archivo(filename: str):
    if ".." in filename or "/" in filename:
        raise HTTPException(400, "Nombre de archivo inválido")
    path = f"/tmp/{filename}"
    if not os.path.exists(path):
        log.error(f"Archivo no encontrado en disco: {path}")
        raise HTTPException(404, f"Archivo no encontrado: {filename}")
    size = os.path.getsize(path)
    log.info(f"Sirviendo archivo: {path} — {size:,} bytes")
    return FileResponse(
        path=path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.post("/procesar-pdf")
async def endpoint_procesar_pdf(pdf: UploadFile = File(...)):
    try:
        pdf_bytes = await pdf.read()
        log.info(f"PDF recibido por web — {len(pdf_bytes):,} bytes, filename: {pdf.filename}")
        mov_banco = await parsear_extracto_bancolombia(pdf_bytes)
        if not mov_banco:
            raise HTTPException(422, "No se pudieron extraer movimientos del PDF")
        resultado = conciliar(mov_banco)
        excel_bytes = generar_excel_conciliacion(resultado)
        filename = f"Conciliacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        url_excel = subir_excel_twilio(excel_bytes, filename)
        movimientos_banco = [
            {
                "id": f"t{i+1}",
                "fecha": m["fecha"],
                "descripcion": m["concepto"],
                "doc": m.get("doc", ""),
                "valor": m["credito"] if m["credito"] > 0 else m["debito"],
                "tipo": "CR" if m["credito"] > 0 else "DB",
                "cuenta_puc": m.get("cuenta_puc"),
                "aprobado": False,
            }
            for i, m in enumerate(resultado["mov_banco"])
        ]
        return JSONResponse({
            "status": "ok",
            "movimientos": len(mov_banco),
            "resumen": resultado["resumen"],
            "excel_url": url_excel,
            "movimientos_banco": movimientos_banco,
            "conciliados": resultado["conciliados"],
            "solo_banco":  resultado["solo_banco"],
            "solo_conta":  resultado["solo_conta"],
            "nota":        resultado["nota"],
        })
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error en /procesar-pdf: {e}", exc_info=True)
        raise HTTPException(500, f"Error procesando PDF: {str(e)}")

# ─── Webhook WhatsApp ─────────────────────────────────────────────────────────

_MSG_SIN_PDF = "⚠️ Primero envíame el PDF de tu extracto bancario para comenzar."

@app.post("/enviar-resumen-wa")
async def enviar_resumen_wa(request: Request):
    """Envía el resumen del reporte ejecutivo al número WhatsApp indicado."""
    body = await request.json()
    numero = str(body.get("numero", "")).replace("whatsapp:", "").strip()
    if not numero:
        raise HTTPException(400, "El campo 'numero' es requerido (ej. +573001234567)")

    resumen  = body.get("resumen", {})
    anomalias = body.get("anomalias")

    total_ingresos = resumen.get("totalIngresos", 0)
    total_egresos  = resumen.get("totalEgresos", 0)
    balance        = resumen.get("balanceNeto", total_ingresos - total_egresos)
    n_tx           = resumen.get("nTx", 0)
    periodo        = resumen.get("periodo", "")
    titular        = resumen.get("titular", "")

    lineas_anomalias = ""
    if anomalias:
        total_a  = anomalias.get("total_alertas", 0)
        criticas = anomalias.get("criticas", 0)
        if total_a > 0:
            lineas_anomalias = f"\n⚠️ {total_a} anomalía{'s' if total_a != 1 else ''} detectada{'s' if total_a != 1 else ''} ({criticas} crítica{'s' if criticas != 1 else ''})"
        else:
            lineas_anomalias = "\n✅ Sin anomalías detectadas"

    msg = (
        f"📊 *Reporte Ejecutivo CONTA*\n"
        f"{f'Titular: {titular}' + chr(10) if titular else ''}"
        f"Período: {periodo}\n\n"
        f"💚 Ingresos: {fmt_cop(total_ingresos)}\n"
        f"🔴 Egresos:  {fmt_cop(total_egresos)}\n"
        f"📈 Balance:  {fmt_cop(balance)}\n"
        f"📋 {n_tx} transacciones analizadas"
        f"{lineas_anomalias}\n\n"
        f"_Generado por CONTA · Metrika Group_"
    )

    try:
        enviar_whatsapp(numero, msg)
        log.info(f"Resumen WA enviado a {numero}")
        return {"status": "ok", "mensaje": "Reporte enviado por WhatsApp"}
    except Exception as e:
        log.error(f"Error enviando resumen WA a {numero}: {e}")
        raise HTTPException(500, f"Error al enviar WhatsApp: {str(e)}")


@app.post("/webhook/whatsapp")
async def webhook_whatsapp(
    background_tasks: BackgroundTasks,
    From: str = Form(...),
    Body: str = Form(default=""),
    NumMedia: str = Form(default="0"),
    MediaUrl0: str = Form(default=""),
    MediaContentType0: str = Form(default=""),
):
    limpiar_sesiones_expiradas()
    numero  = From.replace("whatsapp:", "")
    mensaje = Body.strip().lower()
    log.info(f"WA {numero}: '{Body[:80]}'")

    # ── PDF adjunto → procesar y guardar sesión ──────────────────────────────
    if int(NumMedia) > 0 and "pdf" in MediaContentType0.lower():
        background_tasks.add_task(procesar_extracto_pdf, numero, MediaUrl0)
        enviar_whatsapp(numero,
            "📄 Recibí tu extracto. Procesando... en unos segundos te envío el resumen.")
        return {"status": "procesando"}

    sesion: SesionExtracto | None = SESIONES.get(numero)

    # ── Comando: clasificar ──────────────────────────────────────────────────
    if "clasificar" in mensaje:
        if not sesion:
            enviar_whatsapp(numero, _MSG_SIN_PDF)
            return {"status": "ok"}
        top10 = sesion.resumen["top10"][:10]
        lineas = []
        for m in top10:
            val  = m["credito"] if m["credito"] > 0 else -m["debito"]
            puc  = m.get("cuenta_puc") or "—"
            signo = "✅" if m["credito"] > 0 else "🔴"
            lineas.append(f"{signo} {m['fecha']} | {m['concepto'][:30]} | {fmt_cop(abs(val))} | PUC {puc}")
        explicacion = (
            "ℹ️ El comando *clasificar* muestra las 10 transacciones de mayor valor "
            "con su cuenta PUC asignada. Esto te ayuda a verificar la categorización "
            "contable de tus movimientos bancarios.\n\n"
        )
        msg = explicacion + "📊 *Top 10 Transacciones con PUC*\n\n" + "\n".join(lineas)
        enviar_whatsapp(numero, msg)
        return {"status": "ok"}

    # ── Comando: saldo ───────────────────────────────────────────────────────
    if mensaje in ("saldo", "saldo actual", "mi saldo", "ver saldo"):
        if not sesion:
            enviar_whatsapp(numero, _MSG_SIN_PDF)
            return {"status": "ok"}
        r = sesion.resumen
        msg = (
            f"💰 *Saldo del extracto*\n\n"
            f"💚 Ingresos: {fmt_cop(r['ingresos'])}\n"
            f"🔴 Egresos:  {fmt_cop(r['egresos'])}\n"
            f"💰 Saldo neto: {fmt_cop(r['saldo'])}\n\n"
            f"📋 {r['total_tx']} transacciones en el período"
        )
        enviar_whatsapp(numero, msg)
        return {"status": "ok"}

    # ── Comando: pagos pendientes ────────────────────────────────────────────
    if any(k in mensaje for k in ["pagos", "pendientes", "vencimientos", "proveedores"]):
        if not sesion:
            enviar_whatsapp(numero, _MSG_SIN_PDF)
            return {"status": "ok"}
        background_tasks.add_task(reporte_pagos_pendientes, numero)
        return {"status": "ok"}

    # ── Comando: impuestos / DIAN ────────────────────────────────────────────
    if any(k in mensaje for k in ["impuestos", "fiscal", "tributario", "dian", "obligaciones"]):
        background_tasks.add_task(reporte_fiscal, numero)
        return {"status": "ok"}

    # ── Saludo / ayuda ───────────────────────────────────────────────────────
    if any(k in mensaje for k in ["ayuda", "help", "hola", "buenas", "buenos", "menu"]):
        tiene_extracto = "✅ Extracto cargado" if sesion else "📄 Sin extracto — envía el PDF para comenzar"
        enviar_whatsapp(numero,
            f"¡Hola! Soy *CONTA*, tu Auxiliar Contable de Metrika Group.\n\n"
            f"Estado: {tiene_extracto}\n\n"
            f"Comandos disponibles:\n"
            f"- Envía el *PDF* del extracto bancario\n"
            f"- *clasificar* → top 10 con cuenta PUC\n"
            f"- *saldo* → resumen del período\n"
            f"- *pagos pendientes* → principales egresos\n"
            f"- *impuestos* → calendario DIAN\n"
            f"- Cualquier otra pregunta contable")
        return {"status": "ok"}

    # ── Consulta libre → Claude con contexto de sesión ───────────────────────
    background_tasks.add_task(responder_consulta_libre, numero, Body, sesion)
    return {"status": "ok"}

# ─── Tareas en background ─────────────────────────────────────────────────────

async def procesar_extracto_pdf(numero: str, media_url: str):
    try:
        log.info(f"Descargando PDF — URL: {media_url}")
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(
                media_url,
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                follow_redirects=True,
                timeout=30.0,
            )
            if resp.status_code in (301, 302, 307, 308):
                resp = await client.get(
                    str(resp.headers.get("location", media_url)),
                    follow_redirects=True,
                    timeout=30.0,
                )
            if resp.status_code != 200:
                log.error(f"Error HTTP al descargar PDF: {resp.status_code}")
                enviar_whatsapp(numero, f"❌ No pude descargar el archivo (HTTP {resp.status_code}). Intenta de nuevo.")
                return

        pdf_bytes = resp.content
        log.info(f"PDF descargado: {len(pdf_bytes):,} bytes")

        mov_banco = await parsear_extracto_bancolombia(pdf_bytes)
        if not mov_banco:
            enviar_whatsapp(numero, "❌ No pude leer los movimientos del PDF. Asegúrate de que sea un extracto bancario en formato texto.")
            return

        clasificados = clasificar_con_reglas_locales(mov_banco)
        resumen = computar_resumen_wa(clasificados)

        SESIONES[numero] = SesionExtracto(
            mov_banco=mov_banco,
            resumen=resumen,
            clasificados=clasificados,
        )
        log.info(f"Sesión guardada para {numero} — {len(mov_banco)} movimientos")

        clasificados_n  = sum(1 for m in clasificados if m.get("cuenta_puc"))
        sin_clasificar_n = len(clasificados) - clasificados_n

        enviar_whatsapp(numero,
            f"✅ *Extracto procesado*\n\n"
            f"📊 Transacciones: {resumen['total_tx']}\n"
            f"💚 Ingresos: {fmt_cop(resumen['ingresos'])}\n"
            f"🔴 Egresos: {fmt_cop(resumen['egresos'])}\n"
            f"💰 Saldo: {fmt_cop(resumen['saldo'])}\n"
            f"🏷️ Clasificados PUC: {clasificados_n} | Sin clasificar: {sin_clasificar_n}\n\n"
            f"Escribe *clasificar* para ver el top 10 con cuenta PUC\n"
            f"Escribe *saldo* para ver el resumen en cualquier momento"
        )

        try:
            resultado_conc = conciliar(mov_banco)
            excel_bytes = generar_excel_conciliacion(resultado_conc)
            filename = f"Conciliacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            url_excel = subir_excel_twilio(excel_bytes, filename)
            try:
                enviar_whatsapp_media(numero, "📎 Aquí tu Excel de análisis contable:", url_excel)
            except Exception:
                enviar_whatsapp(numero, f"📎 Descarga el Excel aquí:\n{url_excel}")
        except Exception as e:
            log.warning(f"No se pudo generar Excel: {e}")

    except Exception as e:
        log.error(f"Error en procesar_extracto_pdf: {e}", exc_info=True)
        enviar_whatsapp(numero, "❌ Ocurrió un error procesando el extracto. Intenta de nuevo.")


def reporte_pagos_pendientes(numero: str):
    sesion = SESIONES.get(numero)
    if not sesion:
        enviar_whatsapp(numero, _MSG_SIN_PDF)
        return

    agrupado: dict[str, dict] = defaultdict(lambda: {"n": 0, "total": 0.0, "ultimo": ""})
    for m in sesion.clasificados:
        if m["debito"] > 0:
            clave = m["concepto"][:40].strip().upper()
            agrupado[clave]["n"] += 1
            agrupado[clave]["total"] += m["debito"]
            if m["fecha"] > agrupado[clave]["ultimo"]:
                agrupado[clave]["ultimo"] = m["fecha"]

    top = sorted(agrupado.items(), key=lambda x: x[1]["total"], reverse=True)[:8]

    if not top:
        enviar_whatsapp(numero, "ℹ️ No se encontraron egresos en el extracto cargado.")
        return

    msg = "💸 *Principales Egresos del Período*\n\n"
    for concepto, d in top:
        msg += f"• {concepto[:35]}\n  {fmt_cop(d['total'])} ({d['n']} mov) — último: {d['ultimo']}\n\n"

    msg += f"_Total egresos período: {fmt_cop(sesion.resumen['egresos'])}_\n\n"
    msg += "ℹ️ Para ver facturas y vencimientos pendientes, sincroniza con OSSADO."
    enviar_whatsapp(numero, msg)


def reporte_fiscal(numero: str):
    obligaciones = _calendario_dian_proximas()
    hoy = date.today()
    msg = f"📅 *Próximas Obligaciones Fiscales*\n_{hoy.strftime('%d/%m/%Y')}_\n\n"
    for ob in obligaciones:
        venc = datetime.strptime(ob["vencimiento"], "%Y-%m-%d").date()
        dias = (venc - hoy).days
        icono = "🔴" if dias < 5 else ("⚠️" if dias <= 15 else "✅")
        msg += f"{icono} *{ob['obligacion']}*\nPeríodo: {ob['periodo']}\nVence: {ob['vencimiento']} ({dias} días)\n\n"
    msg += "_Verifica las fechas exactas según tu NIT en el calendario DIAN._"
    enviar_whatsapp(numero, msg)


def responder_consulta_libre(numero: str, mensaje: str, sesion: "SesionExtracto | None" = None):
    contexto = ""
    if sesion:
        r = sesion.resumen
        top5 = sesion.resumen["top10"][:5]
        lineas_top = "\n".join(
            f"  {m['fecha']} | {m['concepto'][:35]} | "
            f"{fmt_cop(m['credito'] if m['credito'] > 0 else m['debito'])} | "
            f"{'Abono' if m['credito'] > 0 else 'Cargo'} | PUC {m.get('cuenta_puc') or '—'}"
            for m in top5
        )
        dist_lineas = "\n".join(
            f"  PUC {cod} {v['nombre']}: {v['n']} mov · {fmt_cop(v['total'])}"
            for cod, v in list(r["por_cuenta"].items())[:6]
        )
        contexto = (
            f"EXTRACTO BANCARIO CARGADO ({r['total_tx']} transacciones):\n"
            f"- Ingresos: {fmt_cop(r['ingresos'])}\n"
            f"- Egresos:  {fmt_cop(r['egresos'])}\n"
            f"- Saldo neto: {fmt_cop(r['saldo'])}\n\n"
            f"TOP 5 POR VALOR:\n{lineas_top}\n\n"
            f"DISTRIBUCIÓN PUC:\n{dist_lineas}"
        )
    respuesta = consultar_agente(mensaje, contexto)
    enviar_whatsapp(numero, respuesta)
