"""
cy_text_to_columns.py
---------------------
Script equivalente a text_to_columns.py pero diseñado para el formato
de Rate Confirmations de Coyote Logistics (fuente CY).

El texto de CY tiene una estructura completamente distinta al de UTB:
  - Encabezado con "Rate Confirmation Load XXXXXXXX"
  - Secciones "Stop N: Pick Up / Delivery" numeradas
  - Bloque "Charges" con "Total USD $XXX.XX"
  - Bloque "Agreement" con datos del broker y carrier
  - Fechas en formato "Wed 03/29/2023" con ventanas "from HH:MM - HH:MM"
    o citas puntuales "at HH:MM"

Uso:
    python cy_text_to_columns.py --source_path <ruta> --target_table <tabla>

Parámetros:
    --source_path   Ruta DBFS/Volume con archivos .txt generados desde PDFs de CY
    --target_table  Tabla Delta destino (ej: logistics.bronze.truckr_loads)
"""

import argparse
import re
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# ── Schema de salida (idéntico al de text_to_columns.py) ───────────────────────
OUTPUT_SCHEMA = StructType([
    StructField("source_file",               StringType(),  True),
    StructField("broker_name",               StringType(),  True),
    StructField("broker_phone",              StringType(),  True),
    StructField("broker_fax",                StringType(),  True),
    StructField("broker_address",            StringType(),  True),
    StructField("broker_city",               StringType(),  True),
    StructField("broker_state",              StringType(),  True),
    StructField("broker_zipcode",            StringType(),  True),
    StructField("broker_email",              StringType(),  True),
    StructField("loadConfirmationNumber",    LongType(),    True),
    StructField("totalCarrierPay",           DoubleType(),  True),
    StructField("carrier_name",              StringType(),  True),
    StructField("carrier_mc",                LongType(),    True),
    StructField("carrier_address",           StringType(),  True),
    StructField("carrier_city",              StringType(),  True),
    StructField("carrier_state",             StringType(),  True),
    StructField("carrier_zipcode",           StringType(),  True),
    StructField("carrier_phone",             StringType(),  True),
    StructField("carrier_fax",               StringType(),  True),
    StructField("carrier_contact",           StringType(),  True),
    StructField("pickup_customer_1",         StringType(),  True),
    StructField("pickup_customer_2",         StringType(),  True),
    StructField("pickup_customer_3",         StringType(),  True),
    StructField("pickup_address_1",          StringType(),  True),
    StructField("pickup_address_2",          StringType(),  True),
    StructField("pickup_address_3",          StringType(),  True),
    StructField("pickup_city_1",             StringType(),  True),
    StructField("pickup_city_2",             StringType(),  True),
    StructField("pickup_city_3",             StringType(),  True),
    StructField("pickup_state_1",            StringType(),  True),
    StructField("pickup_state_2",            StringType(),  True),
    StructField("pickup_state_3",            StringType(),  True),
    StructField("pickup_zipcode_1",          StringType(),  True),
    StructField("pickup_zipcode_2",          StringType(),  True),
    StructField("pickup_zipcode_3",          StringType(),  True),
    StructField("pickup_start_datetime_1",   StringType(),  True),
    StructField("pickup_start_datetime_2",   StringType(),  True),
    StructField("pickup_start_datetime_3",   StringType(),  True),
    StructField("pickup_end_datetime_1",     StringType(),  True),
    StructField("pickup_end_datetime_2",     StringType(),  True),
    StructField("pickup_end_datetime_3",     StringType(),  True),
    StructField("delivery_customer_1",       StringType(),  True),
    StructField("delivery_customer_2",       StringType(),  True),
    StructField("delivery_customer_3",       StringType(),  True),
    StructField("delivery_address_1",        StringType(),  True),
    StructField("delivery_address_2",        StringType(),  True),
    StructField("delivery_address_3",        StringType(),  True),
    StructField("delivery_city_1",           StringType(),  True),
    StructField("delivery_city_2",           StringType(),  True),
    StructField("delivery_city_3",           StringType(),  True),
    StructField("delivery_state_1",          StringType(),  True),
    StructField("delivery_state_2",          StringType(),  True),
    StructField("delivery_state_3",          StringType(),  True),
    StructField("delivery_zipcode_1",        StringType(),  True),
    StructField("delivery_zipcode_2",        StringType(),  True),
    StructField("delivery_zipcode_3",        StringType(),  True),
    StructField("delivery_start_datetime_1", StringType(),  True),
    StructField("delivery_start_datetime_2", StringType(),  True),
    StructField("delivery_start_datetime_3", StringType(),  True),
    StructField("delivery_end_datetime_1",   StringType(),  True),
    StructField("delivery_end_datetime_2",   StringType(),  True),
    StructField("delivery_end_datetime_3",   StringType(),  True),
    StructField("processed_at",              StringType(),  True),
])


# ── Helpers ─────────────────────────────────────────────────────────────────────

def _first(pattern, text, flags=re.IGNORECASE | re.DOTALL):
    """Retorna el primer grupo capturado, o el match completo si no hay grupos, o None."""
    m = re.search(pattern, text, flags)
    if not m:
        return None
    return m.group(1).strip() if m.lastindex else m.group(0).strip()


def _get_stop_block(text, stop_num, next_stop_num=None):
    """
    Extrae el bloque de texto de un stop numerado de Coyote.
    Ejemplo: Stop 1: Pick Up ... hasta Stop 2: Delivery
    """
    if next_stop_num:
        pat = rf'Stop {stop_num}:.*?(?=Stop {next_stop_num}:|\Z)'
    else:
        pat = rf'Stop {stop_num}:.*?(?=###\s+Charges|\Z)'
    m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
    return m.group(0) if m else ''


def _extract_facility(block):
    """
    Extrae el nombre de la instalación del bloque de un stop.
    Ignora la palabra 'Notes' que puede aparecer justo después de 'Facility'.
    """
    m = re.search(r'Facility\s+(?!Notes\b)([^\n]+)', block, re.IGNORECASE)
    return m.group(1).strip() if m else None


def _parse_coyote_address(raw):
    """
    Parsea el bloque de dirección de Coyote, que puede venir en formatos:
      "CALLE\n[CALLE2]\nCiudad, STATE\nZIP"
      "CALLE\nCiudad, STATE ZIP"
    Retorna: (street, city, state, zipcode)
    """
    if not raw:
        return None, None, None, None

    lines = [l.strip() for l in raw.strip().splitlines() if l.strip()]
    city, state, zipcode = None, None, None
    street_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # "Ciudad, STATE ZIP" — todo en una sola línea
        m1 = re.match(r'^(.+),\s+([A-Z]{2})\s+([\d\-]+)$', line)
        if m1:
            city, state, zipcode = m1.group(1).strip(), m1.group(2), m1.group(3)
            i += 1
            continue
        # "Ciudad, STATE" y la siguiente línea es el ZIP
        m2 = re.match(r'^(.+),\s+([A-Z]{2})$', line)
        if m2 and i + 1 < len(lines) and re.match(r'^[\d\-]+$', lines[i + 1]):
            city, state = m2.group(1).strip(), m2.group(2)
            zipcode = lines[i + 1]
            i += 2
            continue
        street_lines.append(line)
        i += 1

    street = ' '.join(street_lines) if street_lines else None
    return street, city, state, zipcode


def _extract_address_from_block(block):
    """Extrae el bloque crudo de dirección de un stop."""
    raw = _first(
        r'Address\s+(.*?)(?:\n\n\n?Contact|\n\n\n?Phone|\n\n\n?Scheduled|\n\n\n?Appointment)',
        block
    )
    return _parse_coyote_address(raw)


def _to_iso(date_str, time_str):
    """
    Convierte 'Wed 03/29/2023' + '08:00' → '2023-03-29T08:00:00'
    Acepta también formato 'Thu 03/30/2023' y time 'at HH:MM' o 'from HH:MM'.
    """
    if not date_str:
        return None
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if not m:
        return None
    month, day, year = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
    time_part = '00:00:00'
    if time_str:
        t = re.search(r'(\d{1,2}:\d{2})', time_str)
        if t:
            time_part = t.group(1) + ':00'
    return f"{year}-{month}-{day}T{time_part}"


def _padded(lst, n=3):
    """Rellena la lista hasta longitud n con None."""
    return (lst + [None] * n)[:n]


# ── Parser principal del TXT de CY ──────────────────────────────────────────────

def parse_cy_txt(file_path: str, content: str) -> dict:
    """
    Parsea un archivo de texto de Rate Confirmation de Coyote Logistics
    y retorna un dict con todas las columnas del schema unificado.
    """
    rec = {
        "source_file":  file_path,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── Broker (Coyote) ──────────────────────────────────────────────────────
    rec["broker_name"]    = _first(r'Broker\s+(Coyote[^\n]+)', content) or "Coyote Logistics, LLC"
    rec["broker_email"]   = _first(r'(CarrierInvoices@[^\s\n]+)', content)
    rec["broker_phone"]   = _first(r'\(877[^)]+\)', content)           # "(877-626-9683)"
    rec["broker_fax"]     = _first(r'Fax:\s*\+?1?\s*([\d\s\(\)\-\.]+)', content)
    # Dirección del broker: "960 Northpoint Parkway, Suite 150, Alpharetta, GA 30005"
    broker_addr_raw = _first(r'\*\*(960[^\*]+)\*\*\s*\n\*\*([^\*]+)\*\*\s*\n\*\*([^\*]+)\*\*', content)
    if broker_addr_raw:
        rec["broker_address"] = broker_addr_raw
    else:
        rec["broker_address"] = _first(r'960 Northpoint Parkway[^\n]*', content)
    # Ciudad/state/zip del broker: Alpharetta, GA 30005
    broker_loc = _first(r'(Alpharetta,\s*GA\s*\d+)', content)
    if broker_loc:
        parts = broker_loc.split(',')
        rec["broker_city"]    = parts[0].strip()
        rest  = parts[1].strip().split()
        rec["broker_state"]   = rest[0] if rest else None
        rec["broker_zipcode"] = rest[1] if len(rest) > 1 else None
    else:
        rec["broker_city"] = rec["broker_state"] = rec["broker_zipcode"] = None

    rec["carrier_contact"] = _first(r'Rep\s+([^\n]+)', content)

    # ── Load ─────────────────────────────────────────────────────────────────
    load_num = _first(r'Load\s+(\d{6,})', content)
    try:
        rec["loadConfirmationNumber"] = int(load_num) if load_num else None
    except (ValueError, TypeError):
        rec["loadConfirmationNumber"] = None

    total_raw = _first(r'Total USD\s+\$([\d,\.]+)', content)
    try:
        rec["totalCarrierPay"] = float(total_raw.replace(',', '')) if total_raw else None
    except (ValueError, TypeError):
        rec["totalCarrierPay"] = None

    # ── Carrier ──────────────────────────────────────────────────────────────
    # El nombre del carrier aparece en la sección "Agreement" y en el pie de página
    rec["carrier_name"]  = _first(r'Carrier\s*\n\n+([^\n]+)', content)
    usdot = _first(r'\[Carrier USDOT\s*-\s*(\d+)\]', content) or _first(r'USDOT\s+(\d+)', content)
    # CY usa USDOT, no MC — guardamos en carrier_mc como referencia
    try:
        rec["carrier_mc"] = int(usdot) if usdot else None
    except (ValueError, TypeError):
        rec["carrier_mc"] = None
    rec["carrier_phone"]   = None   # Coyote no incluye teléfono del carrier en el doc
    rec["carrier_fax"]     = None
    rec["carrier_address"] = None
    rec["carrier_city"]    = None
    rec["carrier_state"]   = None
    rec["carrier_zipcode"] = None

    # ── Detectar número total de stops ───────────────────────────────────────
    stop_nums = [int(m) for m in re.findall(r'Stop\s+(\d+):', content, re.IGNORECASE)]
    stop_nums = sorted(set(stop_nums))

    pickups    = []
    deliveries = []

    for i, snum in enumerate(stop_nums):
        next_snum = stop_nums[i + 1] if i + 1 < len(stop_nums) else None
        block = _get_stop_block(content, snum, next_snum)

        is_pickup   = bool(re.search(r'Stop \d+:\s*Pick\s*Up', block, re.IGNORECASE))
        is_delivery = bool(re.search(r'Stop \d+:\s*Delivery', block, re.IGNORECASE))

        facility            = _extract_facility(block)
        street, city, state, zipcode = _extract_address_from_block(block)

        # Fecha y ventana horaria
        date_str  = _first(r'Scheduled For\s*\n+([^\n]+)', block)
        # Ventana: "from HH:MM - HH:MM"
        t_start   = _first(r'from\s+([\d:]+)\s*-', block)
        t_end     = _first(r'from\s+[\d:]+\s*-\s*([\d:]+)', block)
        # Cita puntual: "at HH:MM"
        t_appt    = _first(r'\bat\s+([\d:]+)', block)

        start_dt  = _to_iso(date_str, t_start or t_appt)
        end_dt    = _to_iso(date_str, t_end)   # None si es cita puntual

        stop_data = {
            "customer":       facility,
            "address":        street,
            "city":           city,
            "state":          state,
            "zipcode":        zipcode,
            "start_datetime": start_dt,
            "end_datetime":   end_dt,
        }

        if is_pickup:
            pickups.append(stop_data)
        elif is_delivery:
            deliveries.append(stop_data)

    # ── Asignar pickups al record (hasta 3) ──────────────────────────────────
    pickups    = _padded(pickups,    3)
    deliveries = _padded(deliveries, 3)

    for i, p in enumerate(pickups, 1):
        if p:
            rec[f"pickup_customer_{i}"]       = p["customer"]
            rec[f"pickup_address_{i}"]        = p["address"]
            rec[f"pickup_city_{i}"]           = p["city"]
            rec[f"pickup_state_{i}"]          = p["state"]
            rec[f"pickup_zipcode_{i}"]        = p["zipcode"]
            rec[f"pickup_start_datetime_{i}"] = p["start_datetime"]
            rec[f"pickup_end_datetime_{i}"]   = p["end_datetime"]
        else:
            rec[f"pickup_customer_{i}"]       = None
            rec[f"pickup_address_{i}"]        = None
            rec[f"pickup_city_{i}"]           = None
            rec[f"pickup_state_{i}"]          = None
            rec[f"pickup_zipcode_{i}"]        = None
            rec[f"pickup_start_datetime_{i}"] = None
            rec[f"pickup_end_datetime_{i}"]   = None

    for i, d in enumerate(deliveries, 1):
        if d:
            rec[f"delivery_customer_{i}"]       = d["customer"]
            rec[f"delivery_address_{i}"]        = d["address"]
            rec[f"delivery_city_{i}"]           = d["city"]
            rec[f"delivery_state_{i}"]          = d["state"]
            rec[f"delivery_zipcode_{i}"]        = d["zipcode"]
            rec[f"delivery_start_datetime_{i}"] = d["start_datetime"]
            rec[f"delivery_end_datetime_{i}"]   = d["end_datetime"]
        else:
            rec[f"delivery_customer_{i}"]       = None
            rec[f"delivery_address_{i}"]        = None
            rec[f"delivery_city_{i}"]           = None
            rec[f"delivery_state_{i}"]          = None
            rec[f"delivery_zipcode_{i}"]        = None
            rec[f"delivery_start_datetime_{i}"] = None
            rec[f"delivery_end_datetime_{i}"]   = None

    return rec


# ── Escritura Delta (MERGE) ──────────────────────────────────────────────────────

def _write_delta(df, target_table: str):
    spark = df.sparkSession
    tmp_view = "tmp_cy_loads"
    df.createOrReplaceTempView(tmp_view)

    if not spark.catalog.tableExists(target_table):
        print(f"[DELTA] Tabla {target_table} no existe — creando...")
        df.write.format("delta").saveAsTable(target_table)
        print(f"[DELTA] Tabla creada con {df.count()} filas.")
        return

    update_set  = ", ".join([f"target.{f.name} = source.{f.name}" for f in OUTPUT_SCHEMA.fields])
    insert_cols = ", ".join([f.name for f in OUTPUT_SCHEMA.fields])
    insert_vals = ", ".join([f"source.{f.name}" for f in OUTPUT_SCHEMA.fields])

    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING {tmp_view} AS source
        ON target.loadConfirmationNumber = source.loadConfirmationNumber
           AND target.source_file = source.source_file
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    print(f"[DELTA] MERGE completado en {target_table}.")


# ── Proceso principal ────────────────────────────────────────────────────────────

def process_cy_txt(spark, source_path: str, target_table: str):
    print(f"[CY] Leyendo TXTs de Coyote desde: {source_path}")

    files = (spark.sparkContext
             .wholeTextFiles(source_path + "/*.txt")
             .collect())

    print(f"[CY] {len(files)} archivos encontrados")

    records = []
    for file_path, content in files:
        try:
            rec = parse_cy_txt(file_path, content)
            records.append(rec)
            print(f"[CY] OK → Load {rec.get('loadConfirmationNumber')} | "
                  f"Pay ${rec.get('totalCarrierPay')} | "
                  f"Pickup: {rec.get('pickup_city_1')}, {rec.get('pickup_state_1')} → "
                  f"Delivery: {rec.get('delivery_city_1')}, {rec.get('delivery_state_1')}")
        except Exception as e:
            print(f"[CY] ERROR en {file_path}: {e}")

    if not records:
        print("[CY] No se procesó ningún archivo.")
        return

    from pyspark.sql import Row
    rows = [Row(**{f.name: rec.get(f.name) for f in OUTPUT_SCHEMA.fields}) for rec in records]
    df_out = spark.createDataFrame(rows, schema=OUTPUT_SCHEMA)

    print(f"[CY] {df_out.count()} filas listas → {target_table}")
    _write_delta(df_out, target_table)


# ── Punto de entrada ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Convierte TXT de Coyote Logistics (CY) al schema Delta de cargas"
    )
    parser.add_argument("--source_path",  required=True,
                        help="Ruta DBFS/Volume con archivos .txt de CY")
    parser.add_argument("--target_table", required=True,
                        help="Tabla Delta destino (catalog.schema.table)")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("cy_text_to_columns")
             .getOrCreate())
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    process_cy_txt(spark, args.source_path.rstrip("/"), args.target_table)


if __name__ == "__main__":
    main()
