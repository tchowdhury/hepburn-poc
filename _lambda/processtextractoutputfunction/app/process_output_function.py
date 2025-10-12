"""
Convert raw Textract JSON (from S3) to output format with KV + line items.
"""

import json
import logging
import os
import boto3
from datetime import datetime
import re
from typing import Dict, Any, List, Tuple, Optional
import random
import string

logger = logging.getLogger(__name__)

# --------------------------- Helpers ---------------------------

_CURR_RE = re.compile(r"[^\d,.\-]")

def _to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = _CURR_RE.sub("", s.strip())
    if s.count(",") == 1 and s.count(".") == 0:
        s = s.replace(",", ".")
    s = re.sub(r"(?<=\d),(?=\d{3}\b)", "", s)
    try: return float(s)
    except: return None

def _to_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = re.sub(r"[^\d\-]", "", s)
    try: return int(s)
    except:
        try: return int(float(s))
        except: return None

def _pct(c: Optional[float]) -> str:
    return f"{c:.1f}%" if isinstance(c, (int, float)) else ""

# --------------------------- QUERIES parsing ---------------------------

def _collect_query_answers(blocks: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Returns: { alias: {"text": "...", "confidence": 99.9} }
    """
    id_map = {b["Id"]: b for b in blocks if "Id" in b}
    out: Dict[str, Dict[str, Any]] = {}

    for b in blocks:
        if b.get("BlockType") != "QUERY":
            continue
        #print(f"DEBUG: Processing QUERY block: {b}")
        alias = b.get("Query", {}).get("Alias") or b.get("Query", {}).get("Text") or "unknown"
        vals, confs = [], []
        for rel in b.get("Relationships") or []:
        #for rel in b.get("Relationships", []):
            if rel.get("Type") == "ANSWER":
                for ans_id in rel.get("Ids", []):
                    ans = id_map.get(ans_id, {})
                    if ans.get("BlockType") == "QUERY_RESULT":
                        t = (ans.get("Text") or "").strip()
                        if t: vals.append(t)
                        c = ans.get("Confidence")
                        if isinstance(c, (int, float)): confs.append(float(c))
        if vals:
            out[alias] = {"text": " ".join(vals), "confidence": max(confs) if confs else None}
    return out

# --------------------------- TABLE parsing ---------------------------

def _extract_tables_with_rows(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Returns: [ { "headers": [...], "rows": [ {header: {"text":str,"confidence":float}}, ...] }, ... ]
    """
    id_map = {b["Id"]: b for b in blocks if "Id" in b}

    def cell_text_conf(cell_block) -> Tuple[str, Optional[float]]:
        texts, confs = [], []
        for rel in cell_block.get("Relationships") or []:
        #for rel in cell_block.get("Relationships", []):
            if rel.get("Type") == "CHILD":
                for cid in rel.get("Ids", []):
                    ch = id_map.get(cid, {})
                    bt = ch.get("BlockType")
                    if bt == "WORD":
                        t = ch.get("Text")
                        if t: texts.append(t)
                        if "Confidence" in ch: confs.append(float(ch["Confidence"]))
                    elif bt == "SELECTION_ELEMENT":
                        txt = "[X]" if ch.get("SelectionStatus") == "SELECTED" else "[ ]"
                        texts.append(txt)
                        if "Confidence" in ch: confs.append(float(ch["Confidence"]))
        avg = (sum(confs)/len(confs)) if confs else cell_block.get("Confidence")
        return " ".join(texts).strip(), (float(avg) if avg is not None else None)

    tables = []
    for table in [b for b in blocks if b.get("BlockType") == "TABLE"]:
        cell_ids = []
        for rel in table.get("Relationships", []):
            if rel.get("Type") == "CHILD":
                cell_ids.extend(rel.get("Ids", []))
        cells = [id_map[cid] for cid in cell_ids if id_map.get(cid, {}).get("BlockType") == "CELL"]

        grid: Dict[int, Dict[int, Dict[str, Any]]] = {}
        header_rows = set()
        max_r = max_c = 0

        for cell in cells:
            r, c = cell.get("RowIndex", 0), cell.get("ColumnIndex", 0)
            rspan, cspan = cell.get("RowSpan", 1), cell.get("ColumnSpan", 1)
            txt, conf = cell_text_conf(cell)
            if "EntityTypes" in cell and cell["EntityTypes"] is not None and "COLUMN_HEADER" in cell["EntityTypes"]:
                header_rows.add(r)
            for rr in range(r, r + rspan):
                for cc in range(c, c + cspan):
                    grid.setdefault(rr, {})[cc] = {"text": txt, "conf": conf}
            max_r = max(max_r, r + rspan - 1)
            max_c = max(max_c, c + cspan - 1)

        if not header_rows and max_r >= 1:
            header_rows = {1}

        headers, header_confs = [], []
        for cc in range(1, max_c + 1):
            parts, confs = [], []
            for rr in sorted(header_rows):
                cell = grid.get(rr, {}).get(cc, {"text": "", "conf": None})
                if cell["text"]: parts.append(cell["text"])
                if cell["conf"] is not None: confs.append(cell["conf"])
            h = " ".join(parts).strip() if parts else f"col_{cc}"
            headers.append(h)
            header_confs.append((sum(confs)/len(confs)) if confs else None)

        dict_rows = []
        for rr in range(1, max_r + 1):
            if rr in header_rows: continue
            row_any = False
            row_dict: Dict[str, Dict[str, Any]] = {}
            for cc in range(1, max_c + 1):
                cell = grid.get(rr, {}).get(cc, {"text": "", "conf": None})
                txt, conf = cell["text"], cell["conf"]
                if txt: row_any = True
                row_dict[headers[cc-1]] = {"text": txt, "confidence": conf}
            if row_any:
                dict_rows.append(row_dict)

        tables.append({"headers": headers, "rows": dict_rows})

    return tables

# --------------------------- Items mapping ---------------------------

def _map_line_items(table: Dict[str, Any]) -> List[Dict[str, Any]]:
    headers = table["headers"]
    lower = [h.lower() for h in headers]

    def find_col(names: Tuple[str, ...]) -> Optional[int]:
        n = tuple(x.lower() for x in names)
        for i, h in enumerate(lower):
            if any(k in h for k in n):
                return i
        return None

    idx_id   = find_col(("id", "item code", "sku", "product code", "item no"))
    idx_desc = find_col(("description of supply", "description", "item", "details", "product"))
    idx_qty  = find_col(("qty", "quantity", "q'ty"))
    idx_amt  = find_col(("amount", "total", "line total", "price"))
    idx_exc  = find_col(("ex gst", "excluding gst", "excl gst", "amount ex gst", "net"))
    idx_gst  = find_col(("gst", "tax"))
    idx_inc  = find_col(("inc gst", "including gst", "incl gst", "amount inc gst", "gross"))

    items = []

    for r in table["rows"]:
        def get_cell(idx):
            if idx is None or idx < 0 or idx >= len(headers):
                return "", None
            cell = r.get(headers[idx], {})
            return (cell.get("text") or "").strip(), cell.get("confidence")

        id_t, id_c     = get_cell(idx_id)
        desc_t, desc_c = get_cell(idx_desc)
        qty_t, qty_c   = get_cell(idx_qty)
        amt_t, amt_c   = get_cell(idx_amt)
        ex_t, ex_c     = get_cell(idx_exc)
        gst_t, gst_c   = get_cell(idx_gst)
        inc_t, inc_c   = get_cell(idx_inc)

        if not (id_t or desc_t or qty_t or amt_t or ex_t or gst_t or inc_t):
            continue

        item = {
            "id": id_t or None,
            "description": desc_t or "",
            "quantity": _to_int(qty_t) if qty_t else None,
            "amount": _to_float(amt_t) if amt_t else None,
            "amount_ex_gst": _to_float(ex_t) if ex_t else None,
            "amount_gst": _to_float(gst_t) if gst_t else None,
            "amount_inc_gst": _to_float(inc_t) if inc_t else None,
            "_confidence": _pct(max([c for c in [id_c, desc_c, qty_c, amt_c, ex_c, gst_c, inc_c] if c is not None])) if any([id_c, desc_c, qty_c, amt_c, ex_c, gst_c, inc_c]) else ""
        }
        items.append(item)

    return items

# --------------------------- Get the document ref from file name ---------------------------
def get_prefix_or_random(newFile_name):
    if "-" in newFile_name:
        return newFile_name.split("-", 1)[0]
    else:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=10))

s3 = boto3.client('s3')
paginator = s3.get_paginator("list_objects_v2")
file_keys = []

def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    
    body_dict = event.get("Payload", {})
    s3_path = body_dict.get("s3_path", "")
    source_bucket = body_dict.get("source_bucket", "")
    source_key = body_dict.get("source_key", "")
    s3_bucket = body_dict.get("s3_bucket", "")
    new_file_name = body_dict.get("newFileName", "")
    classification = body_dict.get("classification", "")
    

    try:
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=f"raw/{classification}/{new_file_name}/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Skip "folder" placeholders and .s3_access_check
                if key.endswith("/") or key.endswith(".s3_access_check") or key.split("/")[-1] == ".s3_access_check":
                    continue
                file_keys.append(key)

        
        # Loop through each part file (page) and construct the final output
        for file_key in file_keys:
            print(f"Processing file: s3://{s3_bucket}/{file_key}")
            obj = s3.get_object(Bucket=s3_bucket, Key=file_key)
            raw_data = json.loads(obj['Body'].read().decode('utf-8'))
        

            blocks = raw_data.get("Blocks", [])
            if not blocks:
                raise ValueError("No Blocks found in the Textract JSON data")
        
            # 1) QUERIES → KV map
            q = _collect_query_answers(blocks)
            kv_out: Dict[str, Dict[str, Any]] = {}
            for alias, ans in q.items():
                raw = ans.get("text", "")
                conf = ans.get("confidence")
                key_l = alias.lower()
                if key_l in {"amount_inc_gst", "amount_ex_gst", "amount_gst"}:
                    val = _to_float(raw)
                else:
                    val = raw
                kv_out[alias] = {"value": val, "confidence": _pct(conf)}

            #print(kv_out)
            # 2) TABLES → line items (pick a likely items table)
            tables = _extract_tables_with_rows(blocks)
            candidate = None
            for t in tables:
                hdrs = " ".join(h.lower() for h in t["headers"])
                if any(k in hdrs for k in ["description", "item", "product"]) and any(k in hdrs for k in ["amount", "price", "total", "gst"]):
                    candidate = t
                    break
            if candidate is None and tables:
                candidate = tables[0]

            line_items = _map_line_items(candidate) if candidate else []

            out = dict(kv_out)
            out["documentRef"] = get_prefix_or_random(new_file_name)
            out["lineitems"] = line_items

            # Save the output JSON back to S3
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            new_s3_key = f"processed/{classification}/{new_file_name}/output_{timestamp}.json"
            mime_type = "application/json"

            s3.put_object(
                Bucket=s3_bucket,
                Key=new_s3_key,
                Body=json.dumps(out),
                ContentType=mime_type
            )
            logger.info(f"Saved processed output to s3://{s3_bucket}/{new_s3_key}") 
        
        return {
            "statusCode": 200,
            "s3_path": s3_path,
            "source_bucket": source_bucket,
            "source_key": source_key,
            "s3_bucket": s3_bucket,
            "newFileName": new_file_name,
            "classification": classification
        }

    except Exception as e:
        logger.error(e)
        raise e