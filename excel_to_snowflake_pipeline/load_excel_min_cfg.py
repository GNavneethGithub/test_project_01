import yaml, sys, copy

REQUIRED_WS_KEYS = ["name", "required_columns"]

def _validate_and_apply_defaults(cfg: dict) -> dict:
    if "excel" not in cfg or "worksheets" not in cfg["excel"]:
        raise ValueError("Missing: excel.worksheets")

    dflt = {
        "timezone": cfg["excel"].get("timezone", "UTC"),
        "date_formats": cfg["excel"].get("date_formats", ["YYYY-MM-DDTHH:MM:SSZ","DD/MM/YYYY HH:MM:SS","MM/DD/YYYY HH:MM:SS"]),
        "header_row": int(cfg["excel"].get("header_row", 1)),
        "skip_rows": int(cfg["excel"].get("skip_rows", 0)),
        "pdf_link_column": cfg["excel"].get("pdf_link_column", "servicenow_link"),
    }

    ws_out = []
    for ws in cfg["excel"]["worksheets"] or []:
        for k in REQUIRED_WS_KEYS:
            if k not in ws or not ws[k]:
                raise ValueError(f"Worksheet missing required key: {k}")

        name = str(ws["name"]).strip()
        req_cols = list(ws["required_columns"])
        pdf_col = dflt["pdf_link_column"]

        # hash_columns default: required_columns minus pdf_link_column (if present)
        hash_cols = ws.get("hash_columns")
        if not hash_cols or len(hash_cols) == 0:
            hash_cols = [c for c in req_cols if c != pdf_col]

        # sanity: ensure all referenced columns exist in required_columns
        missing_in_required = [c for c in hash_cols if c not in req_cols]
        if missing_in_required:
            raise ValueError(f"{name}: hash_columns not in required_columns: {missing_in_required}")

        ws_out.append({
            "name": name,
            "required_columns": req_cols,
            "hash_columns": list(hash_cols),
        })

    return {"defaults": dflt, "worksheets": ws_out}

def _to_flat(valid: dict) -> dict:
    flat = {}
    # defaults
    flat["TIMEZONE"] = valid["defaults"]["timezone"]
    flat["DATE_FORMATS"] = ",".join(valid["defaults"]["date_formats"])
    flat["HEADER_ROW"] = valid["defaults"]["header_row"]
    flat["SKIP_ROWS"] = valid["defaults"]["skip_rows"]
    flat["PDF_LINK_COLUMN"] = valid["defaults"]["pdf_link_column"]

    # worksheet list
    names = [w["name"] for w in valid["worksheets"]]
    flat["WORKSHEETS"] = ",".join(names)

    # per worksheet keys
    for w in valid["worksheets"]:
        prefix = f"WS_{w['name']}".upper()
        flat[f"{prefix}_REQUIRED_COLUMNS"] = ",".join(w["required_columns"])
        flat[f"{prefix}_HASH_COLUMNS"] = ",".join(w["hash_columns"])
    return flat

def load_excel_min_cfg(yaml_path: str) -> dict:
    with open(yaml_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    valid = _validate_and_apply_defaults(raw)
    return _to_flat(valid)

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    cfg = load_excel_min_cfg(path)
    print(cfg)
