from fastapi import FastAPI, File, UploadFile
import os
import requests
import pandas as pd
import json
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()
hubspot_token = os.getenv("HUBSPOT_TOKEN")

VALID_TYPES = ["string", "enumeration", "number", "bool", "datetime", "date", "phone_number"]
VALID_FIELDTYPES = ["text", "textarea", "select", "radio", "checkbox", "number", "booleancheckbox",
                    "date", "phonenumber", "file", "html"]

def validar_e_limpar_propriedade(row, index):
    """Valida e limpa cada propriedade antes de enviar"""
    erros = []
    
    type_raw = str(row.get("type", "")).strip()
    type_clean = type_raw.split()[0] if type_raw else ""

    if type_clean not in VALID_TYPES:
        erros.append(f"Linha {index}: type inválido '{type_raw}' (deve ser um de {VALID_TYPES})")
        type_clean = "string"
    
    fieldtype = str(row.get("fieldType", "")).strip()
    if fieldtype not in VALID_FIELDTYPES:
        erros.append(f"Linha {index}: fieldType inválido '{fieldtype}' (deve ser um de {VALID_FIELDTYPES})")
        fieldtype = "text" 
    
    prop = {
        "label": str(row.get("label", "")).strip(),
        "name": str(row.get("name", "")).strip(),
        "type": type_clean,
        "fieldType": fieldtype,
        "groupName": str(row.get("groupName", "contactinformation")).strip()
    }

    if type_clean == "enumeration" and row.get("options"):
        try:
            options_str = str(row["options"]).strip()
            if options_str:
                prop["options"] = json.loads(options_str)
        except json.JSONDecodeError:
            erros.append(f"Linha {index}: options com JSON inválido")
    
    return prop, erros

@app.post("/createProperty/")
async def upload(file: UploadFile = File(...)):
    contents = await file.read()
    df = pd.read_csv(BytesIO(contents), sep=',', dtype=str, keep_default_na=False, encoding='utf-8')

    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    inputs = []
    todos_erros = []
    
    for idx, row in df.iterrows():
        prop, erros = validar_e_limpar_propriedade(row, idx + 2)
        
        if erros:
            todos_erros.extend(erros)
        
        print(f"Linha {idx + 2}: type={prop['type']}, fieldType={prop['fieldType']}, name={prop['name']}")
        
        inputs.append(prop)
    
    if todos_erros:
        print("⚠️ ERROS ENCONTRADOS:")
        for erro in todos_erros:
            print(f"  - {erro}")
    
    body = {"inputs": inputs}
    
    print("\n📤 Enviando para HubSpot (primeiros 3):")
    print(json.dumps({"inputs": inputs[:3]}, indent=2, ensure_ascii=False))
    
    url = "https://api.hubapi.com/crm/v3/properties/contact/batch/create"
    headers = {"Authorization": f"Bearer {hubspot_token}", "Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json=body)

    return {
        "validacao_erros": todos_erros,
        "hubspot_status_code": response.status_code,
        "hubspot_response": response.json()
    }
