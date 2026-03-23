import json
from pathlib import Path

def load_schema_registry():
    path = Path(__file__).parent / "schema_registry.json"
    
    with open(path) as f:
        return json.load(f)