import json
import os

SETTINGS_FILE = "view_settings.json"

DEFAULT_SETTINGS = {
    # Default structure for a view
    # "view_name": {
    #     "height": 600,
    #     "columns": { "col_name": width_int, ... }
    # }
}

def load_settings():
    if not os.path.exists(SETTINGS_FILE):
        return {}
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_settings(settings):
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Error saving settings: {e}")

def get_view_settings(view_name):
    settings = load_settings()
    return settings.get(view_name, {"height": 600, "columns": {}})

def update_view_settings(view_name, height, column_widths):
    settings = load_settings()
    settings[view_name] = {
        "height": height,
        "columns": column_widths
    }
    save_settings(settings)
