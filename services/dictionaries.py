import os
from typing import Dict, List, Tuple

import pandas as pd

key_buttons_1rang: Dict[str, int] = {}
key_buttons_text: Dict[str, int] = {}
key_buttons_termins: List[str] = []
key_all_opros: List[str] = []


def _load_structure() -> pd.DataFrame:
    return pd.read_excel(os.path.abspath("structure.xlsx"), engine="openpyxl")


def update_dictionaries() -> None:
    global key_buttons_1rang, key_buttons_text, key_buttons_termins, key_all_opros

    structure_f = _load_structure()
    level, marker, text_messege, buttns = structure_f[structure_f["Уровень"] == 1].iloc[0]
    buttns = [btn.strip() for btn in buttns.strip("[]").split("]\n[")]

    key_buttons_1rang = {btn: 2 for btn in buttns}
    key_buttons_text = structure_f.set_index("Маркер")["Уровень"].to_dict()

    first_marker = next(iter(key_buttons_1rang), None)
    if first_marker:
        termins_raw = structure_f[structure_f["Маркер"] == first_marker]["Кнопки"].iloc[0]
        key_buttons_termins = [btn.strip() for btn in termins_raw.strip("[]").split("]\n[")]
    else:
        key_buttons_termins = []

    key_all_opros = list(key_buttons_1rang.keys()) + key_buttons_termins


async def read_table(type_z: int, message_text: str = "") -> Tuple:
    structure_f = _load_structure()
    if type_z == 1:
        return structure_f[structure_f["Уровень"] == 1].iloc[0]
    if type_z == 2:
        return structure_f[
            (structure_f["Уровень"] == 2) & (structure_f["Маркер"] == message_text)
        ].iloc[0]
    if type_z == 3:
        return structure_f[structure_f["Маркер"] == "Задать свой вопрос о ЦФА"].iloc[0]
    if type_z == 4:
        return structure_f[structure_f["Маркер"] == message_text].iloc[0]
    if type_z == 5:
        filtered = structure_f[~structure_f["Маркер"].isin(key_all_opros)]
        return filtered[filtered["Маркер"] == message_text].iloc[0]
    raise ValueError(f"Unknown table type: {type_z}")


def get_key_buttons_1rang() -> Dict[str, int]:
    return key_buttons_1rang


def get_key_buttons_text() -> Dict[str, int]:
    return key_buttons_text


def get_key_buttons_termins() -> List[str]:
    return key_buttons_termins


def get_key_all_opros() -> List[str]:
    return key_all_opros

