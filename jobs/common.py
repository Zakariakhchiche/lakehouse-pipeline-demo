"""Fonctions partagées par les jobs, sans dépendance à Spark (testables en pur Python)."""
from __future__ import annotations

import re
import unicodedata

# Source : jeu ouvert SNCF « Régularité mensuelle TGV par liaison » (AQST).
DATASET_URL = (
    "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/"
    "regularite-mensuelle-tgv-aqst/exports/csv?delimiter=%3B"
)

# Colonnes utiles, après normalisation des en-têtes.
REQUIRED = {
    "date": "month",
    "gare_de_depart": "departure_station",
    "gare_d_arrivee": "arrival_station",
    "nombre_de_circulations_prevues": "planned_trains",
    "nombre_de_trains_annules": "cancelled_trains",
    "nombre_de_trains_en_retard_a_l_arrivee": "late_trains",
    "retard_moyen_de_tous_les_trains_a_l_arrivee": "avg_delay_min",
}


def normalize_column(name: str) -> str:
    """'Gare d'arrivée' -> 'gare_d_arrivee' : sans accents, en snake_case."""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")


def rename_map(columns: list[str]) -> dict[str, str]:
    """Associe les colonnes brutes du CSV aux noms cibles ; échoue si une colonne manque."""
    normalized = {normalize_column(c): c for c in columns}
    missing = [k for k in REQUIRED if k not in normalized]
    if missing:
        raise ValueError(f"Colonnes absentes du fichier source : {missing}")
    return {normalized[k]: v for k, v in REQUIRED.items()}


def punctuality_rate(planned: int, cancelled: int, late: int) -> float | None:
    """Part des trains partis qui arrivent à l'heure, en %."""
    ran = planned - cancelled
    if ran <= 0:
        return None
    return round(100 * (ran - late) / ran, 2)
