import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "jobs"))
from common import normalize_column, punctuality_rate, rename_map  # noqa: E402

RAW_HEADERS = [
    "Date", "Service", "Gare de départ", "Gare d'arrivée", "Durée moyenne du trajet",
    "Nombre de circulations prévues", "Nombre de trains annulés",
    "Nombre de trains en retard à l'arrivée", "Retard moyen de tous les trains à l'arrivée",
]


def test_normalize_column():
    assert normalize_column("Gare d'arrivée") == "gare_d_arrivee"
    assert normalize_column("  Nombre de trains annulés ") == "nombre_de_trains_annules"


def test_rename_map_on_real_headers():
    m = rename_map(RAW_HEADERS)
    assert m["Gare d'arrivée"] == "arrival_station"
    assert m["Retard moyen de tous les trains à l'arrivée"] == "avg_delay_min"
    assert "Service" not in m


def test_rename_map_fails_loudly_on_schema_change():
    with pytest.raises(ValueError, match="gare_de_depart"):
        rename_map([h for h in RAW_HEADERS if h != "Gare de départ"])


@pytest.mark.parametrize("planned,cancelled,late,expected", [
    (100, 0, 10, 90.0),
    (100, 20, 8, 90.0),
    (50, 50, 0, None),
])
def test_punctuality_rate(planned, cancelled, late, expected):
    assert punctuality_rate(planned, cancelled, late) == expected
