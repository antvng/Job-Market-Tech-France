import re
import pytest


# Copie standalone de parse_salaire_ft depuis glue_jobs/transform_jobs.py
def parse_salaire_ft(libelle):
    if not libelle:
        return None, None, None
    nombres = re.findall(r"[\d]+(?:[.,]\d+)?", libelle.replace(" ", ""))
    nombres = [float(n.replace(",", ".")) for n in nombres]
    if len(nombres) < 2:
        return None, None, None
    salaire_min = nombres[0]
    salaire_max = nombres[1]
    libelle_lower = libelle.lower()
    if "annuel" in libelle_lower or "an" in libelle_lower:
        periode = "annuel"
    elif "mensuel" in libelle_lower or "mois" in libelle_lower:
        periode = "mensuel"
        salaire_min *= 12
        salaire_max *= 12
    elif "horaire" in libelle_lower or "heure" in libelle_lower:
        periode = "horaire"
        salaire_min *= 35 * 52
        salaire_max *= 35 * 52
    else:
        periode = "annuel"
    return int(salaire_min), int(salaire_max), periode


# Copie standalone de normalize_contrat depuis glue_jobs/transform_jobs.py
def normalize_contrat(contrat, source):
    if not contrat:
        return None
    contrat_lower = contrat.lower()
    if source == "france-travail":
        # FT utilise des codes 3 lettres : CDI, CDD, MIS (intérim), STA (stage), ALT (alternance)
        mapping = {"cdi": "CDI", "cdd": "CDD", "mis": "Intérim", "fre": "Freelance", "sta": "Stage", "alt": "Alternance"}
        return mapping.get(contrat_lower[:3], contrat)
    elif source == "wttj":
        # WTTJ utilise des labels anglais complets
        mapping = {"full_time": "CDI", "part_time": "CDD", "internship": "Stage", "apprenticeship": "Alternance", "freelance": "Freelance", "temporary": "Intérim"}
        return mapping.get(contrat_lower, contrat)
    return contrat

# On teste parse_salaire_ft

def test_salaire_annuel():
    # Cas simple : libellé annuel, les montants doivent être extraits sans transformation
    mn, mx, p = parse_salaire_ft("Annuel de 40000 à 50000 EUR")
    assert mn == 40000
    assert mx == 50000
    assert p == "annuel"


def test_salaire_mensuel_converti():
    # Cas critique : 3000€/mois doit devenir 36000€/an
    mn, mx, p = parse_salaire_ft("Mensuel de 3000 à 3500 EUR")
    assert mn == 36000  # 3000 * 12
    assert mx == 42000  # 3500 * 12
    assert p == "mensuel"


def test_salaire_horaire_converti():
    # 20€/h → annuel : 20 * 35h * 52 semaines = 36400€
    mn, mx, p = parse_salaire_ft("Horaire de 20 à 25 EUR")
    assert mn == 20 * 35 * 52
    assert mx == 25 * 35 * 52
    assert p == "horaire"


def test_salaire_vide():
    # SI pas de salairek, on retourne NONE
    assert parse_salaire_ft("") == (None, None, None)
    assert parse_salaire_ft(None) == (None, None, None)


def test_salaire_sans_chiffres():
    # Libellé présent mais non parsablen doit retourner none
    assert parse_salaire_ft("Salaire selon profil") == (None, None, None)

# FT envoie "CDI", "MIS" (intérim), "STA" (stage)
# WTTJ envoie "full_time", "internship"
# Check normalisation

def test_normalize_contrat_ft():
    # "MIS" = Mission = Intérim, pas évident sans la doc FT
    assert normalize_contrat("CDI", "france-travail") == "CDI"
    assert normalize_contrat("MIS", "france-travail") == "Intérim"
    assert normalize_contrat("STA", "france-travail") == "Stage"


def test_normalize_contrat_wttj():
    # "full_time" doit devenir "CDI" pour matcher avec les offres FT dans les filtres
    assert normalize_contrat("full_time", "wttj") == "CDI"
    assert normalize_contrat("internship", "wttj") == "Stage"
    assert normalize_contrat("apprenticeship", "wttj") == "Alternance"
