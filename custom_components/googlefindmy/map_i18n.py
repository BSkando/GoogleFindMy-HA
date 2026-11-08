# custom_components/googlefindmy/map_i18n.py
"""Localization catalog for the self-rendered Leaflet map view.

Home Assistant loads ``translations/*.json`` only for entity, config and
service strings into the frontend. The map is a self-built aiohttp HTML page
(``map_view._generate_map_html``) that never receives those translations, so it
needs its own catalog. This module is that dedicated catalog: it is resolved
server-side from ``hass.config.language`` with an English fallback.

It is deliberately kept **out** of ``strings.json`` / ``translations/`` so it
cannot trip the hassfest schema check (a non-standard top-level section there is
a CI risk). See the plan decision A1.

Leaf module: standard library only, with no imports from ``map_view`` or
``coordinator``, so it can be imported from either without an import cycle.

Catalog invariants (enforced by ``tests/test_map_i18n.py``):
- every locale carries the exact same key set as ``en``;
- every value is a non-empty string;
- ``en`` is complete and is the fallback source.

Attribute-safety note: the copy-tooltip values are injected into single-quoted
HTML attributes in the client JS (mirroring the pre-existing English literals),
so no catalog value may contain a single quote ``'``. Keep translations quote
free; the ``en`` reference values already are. Values are also interpolated into
HTML text nodes, so ``<``, ``>``, ``&`` and ``"`` are out as well; both rules are
pinned by ``tests/test_map_i18n.py``. French and Italian wordings are phrased
around the apostrophe on purpose (``cette integration`` rather than
``l'integration``).
"""

from __future__ import annotations

# Languages that render right-to-left. Only ``he`` (Hebrew) is shipped today.
RTL_LANGUAGES: frozenset[str] = frozenset({"he"})

# Full label catalog: locale code -> {label key -> translated string}.
#
# The 18 inventoried source strings map to 21 catalog keys, because the plural
# string ``Showing N points`` splits into ``showing_points_one`` /
# ``showing_points_other`` and the two dual-value strings (own/crowdsourced,
# copy plus-code/coordinates) carry two keys each. ``{n}`` in the plural forms
# is substituted by ``format_showing`` via ``str.replace`` (never ``.format``),
# so literal braces need no escaping.
#
# ``assets_unavailable_title`` / ``assets_unavailable_body`` belong to the
# vendored-asset failure page (``map_view._html_response``), not to the map
# itself: without them a non-English install would drop to an English page
# exactly when something is already wrong.
MAP_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "unknown_device": "Unknown Device",
        "location_history": "Location History",
        "start_time": "Start Time",
        "end_time": "End Time",
        "min_accuracy": "Min Accuracy (meters)",
        "apply_filters": "Apply Filters",
        "filters_summary": "Filters",
        "showing_points_one": "Showing {n} point",
        "showing_points_other": "Showing {n} points",
        "time": "Time:",
        "accuracy": "Accuracy:",
        "source": "Source:",
        "own_device": "Own Device",
        "crowdsourced": "Crowdsourced",
        "location": "Location:",
        "coordinates": "Coordinates:",
        "copy_to_clipboard": "Copy to clipboard",
        "copy_plus_code": "Copy Plus Code to clipboard",
        "copy_coordinates": "Copy coordinates to clipboard",
        "assets_unavailable_title": "Map unavailable",
        "assets_unavailable_body": (
            "The map assets shipped with this integration could not be read. "
            "Reinstall the integration and reload Home Assistant."
        ),
    },
    "de": {
        "unknown_device": "Unbekanntes Gerät",
        "location_history": "Standortverlauf",
        "start_time": "Startzeit",
        "end_time": "Endzeit",
        "min_accuracy": "Mindestgenauigkeit (Meter)",
        "apply_filters": "Filter anwenden",
        "filters_summary": "Filter",
        "showing_points_one": "{n} Punkt angezeigt",
        "showing_points_other": "{n} Punkte angezeigt",
        "time": "Zeit:",
        "accuracy": "Genauigkeit:",
        "source": "Quelle:",
        "own_device": "Eigenes Gerät",
        "crowdsourced": "Crowdsourcing-Netzwerk",
        "location": "Ort:",
        "coordinates": "Koordinaten:",
        "copy_to_clipboard": "In die Zwischenablage kopieren",
        "copy_plus_code": "Plus Code in die Zwischenablage kopieren",
        "copy_coordinates": "Koordinaten in die Zwischenablage kopieren",
        "assets_unavailable_title": "Karte nicht verfügbar",
        "assets_unavailable_body": (
            "Die mit dieser Integration ausgelieferten Kartendateien konnten nicht "
            "gelesen werden. Installiere die Integration neu und lade Home Assistant neu."
        ),
    },
    "es": {
        "unknown_device": "Dispositivo desconocido",
        "location_history": "Historial de ubicaciones",  # codespell:ignore historial
        "start_time": "Hora de inicio",
        "end_time": "Hora de fin",
        "min_accuracy": "Precisión mínima (metros)",
        "apply_filters": "Aplicar filtros",
        "filters_summary": "Filtros",
        "showing_points_one": "Mostrando {n} punto",
        "showing_points_other": "Mostrando {n} puntos",
        "time": "Hora:",
        "accuracy": "Precisión:",
        "source": "Fuente:",
        "own_device": "Dispositivo propio",
        "crowdsourced": "Red colaborativa",
        "location": "Ubicación:",
        "coordinates": "Coordenadas:",
        "copy_to_clipboard": "Copiar al portapapeles",
        "copy_plus_code": "Copiar Plus Code al portapapeles",
        "copy_coordinates": "Copiar coordenadas al portapapeles",
        "assets_unavailable_title": "Mapa no disponible",
        "assets_unavailable_body": (
            "No se pudieron leer los archivos del mapa incluidos en esta integración. "
            "Reinstala esta integración y recarga Home Assistant."
        ),
    },
    "fr": {
        "unknown_device": "Appareil inconnu",  # codespell:ignore appareil
        "location_history": "Historique de localisation",
        "start_time": "Heure de début",
        "end_time": "Heure de fin",
        "min_accuracy": "Précision min. (mètres)",
        "apply_filters": "Appliquer les filtres",
        "filters_summary": "Filtres",
        "showing_points_one": "{n} point affiché",
        "showing_points_other": "{n} points affichés",
        "time": "Heure :",
        "accuracy": "Précision :",
        "source": "Source :",
        "own_device": "Appareil personnel",  # codespell:ignore appareil
        "crowdsourced": "Réseau participatif",
        "location": "Lieu :",
        "coordinates": "Coordonnées :",
        "copy_to_clipboard": "Copier dans le presse-papiers",
        "copy_plus_code": "Copier le Plus Code dans le presse-papiers",
        "copy_coordinates": "Copier les coordonnées dans le presse-papiers",
        "assets_unavailable_title": "Carte indisponible",
        "assets_unavailable_body": (
            "Impossible de lire les fichiers de carte fournis avec cette intégration. "
            "Réinstallez cette intégration puis rechargez Home Assistant."
        ),
    },
    "he": {
        "unknown_device": "מכשיר לא ידוע",
        "location_history": "היסטוריית מיקומים",
        "start_time": "שעת התחלה",
        "end_time": "שעת סיום",
        "min_accuracy": "דיוק מזערי (מטרים)",
        "apply_filters": "החל מסננים",
        "filters_summary": "מסננים",
        "showing_points_one": "מציג {n} נקודה",
        "showing_points_other": "מציג {n} נקודות",
        "time": "זמן:",
        "accuracy": "דיוק:",
        "source": "מקור:",
        "own_device": "מכשיר שלי",
        "crowdsourced": "רשת שיתופית",
        "location": "מיקום:",
        "coordinates": "קואורדינטות:",
        "copy_to_clipboard": "העתק ללוח",
        "copy_plus_code": "העתק Plus Code ללוח",
        "copy_coordinates": "העתק קואורדינטות ללוח",
        "assets_unavailable_title": "המפה אינה זמינה",
        "assets_unavailable_body": (
            "לא ניתן לקרוא את קובצי המפה המצורפים לשילוב הזה. "
            "התקן מחדש את השילוב וטען מחדש את Home Assistant."
        ),
    },
    "it": {
        "unknown_device": "Dispositivo sconosciuto",
        "location_history": "Cronologia posizioni",
        "start_time": "Ora di inizio",
        "end_time": "Ora di fine",
        "min_accuracy": "Precisione minima (metri)",
        "apply_filters": "Applica filtri",
        "filters_summary": "Filtri",
        "showing_points_one": "{n} punto mostrato",
        "showing_points_other": "{n} punti mostrati",
        "time": "Ora:",
        "accuracy": "Precisione:",
        "source": "Sorgente:",
        "own_device": "Dispositivo proprio",
        "crowdsourced": "Rete condivisa",
        "location": "Posizione:",
        "coordinates": "Coordinate:",
        "copy_to_clipboard": "Copia negli appunti",
        "copy_plus_code": "Copia il Plus Code negli appunti",
        "copy_coordinates": "Copia le coordinate negli appunti",
        "assets_unavailable_title": "Mappa non disponibile",
        "assets_unavailable_body": (
            "I file della mappa forniti con questa integrazione non sono "
            "leggibili. Reinstalla questa integrazione e ricarica Home Assistant."
        ),
    },
    "nl": {
        "unknown_device": "Onbekend apparaat",
        "location_history": "Locatiegeschiedenis",
        "start_time": "Starttijd",
        "end_time": "Eindtijd",
        "min_accuracy": "Min. nauwkeurigheid (meters)",
        "apply_filters": "Filters toepassen",
        "filters_summary": "Filters",
        "showing_points_one": "{n} punt weergegeven",
        "showing_points_other": "{n} punten weergegeven",
        "time": "Tijd:",
        "accuracy": "Nauwkeurigheid:",
        "source": "Bron:",
        "own_device": "Eigen apparaat",
        "crowdsourced": "Crowdsourcingnetwerk",
        "location": "Locatie:",
        "coordinates": "Coördinaten:",
        "copy_to_clipboard": "Naar klembord kopiëren",
        "copy_plus_code": "Plus Code naar klembord kopiëren",
        "copy_coordinates": "Coördinaten naar klembord kopiëren",
        "assets_unavailable_title": "Kaart niet beschikbaar",
        "assets_unavailable_body": (
            "De kaartbestanden die bij deze integratie worden meegeleverd, konden niet "
            "worden gelezen. Installeer deze integratie opnieuw en herlaad Home Assistant."
        ),
    },
    "pl": {
        "unknown_device": "Nieznane urządzenie",
        "location_history": "Historia lokalizacji",
        "start_time": "Czas rozpoczęcia",
        "end_time": "Czas zakończenia",
        "min_accuracy": "Min. dokładność (metry)",
        "apply_filters": "Zastosuj filtry",
        "filters_summary": "Filtry",
        "showing_points_one": "Pokazano {n} punkt",
        "showing_points_other": "Pokazano {n} punktów",
        "time": "Czas:",
        "accuracy": "Dokładność:",
        "source": "Źródło:",
        "own_device": "Własne urządzenie",
        "crowdsourced": "Sieć crowdsourcingowa",
        "location": "Lokalizacja:",
        "coordinates": "Współrzędne:",
        "copy_to_clipboard": "Kopiuj do schowka",
        "copy_plus_code": "Kopiuj Plus Code do schowka",
        "copy_coordinates": "Kopiuj współrzędne do schowka",
        "assets_unavailable_title": "Mapa niedostępna",
        "assets_unavailable_body": (
            "Nie można odczytać plików mapy dostarczonych z tą integracją. "
            "Zainstaluj ponownie tę integrację i przeładuj Home Assistant."
        ),
    },
    "pt-BR": {
        "unknown_device": "Dispositivo desconhecido",
        "location_history": "Histórico de localização",
        "start_time": "Hora de início",
        "end_time": "Hora de término",
        "min_accuracy": "Precisão mínima (metros)",
        "apply_filters": "Aplicar filtros",
        "filters_summary": "Filtros",
        "showing_points_one": "Exibindo {n} ponto",
        "showing_points_other": "Exibindo {n} pontos",
        "time": "Hora:",
        "accuracy": "Precisão:",
        "source": "Fonte:",
        "own_device": "Dispositivo próprio",
        "crowdsourced": "Rede colaborativa",
        "location": "Local:",
        "coordinates": "Coordenadas:",
        "copy_to_clipboard": "Copiar para a área de transferência",
        "copy_plus_code": "Copiar Plus Code para a área de transferência",
        "copy_coordinates": "Copiar coordenadas para a área de transferência",
        "assets_unavailable_title": "Mapa indisponível",
        "assets_unavailable_body": (
            "Não foi possível ler os arquivos do mapa incluídos nesta integração. "
            "Reinstale esta integração e recarregue o Home Assistant."
        ),
    },
    "pt": {
        "unknown_device": "Dispositivo desconhecido",
        "location_history": "Histórico de localização",
        "start_time": "Hora de início",
        "end_time": "Hora de fim",
        "min_accuracy": "Precisão mínima (metros)",
        "apply_filters": "Aplicar filtros",
        "filters_summary": "Filtros",
        "showing_points_one": "A mostrar {n} ponto",
        "showing_points_other": "A mostrar {n} pontos",
        "time": "Hora:",
        "accuracy": "Precisão:",
        "source": "Fonte:",
        "own_device": "Dispositivo próprio",
        "crowdsourced": "Rede colaborativa",
        "location": "Localização:",
        "coordinates": "Coordenadas:",
        "copy_to_clipboard": "Copiar para a área de transferência",
        "copy_plus_code": "Copiar Plus Code para a área de transferência",
        "copy_coordinates": "Copiar coordenadas para a área de transferência",
        "assets_unavailable_title": "Mapa indisponível",
        "assets_unavailable_body": (
            "Não foi possível ler os ficheiros do mapa incluídos nesta integração. "
            "Reinstale esta integração e recarregue o Home Assistant."
        ),
    },
}


def resolve_map_labels(language: str | None) -> dict[str, str]:
    """Resolve the map label set for a Home Assistant UI language.

    Three-stage, case-insensitive resolution, always returning a full label set
    (never empty, never a partial dict):

    1. exact code match (``pt-BR`` is preferred over its base ``pt``);
    2. base language without region (``de-DE`` -> ``de``);
    3. English fallback for an unknown or missing language.

    A fresh copy is returned so callers cannot mutate the shared catalog.
    """

    if not language:
        return dict(MAP_LABELS["en"])

    lowered = language.strip().lower()
    if not lowered:
        return dict(MAP_LABELS["en"])

    for code, labels in MAP_LABELS.items():
        if code.lower() == lowered:
            return dict(labels)

    base = lowered.split("-", 1)[0]
    for code, labels in MAP_LABELS.items():
        if code.lower() == base:
            return dict(labels)

    return dict(MAP_LABELS["en"])


def format_showing(labels: dict[str, str], count: int) -> str:
    """Return the localized ``Showing N points`` string for ``count`` points.

    Uses a minimal two-form plural (``n == 1`` -> singular, else plural). This is
    intentionally simple: only one plural string exists in the map (plan A5). For
    languages with richer plural rules (for example Polish) the ``_other`` form is
    used for all non-singular counts, a deliberate, documented simplification.
    """

    key = "showing_points_one" if count == 1 else "showing_points_other"
    template = labels.get(key) or labels["showing_points_other"]
    return template.replace("{n}", str(count))


def is_rtl(language: str | None) -> bool:
    """Return ``True`` if the language renders right-to-left (for example ``he``).

    Case-insensitive, region-tolerant (``he-IL`` -> ``he``).
    """

    if not language:
        return False
    lowered = language.strip().lower()
    if lowered in RTL_LANGUAGES:
        return True
    base = lowered.split("-", 1)[0]
    return base in RTL_LANGUAGES
