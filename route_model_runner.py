import os
import math
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple

import requests

SYD_ZONEINFO = ZoneInfo("Australia/Sydney")

ROUTES_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
PLACES_NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"

PLACE_TYPES_TRANSIT = [
    "bus_station",
    "transit_station",
]


def _load_env_file(path: Path) -> dict:
    if not path.exists():
        return {}
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if (not line) or line.startswith("#") or ("=" not in line):
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


ENV_PATH = Path("text.env")
_env = _load_env_file(ENV_PATH)

GOOGLE_API_KEY = (
    _env.get("GOOGLE_API_KEY")
    or _env.get("GOOGLE_MAPS_API_KEY")
    or _env.get("GMAPS_API_KEY")
    or os.environ.get("GOOGLE_API_KEY")
    or os.environ.get("GOOGLE_MAPS_API_KEY")
    or os.environ.get("GMAPS_API_KEY")
)

DEFAULT_SHUTTLE = {
    "usable_battery_kwh": 73.6,
    "kwh_per_km_baseline": 0.23,
    "onboard_systems_kw": 0.90,
    "device_charging_kw_per_device": 0.015,
    "avg_connected_devices": 4,
    "start_soc_pct_default": 90.0,
    "reserve_soc_pct": 20.0,
    "grid_emission_factor_kg_per_kwh": 0.64,
}

SOC_BUFFER_PP = 0.5

MU = {
    "name": "Macquarie University",
    "lat": -33.77680,
    "lng": 151.11408,
}

HH = {
    "name": "Hunters Hill Village",
    "lat": -33.83481,
    "lng": 151.15390,
}

SAVED_TRIPS = {
    "Macquarie University → Hunters Hill": ("MU_to_HH", MU, HH),
    "Hunters Hill → Macquarie University": ("HH_to_MU", HH, MU),
}

MULTISTOP_LIBRARY_FORWARD = {
    "Macquarie Centre": {
        "name": "Macquarie Centre",
        "lat": -33.77790,
        "lng": 151.11730,
    },
    "Macquarie Park Station": {
        "name": "Macquarie Park Station",
        "lat": -33.78547,
        "lng": 151.12910,
    },
    "Top Ryde City": {
        "name": "Top Ryde City",
        "lat": -33.81290,
        "lng": 151.10490,
    },
    "Gladesville Shops": {
        "name": "Gladesville Shops",
        "lat": -33.83220,
        "lng": 151.12770,
    },
}

MULTISTOP_LIBRARY_REVERSE = {
    "Gladesville Shops": {
        "name": "Gladesville Shops",
        "lat": -33.83245,
        "lng": 151.12720,
    },
    "Top Ryde City": {
        "name": "Top Ryde City",
        "lat": -33.81325,
        "lng": 151.10610,
    },
    "Macquarie Park Station": {
        "name": "Macquarie Park Station",
        "lat": -33.78547,
        "lng": 151.12910,
    },
    "Macquarie Centre": {
        "name": "Macquarie Centre",
        "lat": -33.77765,
        "lng": 151.11760,
    },
}


def decode_google_polyline(encoded: str) -> List[Tuple[float, float]]:
    if not encoded:
        return []

    coords = []
    index = 0
    lat = 0
    lng = 0
    length = len(encoded)

    while index < length:
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng

        coords.append((lat / 1e5, lng / 1e5))

    return coords


def _parse_duration_seconds(dur_str: Any) -> float:
    if dur_str is None:
        return 0.0
    if isinstance(dur_str, (int, float)):
        return float(dur_str)
    s = str(dur_str).strip()
    if s.endswith("s"):
        s = s[:-1]
    return float(s)


def _parse_depart_dt(date_str: str, time_str: str) -> datetime.datetime:
    day, month, year = [int(x) for x in date_str.split("/")]
    hour, minute = [int(x) for x in time_str.split(":")]

    requested_dt = datetime.datetime(
        year, month, day, hour, minute, tzinfo=SYD_ZONEINFO
    )

    now_dt = datetime.datetime.now(SYD_ZONEINFO)
    min_valid_dt = now_dt + datetime.timedelta(minutes=2)

    if requested_dt <= min_valid_dt:
        return min_valid_dt.replace(second=0, microsecond=0)

    return requested_dt


def _resolve_od(saved_trip: str):
    if saved_trip not in SAVED_TRIPS:
        raise ValueError(f"Unsupported saved trip: {saved_trip}")
    _, origin, destination = SAVED_TRIPS[saved_trip]
    return origin, destination


def _active_stop_library(saved_trip: str) -> Dict[str, dict]:
    if saved_trip == "Macquarie University → Hunters Hill":
        return MULTISTOP_LIBRARY_FORWARD
    return MULTISTOP_LIBRARY_REVERSE


def _fallback_route_points(origin: dict, destination: dict, stops: List[dict]) -> List[dict]:
    points = [{"lat": origin["lat"], "lng": origin["lng"]}]
    for stop in stops:
        points.append({"lat": stop["lat"], "lng": stop["lng"]})
    points.append({"lat": destination["lat"], "lng": destination["lng"]})
    return points


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    earth_radius_m = 6371000.0

    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius_m * c


def _nearest_polyline_index(points: List[Tuple[float, float]], lat: float, lng: float) -> int:
    best_idx = 0
    best_dist = float("inf")
    for idx, (p_lat, p_lng) in enumerate(points):
        d = _haversine_m(lat, lng, p_lat, p_lng)
        if d < best_dist:
            best_dist = d
            best_idx = idx
    return best_idx


def _sample_polyline_for_places(points: List[Tuple[float, float]], max_points: int = 9) -> List[Tuple[float, float]]:
    if not points:
        return []
    if len(points) <= max_points:
        return points

    out = []
    for i in range(max_points):
        idx = round(i * (len(points) - 1) / (max_points - 1))
        out.append(points[idx])
    return out


def _compute_leg_arrivals(
    depart_dt: datetime.datetime,
    leg_durations_s: List[float],
    stop_count: int,
) -> List[str]:
    arrivals = []
    cumulative = 0.0

    for i in range(stop_count):
        if i < len(leg_durations_s):
            cumulative += leg_durations_s[i]
        arrivals.append(
            (depart_dt + datetime.timedelta(seconds=cumulative)).strftime("%H:%M")
        )

    return arrivals


def _estimate_stop_indices_from_polyline(
    path_points: List[Tuple[float, float]],
    selected_stops: List[dict],
) -> List[int]:
    indices = []
    if not path_points:
        return indices

    last_idx = 0
    for stop in selected_stops:
        best_idx = last_idx
        best_dist = float("inf")
        for idx in range(last_idx, len(path_points)):
            d = _haversine_m(
                stop["lat"], stop["lng"],
                path_points[idx][0], path_points[idx][1],
            )
            if d < best_dist:
                best_dist = d
                best_idx = idx
        indices.append(best_idx)
        last_idx = best_idx

    return indices


def _places_nearby_search(
    included_types: List[str],
    lat: float,
    lng: float,
    radius_m: float = 450.0,
    max_results: int = 8,
) -> List[dict]:
    if not GOOGLE_API_KEY:
        return []

    body = {
        "includedTypes": included_types,
        "maxResultCount": max_results,
        "locationRestriction": {
            "circle": {
                "center": {
                    "latitude": lat,
                    "longitude": lng,
                },
                "radius": radius_m,
            }
        },
    }

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "places.id,"
            "places.displayName,"
            "places.formattedAddress,"
            "places.location,"
            "places.primaryType"
        ),
    }

    response = requests.post(
        PLACES_NEARBY_URL,
        json=body,
        headers=headers,
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()

    return payload.get("places", []) or []


def _resolve_bus_stop_for_choice(choice_name: str, hint: dict) -> dict:
    try:
        places = _places_nearby_search(
            included_types=PLACE_TYPES_TRANSIT,
            lat=float(hint["lat"]),
            lng=float(hint["lng"]),
            radius_m=500.0,
            max_results=8,
        )

        best = None
        best_dist = float("inf")

        for place in places:
            loc = place.get("location") or {}
            p_lat = loc.get("latitude")
            p_lng = loc.get("longitude")
            if p_lat is None or p_lng is None:
                continue

            dist = _haversine_m(
                float(hint["lat"]),
                float(hint["lng"]),
                float(p_lat),
                float(p_lng),
            )

            if dist < best_dist:
                best_dist = dist
                best = place

        if best:
            loc = best.get("location") or {}
            return {
                "name": choice_name,
                "display_name": ((best.get("displayName") or {}).get("text")) or choice_name,
                "lat": float(loc["latitude"]),
                "lng": float(loc["longitude"]),
                "source": "nearest_transit_stop",
            }
    except Exception:
        pass

    return {
        "name": choice_name,
        "display_name": choice_name,
        "lat": float(hint["lat"]),
        "lng": float(hint["lng"]),
        "source": "fallback_choice_location",
    }


def _resolve_selected_stop_points(selected_stops: List[str], saved_trip: str) -> List[dict]:
    library = _active_stop_library(saved_trip)

    if saved_trip == "Macquarie University → Hunters Hill":
        ordered_names = [name for name in selected_stops if name in library]
    else:
        ordered_names = [name for name in reversed(selected_stops) if name in library]

    resolved = []
    for name in ordered_names:
        resolved.append(_resolve_bus_stop_for_choice(name, library[name]))
    return resolved


def _build_waypoints(selected_stops: List[dict]) -> List[dict]:
    return [
        {
            "location": {
                "latLng": {
                    "latitude": float(stop["lat"]),
                    "longitude": float(stop["lng"]),
                }
            }
        }
        for stop in selected_stops
    ]


def _compute_route_google(
    origin: dict,
    destination: dict,
    selected_stops: List[dict],
    depart_dt: datetime.datetime,
    avoid_tolls: bool,
    fastest_route_only: bool,
    allow_alternatives: bool,
) -> List[dict]:
    if not GOOGLE_API_KEY:
        raise RuntimeError("No Google API key found in text.env or environment variables.")

    body = {
        "origin": {
            "location": {
                "latLng": {
                    "latitude": float(origin["lat"]),
                    "longitude": float(origin["lng"]),
                }
            }
        },
        "destination": {
            "location": {
                "latLng": {
                    "latitude": float(destination["lat"]),
                    "longitude": float(destination["lng"]),
                }
            }
        },
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE",
        "computeAlternativeRoutes": allow_alternatives and (not fastest_route_only),
        "departureTime": depart_dt.isoformat(),
        "languageCode": "en-AU",
        "units": "METRIC",
        "polylineEncoding": "ENCODED_POLYLINE",
        "polylineQuality": "HIGH_QUALITY",
        "extraComputations": ["TOLLS"],
    }

    if selected_stops:
        body["intermediates"] = _build_waypoints(selected_stops)

    if avoid_tolls:
        body["routeModifiers"] = {
            "avoidTolls": True,
            "avoidHighways": False,
            "avoidFerries": False,
        }

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "routes.routeLabels,"
            "routes.duration,"
            "routes.distanceMeters,"
            "routes.polyline.encodedPolyline,"
            "routes.legs.duration,"
            "routes.legs.distanceMeters,"
            "routes.legs.steps.navigationInstruction.instructions,"
            "routes.travelAdvisory.tollInfo"
        ),
    }

    response = requests.post(ROUTES_URL, json=body, headers=headers, timeout=20)

    if not response.ok:
        print("\n========= GOOGLE ROUTES RAW ERROR =========")
        print("STATUS:", response.status_code)
        print("BODY:", response.text)
        print("REQUEST BODY:", body)
        print("FIELD MASK:", headers["X-Goog-FieldMask"])
        print("==========================================\n")

    response.raise_for_status()

    payload = response.json()
    routes = payload.get("routes", [])
    if not routes:
        raise RuntimeError("No routes returned by Google Routes API.")

    results = []

    for idx, route in enumerate(routes, start=1):
        duration_s = _parse_duration_seconds(route.get("duration"))
        distance_m = float(route.get("distanceMeters", 0.0))
        encoded_polyline = ((route.get("polyline") or {}).get("encodedPolyline") or "")
        path_points = decode_google_polyline(encoded_polyline)

        legs = route.get("legs", []) or []
        leg_durations_s = [
            _parse_duration_seconds(leg.get("duration"))
            for leg in legs
        ]
        leg_distances_m = [
            float(leg.get("distanceMeters", 0.0))
            for leg in legs
        ]

        steps = []
        for leg in legs:
            for step in leg.get("steps", []) or []:
                instr = ((step.get("navigationInstruction") or {}).get("instructions"))
                if instr:
                    steps.append(instr)

        toll_status = ((route.get("travelAdvisory") or {}).get("tollInfo")) is not None
        route_labels = route.get("routeLabels", []) or []

        results.append(
            {
                "route_id": f"R{idx}",
                "duration_s": duration_s,
                "distance_m": distance_m,
                "path_points": path_points,
                "toll_status": toll_status,
                "steps": steps,
                "leg_durations_s": leg_durations_s,
                "leg_distances_m": leg_distances_m,
                "route_labels": route_labels,
            }
        )

    return results


def _search_nearby_chargers_along_route(
    path_points: List[Tuple[float, float]],
    max_results: int = 12,
) -> List[dict]:
    if not GOOGLE_API_KEY or not path_points:
        return []

    sampled_points = _sample_polyline_for_places(path_points, max_points=9)
    deduped: Dict[str, dict] = {}

    for sample_idx, (lat, lng) in enumerate(sampled_points):
        body = {
            "includedTypes": ["electric_vehicle_charging_station"],
            "maxResultCount": 8,
            "locationRestriction": {
                "circle": {
                    "center": {
                        "latitude": lat,
                        "longitude": lng,
                    },
                    "radius": 1800.0,
                }
            },
        }

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": GOOGLE_API_KEY,
            "X-Goog-FieldMask": (
                "places.id,"
                "places.displayName,"
                "places.formattedAddress,"
                "places.location,"
                "places.primaryType"
            ),
        }

        try:
            response = requests.post(
                PLACES_NEARBY_URL,
                json=body,
                headers=headers,
                timeout=12,
            )
            response.raise_for_status()
            payload = response.json()

            for place in payload.get("places", []) or []:
                place_id = place.get("id")
                if not place_id or place_id in deduped:
                    continue

                loc = place.get("location") or {}
                p_lat = loc.get("latitude")
                p_lng = loc.get("longitude")
                if p_lat is None or p_lng is None:
                    continue

                nearest_idx = _nearest_polyline_index(path_points, p_lat, p_lng)

                deduped[place_id] = {
                    "id": place_id,
                    "name": ((place.get("displayName") or {}).get("text")) or "Charging station",
                    "address": place.get("formattedAddress", ""),
                    "lat": p_lat,
                    "lng": p_lng,
                    "primary_type": place.get("primaryType"),
                    "sample_order": sample_idx,
                    "route_index": nearest_idx,
                }
        except Exception:
            continue

    chargers = list(deduped.values())
    chargers.sort(key=lambda x: x.get("route_index", 10**9))

    if len(chargers) <= max_results:
        return chargers

    selected = []
    for i in range(max_results):
        idx = round(i * (len(chargers) - 1) / (max_results - 1))
        selected.append(chargers[idx])

    final_deduped = []
    seen_ids = set()
    for c in selected:
        cid = c.get("id")
        if cid in seen_ids:
            continue
        seen_ids.add(cid)
        final_deduped.append(c)

    return final_deduped


def route_energy_breakdown(route_row: dict, passengers: int, depart_dt: datetime.datetime) -> dict:
    distance_km = float(route_row["distance_m"]) / 1000.0
    duration_h = float(route_row["duration_s"]) / 3600.0

    base_kwh_per_km = DEFAULT_SHUTTLE["kwh_per_km_baseline"]
    passenger_factor = 1.0 + 0.015 * passengers

    month = depart_dt.month
    if month in [12, 1, 2]:
        hvac_factor = 1.15
    elif month in [6, 7, 8]:
        hvac_factor = 1.10
    else:
        hvac_factor = 1.05

    traction_kwh = distance_km * base_kwh_per_km * passenger_factor * hvac_factor

    onboard_kw = DEFAULT_SHUTTLE["onboard_systems_kw"]
    device_kw = (
        DEFAULT_SHUTTLE["device_charging_kw_per_device"]
        * DEFAULT_SHUTTLE["avg_connected_devices"]
    )

    onboard_kwh = onboard_kw * duration_h
    device_kwh = device_kw * duration_h
    auxiliary_kwh = onboard_kwh + device_kwh
    total_kwh = traction_kwh + auxiliary_kwh

    avg_trip_power_kw = total_kwh / duration_h if duration_h > 0 else 0.0
    avg_speed_kmh = distance_km / duration_h if duration_h > 0 else 0.0

    return {
        "total_kwh": round(total_kwh, 2),
        "traction_kwh": round(traction_kwh, 2),
        "auxiliary_kwh": round(auxiliary_kwh, 2),
        "onboard_kwh": round(onboard_kwh, 2),
        "device_kwh": round(device_kwh, 2),
        "hvac_kw_est": round((hvac_factor - 1.0) * traction_kwh, 2),
        "avg_trip_power_kw": round(avg_trip_power_kw, 2),
        "avg_speed_kmh": round(avg_speed_kmh, 1),
    }


def soc_after_trip(energy_kwh: float) -> dict:
    battery = DEFAULT_SHUTTLE["usable_battery_kwh"]
    start_soc = DEFAULT_SHUTTLE["start_soc_pct_default"]
    reserve = DEFAULT_SHUTTLE["reserve_soc_pct"]

    drop_pct = (energy_kwh / battery) * 100.0
    end_soc = max(0.0, start_soc - drop_pct)

    charging_energy_to_90 = max(0.0, ((90.0 - end_soc) / 100.0) * battery)
    required_30min_kw = charging_energy_to_90 / 0.5 if charging_energy_to_90 > 0 else 0.0
    charging_time_ac_22_h = charging_energy_to_90 / 22.0 if charging_energy_to_90 > 0 else 0.0
    charging_time_dc_50_h = charging_energy_to_90 / 50.0 if charging_energy_to_90 > 0 else 0.0

    usable_above_reserve_kwh = max(0.0, ((end_soc - reserve) / 100.0) * battery)
    remaining_trips_before_charge = int(usable_above_reserve_kwh / max(energy_kwh, 0.001))

    return {
        "start_soc_pct": round(start_soc, 1),
        "end_soc_pct": round(end_soc, 1),
        "reserve_soc_pct": round(reserve, 1),
        "effective_reserve_pct": round(reserve + SOC_BUFFER_PP, 1),
        "soc_drop_pp": round(drop_pct, 1),
        "charging_energy_to_recover_90_soc_kwh": round(charging_energy_to_90, 2),
        "required_charger_power_30min_kw": round(required_30min_kw, 1),
        "charging_time_ac_22kw": _hours_to_mmss(charging_time_ac_22_h),
        "charging_time_dc_50kw": _hours_to_mmss(charging_time_dc_50_h),
        "remaining_trips_before_charge": remaining_trips_before_charge,
    }


def _hours_to_mmss(hours_value: float) -> str:
    if hours_value <= 0:
        return "00:00"
    total_seconds = int(round(hours_value * 3600.0))
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes:02d}:{seconds:02d}"


def _rank_routes_balanced(routes: List[dict], fastest_route_only: bool) -> List[dict]:
    if not routes:
        return []

    durations = [float(r["duration_s"]) for r in routes]
    distances = [float(r["distance_m"]) for r in routes]
    energies = [float(r["energy"]["total_kwh"]) for r in routes]
    emissions = [float(r["sustainability_metrics"]["emissions_kg_co2e"]) for r in routes]

    min_duration, max_duration = min(durations), max(durations)
    min_distance, max_distance = min(distances), max(distances)
    min_energy, max_energy = min(energies), max(energies)
    min_emissions, max_emissions = min(emissions), max(emissions)

    def norm(v: float, v_min: float, v_max: float) -> float:
        if v_max <= v_min:
            return 0.0
        return (v - v_min) / (v_max - v_min)

    ranked = []

    for r in routes:
        duration_norm = norm(float(r["duration_s"]), min_duration, max_duration)
        distance_norm = norm(float(r["distance_m"]), min_distance, max_distance)
        energy_norm = norm(float(r["energy"]["total_kwh"]), min_energy, max_energy)
        emissions_norm = norm(
            float(r["sustainability_metrics"]["emissions_kg_co2e"]),
            min_emissions,
            max_emissions,
        )

        if fastest_route_only:
            score = duration_norm
            basis = "Fastest-route selection"
        else:
            score = (
                0.45 * energy_norm
                + 0.25 * distance_norm
                + 0.20 * emissions_norm
                + 0.10 * duration_norm
            )
            basis = "Energy-first balanced routing across time, distance, energy use and grid-charging emissions"

        score = max(0.0, min(1.0, score))

        sustainability_index = round(0.20 + 0.80 * (1.0 - score), 3)

        item = dict(r)
        item["score"] = round(score, 6)
        item["route_sustainability_index"] = sustainability_index
        item["recommendation_basis"] = basis
        ranked.append(item)

    ranked.sort(key=lambda x: x["score"])
    return ranked


def _queue_risk_from_charger_count(charger_count: int) -> str:
    if charger_count >= 7:
        return "Low"
    if charger_count >= 3:
        return "Medium"
    if charger_count >= 1:
        return "High"
    return "Unknown"


def _charging_schedule_from_soc(arrive_soc_pct: float, charger_count: int) -> str:
    if arrive_soc_pct >= 75 and charger_count >= 3:
        return "Deferred charging acceptable"
    if arrive_soc_pct >= 40:
        return "Charge later today"
    return "Charge soon after arrival"


def _build_origin_for_reroute(request_data: Dict[str, Any], default_origin: dict) -> dict:
    current_lat = request_data.get("current_lat")
    current_lng = request_data.get("current_lng")

    if isinstance(current_lat, (int, float)) and isinstance(current_lng, (int, float)):
        return {
            "name": "Current vehicle position",
            "lat": float(current_lat),
            "lng": float(current_lng),
        }

    return default_origin


def _enrich_route_metrics(
    route: dict,
    passengers: int,
    depart_dt: datetime.datetime,
    selected_stops: List[dict],
) -> dict:
    energy = route_energy_breakdown(route_row=route, passengers=passengers, depart_dt=depart_dt)
    soc = soc_after_trip(energy["total_kwh"])

    distance_km = float(route["distance_m"]) / 1000.0
    pax_km = distance_km * max(passengers, 1)
    emissions = energy["total_kwh"] * DEFAULT_SHUTTLE["grid_emission_factor_kg_per_kwh"]

    stop_arrivals = _compute_leg_arrivals(
        depart_dt=depart_dt,
        leg_durations_s=route.get("leg_durations_s", []),
        stop_count=len(selected_stops),
    )

    stop_indices = _estimate_stop_indices_from_polyline(
        path_points=route.get("path_points", []),
        selected_stops=selected_stops,
    )

    sustainability_metrics = {
        "energy_per_km": round(energy["total_kwh"] / distance_km, 3) if distance_km > 0 else 0.0,
        "energy_per_passenger_km": round(energy["total_kwh"] / pax_km, 3) if pax_km > 0 else 0.0,
        "emissions_kg_co2e": round(emissions, 2),
        "avg_speed_kmh": energy["avg_speed_kmh"],
        "average_trip_power_kw": energy["avg_trip_power_kw"],
    }

    enriched = dict(route)
    enriched["energy"] = energy
    enriched["soc"] = soc
    enriched["sustainability_metrics"] = sustainability_metrics
    enriched["stop_arrivals"] = stop_arrivals
    enriched["stop_indices"] = stop_indices
    return enriched


def _route_payload(route: dict, route_kind: str, selected_stops: List[dict]) -> dict:
    return {
        "route_id": route["route_id"],
        "travel_time_min": round(float(route["duration_s"]) / 60.0, 1),
        "distance_km": round(float(route["distance_m"]) / 1000.0, 2),
        "arrival_time": route["arrival_time"],
        "energy_kwh": route["energy"]["total_kwh"],
        "traction_kwh": route["energy"]["traction_kwh"],
        "auxiliary_kwh": route["energy"]["auxiliary_kwh"],
        "onboard_kwh": route["energy"]["onboard_kwh"],
        "device_kwh": route["energy"]["device_kwh"],
        "avg_trip_power_kw": route["energy"]["avg_trip_power_kw"],
        "energy_per_km": route["sustainability_metrics"]["energy_per_km"],
        "energy_per_passenger_km": route["sustainability_metrics"]["energy_per_passenger_km"],
        "emissions_kg_co2e": route["sustainability_metrics"]["emissions_kg_co2e"],
        "avg_speed_kmh": route["sustainability_metrics"]["avg_speed_kmh"],
        "soc_start_pct": route["soc"]["start_soc_pct"],
        "soc_end_pct": route["soc"]["end_soc_pct"],
        "soc_drop_pp": route["soc"]["soc_drop_pp"],
        "charging_energy_to_recover_90_soc_kwh": route["soc"]["charging_energy_to_recover_90_soc_kwh"],
        "required_charger_power_30min_kw": route["soc"]["required_charger_power_30min_kw"],
        "charging_time_ac_22kw": route["soc"]["charging_time_ac_22kw"],
        "charging_time_dc_50kw": route["soc"]["charging_time_dc_50kw"],
        "remaining_trips_before_charge": route["soc"]["remaining_trips_before_charge"],
        "tolls": "Toll applies" if route.get("toll_status") else "No toll",
        "route_sustainability_index": route["route_sustainability_index"],
        "score": round(route["score"], 4),
        "recommendation_basis": route.get("recommendation_basis", ""),
        "route_points": [
            {"lat": float(lat), "lng": float(lng)}
            for lat, lng in route.get("path_points", [])
        ],
        "stop_indices": route.get("stop_indices", []),
        "stop_arrivals": route.get("stop_arrivals", []),
        "stop_points": [
            {
                "name": stop["name"],
                "display_name": stop.get("display_name", stop["name"]),
                "lat": stop["lat"],
                "lng": stop["lng"],
            }
            for stop in selected_stops
        ] if route_kind != "reference_alternative_without_stops" else [],
        "selected_stops": [stop["name"] for stop in selected_stops]
        if route_kind != "reference_alternative_without_stops" else [],
        "is_selectable": route_kind != "reference_alternative_without_stops",
        "route_kind": route_kind,
    }

def _concat_path_points(legs: List[dict]) -> List[Tuple[float, float]]:
    combined: List[Tuple[float, float]] = []

    for i, leg in enumerate(legs):
        leg_points = leg.get("path_points", []) or []
        if not leg_points:
            continue

        if i == 0:
            combined.extend(leg_points)
        else:
            combined.extend(leg_points[1:])

    return combined


def _build_combined_route_from_legs(
    legs: List[dict],
    route_id: str,
) -> dict:
    duration_s = sum(float(leg.get("duration_s", 0.0)) for leg in legs)
    distance_m = sum(float(leg.get("distance_m", 0.0)) for leg in legs)

    path_points = _concat_path_points(legs)

    steps: List[str] = []
    leg_durations_s: List[float] = []
    leg_distances_m: List[float] = []
    toll_status = False

    for leg in legs:
        steps.extend(leg.get("steps", []) or [])
        leg_durations_s.append(float(leg.get("duration_s", 0.0)))
        leg_distances_m.append(float(leg.get("distance_m", 0.0)))
        toll_status = toll_status or bool(leg.get("toll_status"))

    return {
        "route_id": route_id,
        "duration_s": duration_s,
        "distance_m": distance_m,
        "path_points": path_points,
        "toll_status": toll_status,
        "steps": steps,
        "leg_durations_s": leg_durations_s,
        "leg_distances_m": leg_distances_m,
        "route_labels": ["COMBINED_MULTILEG"],
    }


def _build_stop_preserving_alternatives(
    origin: dict,
    destination: dict,
    selected_stops: List[dict],
    depart_dt: datetime.datetime,
    avoid_tolls: bool,
) -> List[dict]:
    nodes = [origin] + selected_stops + [destination]
    if len(nodes) < 2:
        return []

    all_leg_options: List[List[dict]] = []

    for i in range(len(nodes) - 1):
        leg_origin = nodes[i]
        leg_destination = nodes[i + 1]

        leg_routes = _compute_route_google(
            origin=leg_origin,
            destination=leg_destination,
            selected_stops=[],
            depart_dt=depart_dt,
            avoid_tolls=avoid_tolls,
            fastest_route_only=False,
            allow_alternatives=True,
        )

        if not leg_routes:
            return []

        all_leg_options.append(leg_routes)

    combinations: List[List[int]] = []
    base_combo = [0] * len(all_leg_options)
    combinations.append(base_combo)

    for leg_idx, leg_options in enumerate(all_leg_options):
        max_alt_index = min(len(leg_options) - 1, 2)
        for alt_idx in range(1, max_alt_index + 1):
            combo = [0] * len(all_leg_options)
            combo[leg_idx] = alt_idx
            combinations.append(combo)

    built_routes: List[dict] = []
    seen_keys = set()

    for combo_idx, combo in enumerate(combinations, start=1):
        chosen_legs = [
            all_leg_options[leg_i][choice_idx]
            for leg_i, choice_idx in enumerate(combo)
        ]

        combined = _build_combined_route_from_legs(
            chosen_legs,
            route_id=f"R{combo_idx}",
        )

        key = (
            round(float(combined["duration_s"]), 1),
            round(float(combined["distance_m"]), 1),
            tuple(
                (
                    round(float(p[0]), 5),
                    round(float(p[1]), 5),
                )
                for p in combined.get("path_points", [])[:40]
            ),
        )

        if key in seen_keys:
            continue

        seen_keys.add(key)
        built_routes.append(combined)

    return built_routes


def run_route_model(request_data: Dict[str, Any]) -> Dict[str, Any]:
    saved_trip = request_data["saved_trip"]
    selected_stop_names = request_data.get("selected_stops", [])
    trip_type = request_data.get("trip_type", "single")
    passengers = int(request_data.get("passengers", 0))
    avoid_tolls = bool(request_data.get("avoid_tolls", False))
    nearby_chargers = bool(request_data.get("nearby_chargers", False))
    fastest_route_only = bool(request_data.get("fastest_route_only", False))
    departure_date = request_data["departure_date"]
    departure_time = request_data["departure_time"]

    default_origin, destination = _resolve_od(saved_trip)
    origin = _build_origin_for_reroute(request_data, default_origin)
    selected_stops = _resolve_selected_stop_points(selected_stop_names, saved_trip)
    depart_dt = _parse_depart_dt(departure_date, departure_time)

    try:
        stop_routes_raw = _compute_route_google(
            origin=origin,
            destination=destination,
            selected_stops=selected_stops,
            depart_dt=depart_dt,
            avoid_tolls=avoid_tolls,
            fastest_route_only=fastest_route_only,
            allow_alternatives=not fastest_route_only,
        )

        enriched_stop_routes = [
            _enrich_route_metrics(
                route=r,
                passengers=passengers,
                depart_dt=depart_dt,
                selected_stops=selected_stops,
            )
            for r in stop_routes_raw
        ]

        ranked_stop_routes = _rank_routes_balanced(
            enriched_stop_routes,
            fastest_route_only=fastest_route_only,
        )

        for r in ranked_stop_routes:
            r["arrival_time"] = (
                depart_dt + datetime.timedelta(seconds=float(r["duration_s"]))
            ).strftime("%H:%M")

        best = ranked_stop_routes[0]

        if (len(ranked_stop_routes) <= 1) and (not fastest_route_only) and selected_stops:
            try:
                combined_alt_routes_raw = _build_stop_preserving_alternatives(
                    origin=origin,
                    destination=destination,
                    selected_stops=selected_stops,
                    depart_dt=depart_dt,
                    avoid_tolls=avoid_tolls,
                )

                enriched_combined_routes = [
                    _enrich_route_metrics(
                        route=r,
                        passengers=passengers,
                        depart_dt=depart_dt,
                        selected_stops=selected_stops,
                    )
                    for r in combined_alt_routes_raw
                ]

                ranked_stop_routes = _rank_routes_balanced(
                    enriched_combined_routes,
                    fastest_route_only=False,
                )

                for idx, r in enumerate(ranked_stop_routes, start=1):
                    r["route_id"] = f"R{idx}"
                    r["arrival_time"] = (
                        depart_dt + datetime.timedelta(seconds=float(r["duration_s"]))
                    ).strftime("%H:%M")
            except Exception:
                pass        


        duration_s = float(best["duration_s"])
        if trip_type == "round":
            selected_arrival_dt = depart_dt + datetime.timedelta(seconds=duration_s * 2.0)
        else:
            selected_arrival_dt = depart_dt + datetime.timedelta(seconds=duration_s)

        best_path_points = best.get("path_points", [])
        route_points = [
            {"lat": float(lat), "lng": float(lng)}
            for lat, lng in best_path_points
        ]
        if not route_points:
            route_points = _fallback_route_points(origin, destination, selected_stops)

        distance_km = float(best["distance_m"]) / 1000.0
        total_kwh = float(best["energy"]["total_kwh"])

        highest_energy_kwh = max(float(r["energy"]["total_kwh"]) for r in ranked_stop_routes)
        saving_vs_highest_pct = 0.0
        if highest_energy_kwh > 0:
            saving_vs_highest_pct = ((highest_energy_kwh - total_kwh) / highest_energy_kwh) * 100.0

        chargers = (
            _search_nearby_chargers_along_route(best_path_points, max_results=12)
            if nearby_chargers
            else []
        )

        queue_risk = _queue_risk_from_charger_count(len(chargers))
        charging_schedule = _charging_schedule_from_soc(best["soc"]["end_soc_pct"], len(chargers))

        all_routes_payload = []
        for idx, route in enumerate(ranked_stop_routes):
            all_routes_payload.append(
                _route_payload(
                    route,
                    route_kind="selected_route" if idx == 0 else "alternative_route",
                    selected_stops=selected_stops,
                )
            )

        
        return {
            "selected_route": {
                "route_id": best["route_id"],
                "travel_time_min": round(duration_s / 60.0, 1),
                "distance_km": round(distance_km, 2),
                "arrival_time": selected_arrival_dt.strftime("%H:%M"),
                "tolls": "Toll applies" if best.get("toll_status") else "No toll",
                "sustainability_score": best["route_sustainability_index"],
                "energy_saving_vs_highest_energy_route_pct": round(saving_vs_highest_pct, 1),
            },
            "energy": {
                **best["energy"],
                "charging_energy_to_recover_90_soc_kwh": best["soc"]["charging_energy_to_recover_90_soc_kwh"],
                "required_charger_power_30min_kw": best["soc"]["required_charger_power_30min_kw"],
                "charging_time_ac_22kw": best["soc"]["charging_time_ac_22kw"],
                "charging_time_dc_50kw": best["soc"]["charging_time_dc_50kw"],
            },
            "soc": best["soc"],
            "sustainability_metrics": {
                **best["sustainability_metrics"],
                "energy_saving_vs_highest_energy_route_pct": round(saving_vs_highest_pct, 1),
                "soc_drop_pp": best["soc"]["soc_drop_pp"],
            },
            "navigation_steps": best.get("steps", []),
            "route_points": route_points,
            "selected_stops": [stop["name"] for stop in selected_stops],
            "stop_arrivals": best["stop_arrivals"],
            "stop_indices": best["stop_indices"],
            "stop_points": [
                {
                    "name": stop["name"],
                    "display_name": stop.get("display_name", stop["name"]),
                    "lat": stop["lat"],
                    "lng": stop["lng"],
                }
                for stop in selected_stops
            ],
            "chargers": chargers,
            "queue_risk": queue_risk,
            "suggested_charging_schedule": charging_schedule,
            "route_sustainability_index": best["route_sustainability_index"],
            "avg_speed_kmh": best["sustainability_metrics"]["avg_speed_kmh"],
            "remaining_trips_before_charge": best["soc"]["remaining_trips_before_charge"],
            "recommended_route": best["route_id"],
            "recommendation_basis": best["recommendation_basis"],
            "all_routes": all_routes_payload,
            "alternatives_note": (
                "" if (len(ranked_stop_routes) > 1 or fastest_route_only) else
                "Reference alternatives may be shown if Google does not return multiple stop-based routes."
            ),
        }

    except Exception:
        import traceback
        print("\n========= GOOGLE ROUTE ERROR =========")
        traceback.print_exc()
        print("=====================================\n")

        fallback_points = _fallback_route_points(origin, destination, selected_stops)

        total_distance_km = 18.5 + (2.0 * len(selected_stops))
        duration_min = 42.0 + (5.0 * len(selected_stops))
        duration_s = duration_min * 60.0

        if trip_type == "round":
            arrival_dt = depart_dt + datetime.timedelta(seconds=duration_s * 2.0)
        else:
            arrival_dt = depart_dt + datetime.timedelta(seconds=duration_s)

        fallback_route = {
            "route_id": "R1",
            "duration_s": duration_s,
            "distance_m": total_distance_km * 1000.0,
            "path_points": [(p["lat"], p["lng"]) for p in fallback_points],
            "steps": [
                f"Start from {origin['name']}",
                "Proceed via selected stops",
                f"Arrive at {destination['name']}",
            ],
            "toll_status": None,
            "leg_durations_s": [duration_s / max(len(selected_stops) + 1, 1)] * max(len(selected_stops) + 1, 1),
            "leg_distances_m": [total_distance_km * 1000.0 / max(len(selected_stops) + 1, 1)] * max(len(selected_stops) + 1, 1),
        }

        enriched = _enrich_route_metrics(
            route=fallback_route,
            passengers=passengers,
            depart_dt=depart_dt,
            selected_stops=selected_stops,
        )
        enriched["arrival_time"] = (depart_dt + datetime.timedelta(seconds=duration_s)).strftime("%H:%M")

        queue_risk = "Unknown"
        charging_schedule = _charging_schedule_from_soc(enriched["soc"]["end_soc_pct"], 0)

        return {
            "selected_route": {
                "route_id": "R1",
                "travel_time_min": round(duration_min, 1),
                "distance_km": round(total_distance_km, 2),
                "arrival_time": arrival_dt.strftime("%H:%M"),
                "tolls": "Unknown",
                "sustainability_score": 0.75,
                "energy_saving_vs_highest_energy_route_pct": 0.0,
            },
            "energy": {
                **enriched["energy"],
                "charging_energy_to_recover_90_soc_kwh": enriched["soc"]["charging_energy_to_recover_90_soc_kwh"],
                "required_charger_power_30min_kw": enriched["soc"]["required_charger_power_30min_kw"],
                "charging_time_ac_22kw": enriched["soc"]["charging_time_ac_22kw"],
                "charging_time_dc_50kw": enriched["soc"]["charging_time_dc_50kw"],
            },
            "soc": enriched["soc"],
            "sustainability_metrics": {
                **enriched["sustainability_metrics"],
                "energy_saving_vs_highest_energy_route_pct": 0.0,
                "soc_drop_pp": enriched["soc"]["soc_drop_pp"],
            },
            "navigation_steps": fallback_route["steps"],
            "route_points": fallback_points,
            "selected_stops": [stop["name"] for stop in selected_stops],
            "stop_arrivals": enriched["stop_arrivals"],
            "stop_indices": enriched["stop_indices"],
            "stop_points": [
                {
                    "name": stop["name"],
                    "display_name": stop.get("display_name", stop["name"]),
                    "lat": stop["lat"],
                    "lng": stop["lng"],
                }
                for stop in selected_stops
            ],
            "chargers": [],
            "queue_risk": queue_risk,
            "suggested_charging_schedule": charging_schedule,
            "route_sustainability_index": 0.75,
            "avg_speed_kmh": enriched["sustainability_metrics"]["avg_speed_kmh"],
            "remaining_trips_before_charge": enriched["soc"]["remaining_trips_before_charge"],
            "recommended_route": "R1",
            "recommendation_basis": "Fallback route estimate",
            "all_routes": [
                _route_payload(
                    enriched,
                    route_kind="selected_route",
                    selected_stops=selected_stops,
                )
            ],
            "alternatives_note": "",
        }
