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
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
ELEVATION_API_URL = "https://maps.googleapis.com/maps/api/elevation/json"


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
    "vehicle_mass_kg": 2980.0,
    "avg_passenger_mass_kg": 75.0,
    "driveline_efficiency_uphill": 0.90,
    "regen_efficiency_downhill": 0.55,
}

SOC_BUFFER_PP = 0.5
UNIVERSITY_CHARGE_LIMIT_SOC_PCT = 25.0
AERO_DRAG_COEFF_CD = 0.68
FRONTAL_AREA_M2 = 7.6
AIR_DENSITY_KG_PER_M3 = 1.225
ROLLING_RESISTANCE_COEFF = 0.0105
IDLE_HVAC_BUFFER_KW = 0.35
STOP_START_BASE_KWH = 0.012
STOP_START_PER_PASSENGER_KWH = 0.0012
LOW_SPEED_STOP_GO_THRESHOLD_KMH = 22.0
HIGHWAY_EFFICIENCY_THRESHOLD_KMH = 58.0

ENERGY_MODEL_CALIBRATION = {
    "rolling_multiplier": float(_env.get("ROLLING_MULTIPLIER", os.environ.get("ROLLING_MULTIPLIER", 1.00))),
    "aero_multiplier": float(_env.get("AERO_MULTIPLIER", os.environ.get("AERO_MULTIPLIER", 1.00))),
    "hvac_multiplier": float(_env.get("HVAC_MULTIPLIER", os.environ.get("HVAC_MULTIPLIER", 1.00))),
    "stop_start_multiplier": float(_env.get("STOP_START_MULTIPLIER", os.environ.get("STOP_START_MULTIPLIER", 1.00))),
    "idling_multiplier": float(_env.get("IDLING_MULTIPLIER", os.environ.get("IDLING_MULTIPLIER", 1.00))),
    "traction_multiplier": float(_env.get("TRACTION_MULTIPLIER", os.environ.get("TRACTION_MULTIPLIER", 1.00))),
}


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



def _estimate_step_route_indices(
    path_points: List[Tuple[float, float]],
    step_details: List[dict],
) -> List[dict]:
    if not path_points or not step_details:
        return step_details

    last_idx = 0
    out = []

    for step in step_details:
        poly_points = step.get("poly_points", []) or []
        if poly_points:
            ref_lat, ref_lng = poly_points[0]
        else:
            ref_lat, ref_lng = path_points[min(last_idx, len(path_points) - 1)]

        best_idx = last_idx
        best_dist = float("inf")

        for idx in range(last_idx, len(path_points)):
            p_lat, p_lng = path_points[idx]
            d = _haversine_m(ref_lat, ref_lng, p_lat, p_lng)
            if d < best_dist:
                best_dist = d
                best_idx = idx

        item = dict(step)
        item["route_index"] = best_idx
        out.append(item)
        last_idx = best_idx

    return out


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
            "routes.legs.steps.distanceMeters,"
            "routes.legs.steps.polyline.encodedPolyline,"
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
        step_details = []
        for leg in legs:
            for step in leg.get("steps", []) or []:
                instr = ((step.get("navigationInstruction") or {}).get("instructions"))
                if not instr:
                    continue

                steps.append(instr)

                step_distance_m = float(step.get("distanceMeters", 0.0))
                step_poly = ((step.get("polyline") or {}).get("encodedPolyline") or "")
                step_poly_points = decode_google_polyline(step_poly) if step_poly else []

                step_details.append(
                    {
                        "instruction": instr,
                        "distance_m": step_distance_m,
                        "poly_points": step_poly_points,
                    }
                )

        step_details = _estimate_step_route_indices(path_points, step_details)

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
                "step_details": step_details,
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

_weather_cache: Dict[Tuple[float, float, str], float] = {}


def _route_midpoint_lat_lng(route_row: dict) -> Tuple[float, float]:
    path_points = route_row.get("path_points", []) or []

    if path_points:
        mid_idx = len(path_points) // 2
        mid_lat, mid_lng = path_points[mid_idx]
        return float(mid_lat), float(mid_lng)

    return float(MU["lat"]), float(MU["lng"])


def _fetch_outdoor_temp_c(lat: float, lng: float, target_dt: datetime.datetime) -> float:
    hour_dt = target_dt.astimezone(SYD_ZONEINFO).replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    cache_key = (round(lat, 4), round(lng, 4), hour_dt.isoformat())
    if cache_key in _weather_cache:
        return _weather_cache[cache_key]

    params = {
        "latitude": lat,
        "longitude": lng,
        "hourly": "temperature_2m",
        "timezone": "Australia/Sydney",
        "start_hour": hour_dt.strftime("%Y-%m-%dT%H:00"),
        "end_hour": hour_dt.strftime("%Y-%m-%dT%H:00"),
        "forecast_days": 3,
        "past_days": 1,
    }

    try:
        response = requests.get(OPEN_METEO_URL, params=params, timeout=12)
        response.raise_for_status()
        payload = response.json()

        hourly = payload.get("hourly", {}) or {}
        temps = hourly.get("temperature_2m", []) or []

        if temps:
            temp_c = float(temps[0])
            _weather_cache[cache_key] = temp_c
            return temp_c
    except Exception:
        pass

    fallback_by_month = {
        1: 24.0, 2: 24.0, 3: 22.0,
        4: 19.0, 5: 16.0, 6: 13.0,
        7: 12.0, 8: 14.0, 9: 17.0,
        10: 20.0, 11: 22.0, 12: 24.0,
    }
    return float(fallback_by_month.get(target_dt.month, 20.0))


def _estimate_hvac_power_kw(outdoor_temp_c: float, passengers: int) -> float:
    cooling_setpoint_c = 24.0
    heating_setpoint_c = 20.0

    if outdoor_temp_c > cooling_setpoint_c:
        hvac_kw = 1.2 + 0.22 * (outdoor_temp_c - cooling_setpoint_c)
    elif outdoor_temp_c < heating_setpoint_c:
        hvac_kw = 1.0 + 0.18 * (heating_setpoint_c - outdoor_temp_c)
    else:
        hvac_kw = 0.6

    passenger_hvac_factor = 1.0 + 0.01 * passengers
    hvac_kw *= passenger_hvac_factor

    return max(0.6, min(hvac_kw, 6.0))

def _sample_polyline_for_elevation(
    points: List[Tuple[float, float]],
    max_points: int = 32,
) -> List[Tuple[float, float]]:
    if not points:
        return []
    if len(points) <= max_points:
        return points

    out = []
    for i in range(max_points):
        idx = round(i * (len(points) - 1) / (max_points - 1))
        out.append(points[idx])
    return out


def _fetch_elevations_for_points(
    points: List[Tuple[float, float]],
) -> List[float]:
    if not GOOGLE_API_KEY or not points:
        return []

    sampled_points = _sample_polyline_for_elevation(points, max_points=32)
    locations_param = "|".join(
        f"{lat:.6f},{lng:.6f}" for lat, lng in sampled_points
    )

    try:
        response = requests.get(
            ELEVATION_API_URL,
            params={
                "locations": locations_param,
                "key": GOOGLE_API_KEY,
            },
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()

        results = payload.get("results", []) or []
        elevations = []
        for item in results:
            elev = item.get("elevation")
            if elev is None:
                return []
            elevations.append(float(elev))

        return elevations
    except Exception:
        return []


def _estimate_slope_energy_adjustment_kwh(
    route_row: dict,
    passengers: int,
) -> dict:
    path_points = route_row.get("path_points", []) or []
    sampled_points = _sample_polyline_for_elevation(path_points, max_points=32)
    elevations_m = _fetch_elevations_for_points(sampled_points)

    if len(sampled_points) < 2 or len(sampled_points) != len(elevations_m):
        return {
            "uphill_kwh": 0.0,
            "regen_kwh": 0.0,
            "net_kwh": 0.0,
            "elevation_api_used": False,
        }

    total_mass_kg = (
        DEFAULT_SHUTTLE["vehicle_mass_kg"]
        + max(passengers, 0) * DEFAULT_SHUTTLE["avg_passenger_mass_kg"]
    )

    uphill_kwh = 0.0
    regen_kwh = 0.0

    for i in range(len(sampled_points) - 1):
        lat1, lng1 = sampled_points[i]
        lat2, lng2 = sampled_points[i + 1]

        horiz_m = _haversine_m(lat1, lng1, lat2, lng2)
        if horiz_m < 5.0:
            continue

        delta_h_m = elevations_m[i + 1] - elevations_m[i]

        if abs(delta_h_m) < 1.0:
            continue

        potential_kwh = (
            total_mass_kg * 9.81 * abs(delta_h_m)
        ) / 3_600_000.0

        if delta_h_m > 0:
            uphill_kwh += (
                potential_kwh / DEFAULT_SHUTTLE["driveline_efficiency_uphill"]
            )
        else:
            regen_kwh += (
                potential_kwh * DEFAULT_SHUTTLE["regen_efficiency_downhill"]
            )

    net_kwh = uphill_kwh - regen_kwh

    return {
        "uphill_kwh": round(uphill_kwh, 3),
        "regen_kwh": round(regen_kwh, 3),
        "net_kwh": round(net_kwh, 3),
        "elevation_api_used": True,
    }

def _estimate_stop_start_penalty_kwh(route_row: dict, passengers: int) -> dict:
    step_details = route_row.get("step_details", []) or []
    steps = route_row.get("steps", []) or []

    reference_steps = step_details if step_details else [{"instruction": s} for s in steps]
    stop_like_steps = 0

    for step in reference_steps:
        instruction = str(step.get("instruction", "")).lower()
        if any(
            token in instruction
            for token in [
                "turn left",
                "turn right",
                "u-turn",
                "roundabout",
                "keep left",
                "keep right",
                "slight left",
                "slight right",
                "ramp",
                "exit",
                "merge",
            ]
        ):
            stop_like_steps += 1

    stop_like_steps += max(0, len(route_row.get("stop_indices", []) or []))

    base_events = max(1, min(stop_like_steps, 14))
    event_kwh = STOP_START_BASE_KWH + (STOP_START_PER_PASSENGER_KWH * max(passengers, 0))
    total_penalty = base_events * event_kwh

    return {
        "events": int(base_events),
        "kwh": round(total_penalty, 3),
    }


def _calibration_multiplier(name: str) -> float:
    value = float(ENERGY_MODEL_CALIBRATION.get(name, 1.0) or 1.0)
    return max(0.70, min(value, 1.35))


def _apply_energy_calibration(raw_kwh: float, name: str) -> float:
    return raw_kwh * _calibration_multiplier(name)


def _estimate_speed_traction_kwh(distance_km: float, avg_speed_kmh: float, passengers: int) -> dict:
    speed_kmh = max(avg_speed_kmh, 8.0)
    speed_mps = speed_kmh / 3.6

    total_mass_kg = (
        DEFAULT_SHUTTLE["vehicle_mass_kg"]
        + max(passengers, 0) * DEFAULT_SHUTTLE["avg_passenger_mass_kg"]
    )

    rolling_force_n = total_mass_kg * 9.81 * ROLLING_RESISTANCE_COEFF
    aero_force_n = 0.5 * AIR_DENSITY_KG_PER_M3 * AERO_DRAG_COEFF_CD * FRONTAL_AREA_M2 * (speed_mps ** 2)

    driveline_eff = 0.89 if speed_kmh <= HIGHWAY_EFFICIENCY_THRESHOLD_KMH else 0.86

    rolling_kwh_raw = (rolling_force_n * (distance_km * 1000.0)) / 3_600_000.0 / max(driveline_eff, 0.75)
    aero_kwh_raw = (aero_force_n * (distance_km * 1000.0)) / 3_600_000.0 / max(driveline_eff, 0.75)

    rolling_kwh = _apply_energy_calibration(rolling_kwh_raw, "rolling_multiplier")
    aero_kwh = _apply_energy_calibration(aero_kwh_raw, "aero_multiplier")
    traction_kwh = rolling_kwh + aero_kwh

    if speed_kmh <= LOW_SPEED_STOP_GO_THRESHOLD_KMH:
        traction_kwh *= 1.08
    elif speed_kmh >= 80.0:
        traction_kwh *= 1.04

    traction_kwh = _apply_energy_calibration(traction_kwh, "traction_multiplier")

    baseline_floor = distance_km * DEFAULT_SHUTTLE["kwh_per_km_baseline"] * (1.0 + 0.010 * max(passengers, 0))
    traction_kwh = max(traction_kwh, baseline_floor * 0.82)

    return {
        "rolling_kwh": round(rolling_kwh, 3),
        "aero_kwh": round(aero_kwh, 3),
        "traction_kwh": round(traction_kwh, 3),
        "speed_kmh_used": round(speed_kmh, 1),
    }


def _estimate_idling_energy_kwh(route_row: dict, hvac_kw: float) -> dict:
    traffic_delay_min = _traffic_delay_minutes(route_row)
    idle_kw = hvac_kw + DEFAULT_SHUTTLE["onboard_systems_kw"] + IDLE_HVAC_BUFFER_KW
    idle_kwh = idle_kw * (traffic_delay_min / 60.0)
    return {
        "traffic_delay_min": round(traffic_delay_min, 2),
        "idle_kwh": round(idle_kwh, 3),
    }


def route_energy_breakdown(route_row: dict, passengers: int, depart_dt: datetime.datetime) -> dict:
    distance_km = float(route_row["distance_m"]) / 1000.0
    duration_h = float(route_row["duration_s"]) / 3600.0
    avg_speed_kmh = distance_km / duration_h if duration_h > 0 else 0.0

    speed_based = _estimate_speed_traction_kwh(
        distance_km=distance_km,
        avg_speed_kmh=avg_speed_kmh,
        passengers=passengers,
    )

    traction_base_kwh = float(speed_based["traction_kwh"])

    slope_adj = _estimate_slope_energy_adjustment_kwh(route_row, passengers)
    slope_net_kwh = float(slope_adj["net_kwh"])

    max_downhill_credit_kwh = 0.35 * traction_base_kwh
    slope_net_kwh = max(slope_net_kwh, -max_downhill_credit_kwh)

    stop_start_adj = _estimate_stop_start_penalty_kwh(route_row, passengers)
    stop_start_kwh = _apply_energy_calibration(float(stop_start_adj["kwh"]), "stop_start_multiplier")

    traction_kwh = max(0.0, traction_base_kwh + slope_net_kwh + stop_start_kwh)

    outdoor_temp_c = None
    hvac_kw = 0.0
    hvac_kwh = 0.0

    if (
        "_route_midpoint_lat_lng" in globals()
        and "_fetch_outdoor_temp_c" in globals()
        and "_estimate_hvac_power_kw" in globals()
    ):
        mid_lat, mid_lng = _route_midpoint_lat_lng(route_row)
        outdoor_temp_c = _fetch_outdoor_temp_c(mid_lat, mid_lng, depart_dt)
        hvac_kw = _estimate_hvac_power_kw(outdoor_temp_c, passengers)
        hvac_kwh = _apply_energy_calibration(hvac_kw * duration_h, "hvac_multiplier")
        hvac_kw = hvac_kwh / duration_h if duration_h > 0 else hvac_kw
    else:
        month = depart_dt.month
        if month in [12, 1, 2]:
            hvac_factor = 1.15
        elif month in [6, 7, 8]:
            hvac_factor = 1.10
        else:
            hvac_factor = 1.05

        hvac_kwh = _apply_energy_calibration(max(0.0, (hvac_factor - 1.0) * traction_base_kwh), "hvac_multiplier")
        hvac_kw = hvac_kwh / duration_h if duration_h > 0 else 0.0

    idling_adj = _estimate_idling_energy_kwh(route_row, hvac_kw)
    idling_kwh = _apply_energy_calibration(float(idling_adj["idle_kwh"]), "idling_multiplier")

    onboard_kw = DEFAULT_SHUTTLE["onboard_systems_kw"]
    device_kw = (
        DEFAULT_SHUTTLE["device_charging_kw_per_device"]
        * DEFAULT_SHUTTLE["avg_connected_devices"]
    )

    onboard_kwh = onboard_kw * duration_h
    device_kwh = device_kw * duration_h
    auxiliary_kwh = onboard_kwh + device_kwh + hvac_kwh + idling_kwh
    total_kwh = traction_kwh + auxiliary_kwh

    avg_trip_power_kw = total_kwh / duration_h if duration_h > 0 else 0.0

    return {
        "total_kwh": round(total_kwh, 2),
        "traction_kwh": round(traction_kwh, 2),
        "traction_base_kwh": round(traction_base_kwh, 2),
        "rolling_kwh": round(float(speed_based["rolling_kwh"]), 2),
        "aero_kwh": round(float(speed_based["aero_kwh"]), 2),
        "stop_start_kwh": round(stop_start_kwh, 2),
        "stop_start_events": int(stop_start_adj["events"]),
        "slope_uphill_kwh": round(float(slope_adj["uphill_kwh"]), 2),
        "slope_regen_kwh": round(float(slope_adj["regen_kwh"]), 2),
        "slope_net_kwh": round(float(slope_net_kwh), 2),
        "elevation_api_used": bool(slope_adj["elevation_api_used"]),
        "auxiliary_kwh": round(auxiliary_kwh, 2),
        "onboard_kwh": round(onboard_kwh, 2),
        "device_kwh": round(device_kwh, 2),
        "hvac_kwh": round(hvac_kwh, 2),
        "idling_kwh": round(idling_kwh, 2),
        "traffic_delay_min": round(float(idling_adj["traffic_delay_min"]), 2),
        "hvac_kw_est": round(hvac_kw, 2),
        "outdoor_temp_c": round(outdoor_temp_c, 1) if outdoor_temp_c is not None else None,
        "avg_trip_power_kw": round(avg_trip_power_kw, 2),
        "avg_speed_kmh": round(float(speed_based["speed_kmh_used"]), 1),
    }


def soc_after_trip(energy_kwh: float, start_soc_pct: float | None = None) -> dict:
    battery = DEFAULT_SHUTTLE["usable_battery_kwh"]
    start_soc = float(start_soc_pct if start_soc_pct is not None else DEFAULT_SHUTTLE["start_soc_pct_default"])
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
        "charge_limit_soc_pct": round(UNIVERSITY_CHARGE_LIMIT_SOC_PCT, 1),
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


def _confidence_label_from_pct(score_pct: float) -> str:
    if score_pct >= 85:
        return "High"
    if score_pct >= 65:
        return "Good"
    if score_pct >= 45:
        return "Moderate"
    return "Low"


def _eta_confidence_label(traffic_delay_min: float) -> str:
    if traffic_delay_min <= 0.5:
        return "High"
    if traffic_delay_min <= 2.0:
        return "Good"
    if traffic_delay_min <= 4.0:
        return "Moderate"
    return "Low"


def _route_confidence_payload(route: dict) -> dict:
    closeness = float(route.get("topsis_closeness", 0.0) or 0.0)
    front_rank = int(route.get("pareto_front_rank", 1) or 1)
    front_factor = 1.0 / max(front_rank, 1)
    traffic_delay = float(route.get("sustainability_metrics", {}).get("traffic_delay_min", 0.0) or 0.0)
    reserve_pct = float(route["soc"].get("effective_reserve_pct", route["soc"].get("reserve_soc_pct", 20.0)))
    end_soc_pct = float(route["soc"]["end_soc_pct"])
    soc_margin = max(0.0, end_soc_pct - reserve_pct)
    margin_factor = min(1.0, soc_margin / 18.0)
    traffic_factor = max(0.0, 1.0 - min(traffic_delay, 6.0) / 6.0)
    score_pct = round(100.0 * ((0.50 * closeness) + (0.20 * front_factor) + (0.15 * margin_factor) + (0.15 * traffic_factor)), 1)
    return {
        "confidence_score_pct": score_pct,
        "confidence_label": _confidence_label_from_pct(score_pct),
        "eta_confidence_label": _eta_confidence_label(traffic_delay),
    }


def _traffic_delay_minutes(route: dict) -> float:
    labels = set(route.get("route_labels", []) or [])
    if "DEFAULT_ROUTE" in labels:
        return 0.0
    if "DEFAULT_ROUTE_ALTERNATE" in labels:
        return 2.0
    return 1.0


def _build_charge_policy(
    selected_trip: str,
    best_route: dict,
    all_ranked_routes: List[dict],
    start_soc_pct: float,
) -> dict:
    reverse_trip = (
        "Hunters Hill → Macquarie University"
        if selected_trip == "Macquarie University → Hunters Hill"
        else "Macquarie University → Hunters Hill"
    )
    reverse_candidates = [r for r in all_ranked_routes if r.get("saved_trip") == reverse_trip]
    worst_case_energy = max(
        [float(best_route["energy"]["total_kwh"])] + [float(r["energy"]["total_kwh"]) for r in reverse_candidates]
    )
    battery = DEFAULT_SHUTTLE["usable_battery_kwh"]
    projected_round_trip_drop_pct = ((float(best_route["energy"]["total_kwh"]) + worst_case_energy) / battery) * 100.0
    projected_round_trip_end_soc_pct = max(0.0, float(start_soc_pct) - projected_round_trip_drop_pct)
    trip_end_soc_pct = float(best_route["soc"]["end_soc_pct"])
    remaining_trips = int(best_route["soc"].get("remaining_trips_before_charge", 0) or 0)

    charge_required = (
        projected_round_trip_end_soc_pct < UNIVERSITY_CHARGE_LIMIT_SOC_PCT or
        trip_end_soc_pct < UNIVERSITY_CHARGE_LIMIT_SOC_PCT
    )
    charge_recommended_soon = (
        not charge_required and (
            projected_round_trip_end_soc_pct < (UNIVERSITY_CHARGE_LIMIT_SOC_PCT + 10.0) or
            trip_end_soc_pct < (UNIVERSITY_CHARGE_LIMIT_SOC_PCT + 8.0) or
            remaining_trips <= 1
        )
    )

    if charge_required:
        status = "critical"
        status_label = "Charge before departure"
        banner_message = "Charge at Macquarie University before departure."
    elif charge_recommended_soon:
        status = "warning"
        status_label = "Charge recommended soon"
        banner_message = "Trip is feasible, but charging at Macquarie University is recommended soon."
    else:
        status = "safe"
        status_label = "Safe to start"
        banner_message = "Current SOC is acceptable for the selected trip."

    return {
        "status": status,
        "status_label": status_label,
        "banner_message": banner_message,
        "charge_limit_soc_pct": round(UNIVERSITY_CHARGE_LIMIT_SOC_PCT, 1),
        "reserve_soc_pct": round(DEFAULT_SHUTTLE["reserve_soc_pct"], 1),
        "charge_required_before_departure": charge_required,
        "charge_recommended_soon": charge_recommended_soon,
        "can_complete_worst_case_round_trip_from_university": not charge_required,
        "projected_round_trip_end_soc_pct": round(projected_round_trip_end_soc_pct, 1),
        "trip_end_soc_pct": round(trip_end_soc_pct, 1),
        "remaining_trips_before_charge": remaining_trips,
        "departure_warning": banner_message,
    }


def _build_sequential_plan(
    saved_trip: str,
    ranked_routes: List[dict],
    requested_legs: int,
    start_soc_pct: float,
) -> dict:
    if requested_legs <= 1:
        return {"enabled": False}

    legs = []
    current_trip = saved_trip
    current_soc = float(start_soc_pct)
    for leg_number in range(1, requested_legs + 1):
        route = None
        for r in ranked_routes:
            if r.get("saved_trip") == current_trip:
                route = r
                break
        if route is None:
            break

        leg_soc = soc_after_trip(float(route["energy"]["total_kwh"]), start_soc_pct=current_soc)
        needs_charge = leg_soc["end_soc_pct"] < UNIVERSITY_CHARGE_LIMIT_SOC_PCT and current_trip.endswith("Macquarie University")
        legs.append({
            "leg_number": leg_number,
            "saved_trip": current_trip,
            "start_soc_pct": leg_soc["start_soc_pct"],
            "end_soc_pct": leg_soc["end_soc_pct"],
            "travel_time_min": round(float(route["duration_s"]) / 60.0, 1),
            "energy_kwh": round(float(route["energy"]["total_kwh"]), 2),
            "departure_warning": "Charge at Macquarie University before next departure." if needs_charge else "",
        })
        current_soc = 90.0 if needs_charge else float(leg_soc["end_soc_pct"])
        current_trip = (
            "Hunters Hill → Macquarie University"
            if current_trip == "Macquarie University → Hunters Hill"
            else "Macquarie University → Hunters Hill"
        )

    return {
        "enabled": True,
        "requested_legs": requested_legs,
        "next_saved_trip": current_trip,
        "next_start_soc_pct": round(current_soc, 1),
        "needs_charge_at_university_before_next_departure": current_trip == "Macquarie University → Hunters Hill" and current_soc < UNIVERSITY_CHARGE_LIMIT_SOC_PCT,
        "legs": legs,
    }


def _adaptive_route_weights(routes: List[dict], current_soc_pct: float | None = None) -> tuple[Dict[str, float], str]:
    weights = {
        "travel_time_min": 0.24,
        "distance_km": 0.10,
        "total_kwh": 0.28,
        "soc_deficit_pct": 0.12,
        "toll_penalty": 0.08,
        "traffic_delay_min": 0.10,
        "charge_risk_penalty": 0.08,
    }

    if not routes:
        return weights, "Balanced multi-objective ranking"

    avg_traffic_delay = sum(_traffic_delay_minutes(r) for r in routes) / len(routes)
    min_end_soc = min(float(r["soc"]["end_soc_pct"]) for r in routes)
    current_soc = float(current_soc_pct) if current_soc_pct is not None else float(routes[0]["soc"]["start_soc_pct"])

    notes = ["Balanced multi-objective ranking"]

    if current_soc <= 35.0 or min_end_soc <= (UNIVERSITY_CHARGE_LIMIT_SOC_PCT + 8.0):
        weights.update({
            "travel_time_min": 0.18,
            "distance_km": 0.08,
            "total_kwh": 0.32,
            "soc_deficit_pct": 0.14,
            "toll_penalty": 0.06,
            "traffic_delay_min": 0.08,
            "charge_risk_penalty": 0.14,
        })
        notes.append("SOC-protective weighting active")

    elif avg_traffic_delay >= 1.5:
        weights.update({
            "travel_time_min": 0.28,
            "distance_km": 0.09,
            "total_kwh": 0.24,
            "soc_deficit_pct": 0.11,
            "toll_penalty": 0.07,
            "traffic_delay_min": 0.15,
            "charge_risk_penalty": 0.06,
        })
        notes.append("Traffic-responsive weighting active")

    total = sum(weights.values())
    weights = {k: v / total for k, v in weights.items()}
    return weights, "; ".join(notes)


def _route_cost_vector(route: dict) -> Dict[str, float]:
    travel_time_min = float(route["duration_s"]) / 60.0
    distance_km = float(route["distance_m"]) / 1000.0
    total_kwh = float(route["energy"]["total_kwh"])
    end_soc_pct = float(route["soc"]["end_soc_pct"])
    reserve_pct = float(route["soc"].get("effective_reserve_pct", route["soc"]["reserve_soc_pct"]))
    soc_deficit_pct = max(0.0, reserve_pct - end_soc_pct)
    toll_penalty = 1.0 if route.get("toll_status") else 0.0
    traffic_delay_min = _traffic_delay_minutes(route)
    charge_risk_penalty = max(0.0, UNIVERSITY_CHARGE_LIMIT_SOC_PCT - end_soc_pct)

    return {
        "travel_time_min": travel_time_min,
        "distance_km": distance_km,
        "total_kwh": total_kwh,
        "soc_deficit_pct": soc_deficit_pct,
        "toll_penalty": toll_penalty,
        "traffic_delay_min": traffic_delay_min,
        "charge_risk_penalty": charge_risk_penalty,
    }


def _is_route_feasible(route: dict) -> bool:
    end_soc_pct = float(route["soc"]["end_soc_pct"])
    reserve_pct = float(route["soc"].get("effective_reserve_pct", route["soc"]["reserve_soc_pct"]))
    return end_soc_pct >= reserve_pct


def _dominates(cost_a: Dict[str, float], cost_b: Dict[str, float]) -> bool:
    keys = list(cost_a.keys())
    no_worse = all(cost_a[k] <= cost_b[k] for k in keys)
    strictly_better = any(cost_a[k] < cost_b[k] for k in keys)
    return no_worse and strictly_better


def _non_dominated_fronts(routes: List[dict]) -> List[List[dict]]:
    remaining = list(routes)
    fronts: List[List[dict]] = []

    while remaining:
        front = []
        for candidate in remaining:
            candidate_cost = _route_cost_vector(candidate)
            dominated = False

            for other in remaining:
                if other is candidate:
                    continue
                other_cost = _route_cost_vector(other)
                if _dominates(other_cost, candidate_cost):
                    dominated = True
                    break

            if not dominated:
                front.append(candidate)

        if not front:
            front = [remaining[0]]

        fronts.append(front)
        front_ids = {id(r) for r in front}
        remaining = [r for r in remaining if id(r) not in front_ids]

    return fronts


def _topsis_closeness(routes: List[dict], weights: Dict[str, float]) -> Dict[int, float]:
    if not routes:
        return {}

    criteria = list(weights.keys())
    cost_rows = [_route_cost_vector(r) for r in routes]

    denom = {}
    for c in criteria:
        denom[c] = math.sqrt(sum((row[c] ** 2) for row in cost_rows))
        if denom[c] == 0:
            denom[c] = 1.0

    weighted_rows = []
    for row in cost_rows:
        weighted = {}
        for c in criteria:
            weighted[c] = (row[c] / denom[c]) * weights[c]
        weighted_rows.append(weighted)

    ideal_best = {c: min(row[c] for row in weighted_rows) for c in criteria}
    ideal_worst = {c: max(row[c] for row in weighted_rows) for c in criteria}

    closeness: Dict[int, float] = {}
    for route, row in zip(routes, weighted_rows):
        d_best = math.sqrt(sum((row[c] - ideal_best[c]) ** 2 for c in criteria))
        d_worst = math.sqrt(sum((row[c] - ideal_worst[c]) ** 2 for c in criteria))

        if d_best + d_worst == 0:
            coeff = 1.0
        else:
            coeff = d_worst / (d_best + d_worst)

        closeness[id(route)] = round(coeff, 6)

    return closeness


def _assign_rank_metadata(
    fronts: List[List[dict]],
    fastest_route_only: bool,
    weights: Dict[str, float] | None = None,
    recommendation_basis: str | None = None,
) -> List[dict]:
    ranked: List[dict] = []

    if fastest_route_only:
        ordered = sorted(fronts[0] if fronts else [], key=lambda r: float(r["duration_s"]))
        for pos, route in enumerate(ordered, start=1):
            item = dict(route)
            item["pareto_front_rank"] = 1
            item["topsis_closeness"] = round(
                1.0 if len(ordered) == 1 else 1.0 - ((pos - 1) / (len(ordered) - 1)),
                6,
            )
            item["score"] = round(float(route["duration_s"]) / 60.0, 6)
            item["route_sustainability_index"] = round(
                1.0 if len(ordered) == 1 else max(0.30, 1.0 - 0.70 * ((pos - 1) / (len(ordered) - 1))),
                3,
            )
            item["recommendation_basis"] = recommendation_basis or "Fastest feasible route selection"
            ranked.append(item)
        return ranked

    weights = weights or {
        "travel_time_min": 0.24,
        "distance_km": 0.10,
        "total_kwh": 0.28,
        "soc_deficit_pct": 0.12,
        "toll_penalty": 0.08,
        "traffic_delay_min": 0.10,
        "charge_risk_penalty": 0.08,
    }

    for front_rank, front in enumerate(fronts, start=1):
        closeness = _topsis_closeness(front, weights=weights)
        ordered_front = sorted(
            front,
            key=lambda r: (-closeness.get(id(r), 0.0), float(r["duration_s"]), float(r["distance_m"])),
        )

        for route in ordered_front:
            c = closeness.get(id(route), 0.0)
            item = dict(route)
            item["pareto_front_rank"] = front_rank
            item["topsis_closeness"] = round(c, 6)
            item["score"] = round((front_rank - 1) + (1.0 - c), 6)

            front_factor = 1.0 / front_rank
            sustainability_index = 0.20 + 0.80 * ((0.65 * c) + (0.35 * front_factor))
            item["route_sustainability_index"] = round(min(1.0, max(0.20, sustainability_index)), 3)

            item["recommendation_basis"] = recommendation_basis or (
                "Constrained multi-objective ranking using Pareto-front screening "
                "followed by TOPSIS compromise selection across travel time, "
                "distance, total energy, SOC reserve compliance, toll exposure and traffic delay"
            )
            ranked.append(item)

    return ranked

def _rank_routes_balanced(
    routes: List[dict],
    fastest_route_only: bool,
    current_soc_pct: float | None = None,
) -> List[dict]:
    if not routes:
        return []

    feasible_routes = [r for r in routes if _is_route_feasible(r)]
    working_routes = feasible_routes if feasible_routes else list(routes)

    if fastest_route_only:
        ordered = sorted(working_routes, key=lambda r: float(r["duration_s"]))
        return _assign_rank_metadata(
            [ordered],
            fastest_route_only=True,
            recommendation_basis="Fastest feasible route selection",
        )

    adaptive_weights, recommendation_basis = _adaptive_route_weights(
        working_routes,
        current_soc_pct=request_data.get("current_soc_pct"),
    )

    fronts = _non_dominated_fronts(working_routes)
    ranked = _assign_rank_metadata(
        fronts,
        fastest_route_only=False,
        weights=adaptive_weights,
        recommendation_basis=(
            "Constrained multi-objective ranking using Pareto-front screening "
            "followed by adaptive TOPSIS compromise selection across travel time, "
            "distance, total energy, SOC reserve compliance, toll exposure and traffic delay. "
            f"{recommendation_basis}"
        ),
    )
    ranked.sort(key=lambda x: (x["pareto_front_rank"], x["score"]))

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
    current_soc_pct: float | None = None,
    saved_trip: str | None = None,
) -> dict:
    energy = route_energy_breakdown(route_row=route, passengers=passengers, depart_dt=depart_dt)
    soc = soc_after_trip(energy["total_kwh"], start_soc_pct=current_soc_pct)

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
    traffic_delay_min = _traffic_delay_minutes(route)
    sustainability_metrics["traffic_delay_min"] = round(traffic_delay_min, 1)
    sustainability_metrics["traffic_level"] = (
        "Low" if traffic_delay_min <= 0.5 else "Medium" if traffic_delay_min <= 2.0 else "High"
    )

    enriched = dict(route)
    enriched["energy"] = energy
    enriched["soc"] = soc
    enriched["sustainability_metrics"] = sustainability_metrics
    enriched["stop_arrivals"] = stop_arrivals
    enriched["stop_indices"] = stop_indices
    if saved_trip is not None:
        enriched["saved_trip"] = saved_trip
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
        "traction_base_kwh": route["energy"]["traction_base_kwh"],
        "rolling_kwh": route["energy"].get("rolling_kwh"),
        "aero_kwh": route["energy"].get("aero_kwh"),
        "stop_start_kwh": route["energy"].get("stop_start_kwh"),
        "stop_start_events": route["energy"].get("stop_start_events"),
        "slope_uphill_kwh": route["energy"]["slope_uphill_kwh"],
        "slope_regen_kwh": route["energy"]["slope_regen_kwh"],
        "slope_net_kwh": route["energy"]["slope_net_kwh"],
        "elevation_api_used": route["energy"]["elevation_api_used"],
        "hvac_kwh": route["energy"]["hvac_kwh"],
        "idling_kwh": route["energy"].get("idling_kwh"),
        "hvac_kw_est": route["energy"]["hvac_kw_est"],
        "outdoor_temp_c": route["energy"]["outdoor_temp_c"],
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
        "traffic_delay_min": route["sustainability_metrics"].get("traffic_delay_min", 0.0),
        "traffic_level": route["sustainability_metrics"].get("traffic_level", "Unknown"),
        "route_sustainability_index": route.get("route_sustainability_index", route.get("sustainability_score", 0.75)),
        "score": round(route["score"], 4),
        "pareto_front_rank": route.get("pareto_front_rank"),                
        "topsis_closeness": route.get("topsis_closeness"),
        "confidence_score_pct": route.get("confidence_score_pct"),
        "confidence_label": route.get("confidence_label"),
        "eta_confidence_label": route.get("eta_confidence_label"),
        "recommendation_basis": route.get("recommendation_basis", ""),
        "route_points": [
            {"lat": float(lat), "lng": float(lng)}
            for lat, lng in route.get("path_points", [])
        ],
        "step_details": [
            {
                "instruction": sd.get("instruction", ""),
                "distance_m": round(float(sd.get("distance_m", 0.0)), 1),
                "route_index": int(sd.get("route_index", 0)),
            }
            for sd in route.get("step_details", [])
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
    step_details: List[dict] = []
    leg_durations_s: List[float] = []
    leg_distances_m: List[float] = []
    toll_status = False

    point_offset = 0

    for leg_idx, leg in enumerate(legs):
        leg_points = leg.get("path_points", []) or []
        leg_steps = leg.get("steps", []) or []
        leg_step_details = leg.get("step_details", []) or []

        steps.extend(leg_steps)
        leg_durations_s.append(float(leg.get("duration_s", 0.0)))
        leg_distances_m.append(float(leg.get("distance_m", 0.0)))
        toll_status = toll_status or bool(leg.get("toll_status"))

        for sd in leg_step_details:
            item = dict(sd)
            item["route_index"] = int(item.get("route_index", 0)) + point_offset
            step_details.append(item)

        if leg_idx == 0:
            point_offset += len(leg_points)
        else:
            point_offset += max(0, len(leg_points) - 1)

    return {
        "route_id": route_id,
        "duration_s": duration_s,
        "distance_m": distance_m,
        "path_points": path_points,
        "toll_status": toll_status,
        "steps": steps,
        "step_details": step_details,
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
                current_soc_pct=request_data.get("current_soc_pct"),
                saved_trip=saved_trip,
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
            r.update(_route_confidence_payload(r))
            r.setdefault("route_sustainability_index", r.get("sustainability_score", 0.75))

        
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
                        current_soc_pct=request_data.get("current_soc_pct"),
                        saved_trip=saved_trip,
                    )
                    for r in combined_alt_routes_raw
                ]

                ranked_stop_routes = _rank_routes_balanced(
                    enriched_combined_routes,
                    fastest_route_only=False,
                    current_soc_pct=request_data.get("current_soc_pct"),
                )

                for idx, r in enumerate(ranked_stop_routes, start=1):
                    r["route_id"] = f"R{idx}"
                    r["arrival_time"] = (
                        depart_dt + datetime.timedelta(
                            seconds=float(r["duration_s"])
                        )
                    ).strftime("%H:%M")
                    r.update(_route_confidence_payload(r))
                    r.setdefault("route_sustainability_index", r.get("sustainability_score", 0.75))

            except Exception:
                pass

        best = ranked_stop_routes[0]
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
        charge_policy = _build_charge_policy(
            selected_trip=saved_trip,
            best_route=best,
            all_ranked_routes=ranked_stop_routes,
            start_soc_pct=float(best["soc"]["start_soc_pct"]),
        )
        sequential_plan = _build_sequential_plan(
            saved_trip=saved_trip,
            ranked_routes=ranked_stop_routes,
            requested_legs=max(int(request_data.get("trip_number", 1) or 1), 1),
            start_soc_pct=float(best["soc"]["start_soc_pct"]),
        )

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
                "sustainability_score": best.get("route_sustainability_index", best.get("sustainability_score", 0.75)),
                "pareto_front_rank": best.get("pareto_front_rank"),
                "topsis_closeness": best.get("topsis_closeness"),
                "traffic_delay_min": best["sustainability_metrics"].get("traffic_delay_min", 0.0),
                "traffic_level": best["sustainability_metrics"].get("traffic_level", "Unknown"),
                "energy_saving_vs_highest_energy_route_pct": round(saving_vs_highest_pct, 1),
                "confidence_score_pct": best.get("confidence_score_pct"),
                "confidence_label": best.get("confidence_label"),
                "eta_confidence_label": best.get("eta_confidence_label"),
                "ranking_basis": best.get("recommendation_basis"),
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
            "step_details": [
                {
                    "instruction": sd.get("instruction", ""),
                    "distance_m": round(float(sd.get("distance_m", 0.0)), 1),
                    "route_index": int(sd.get("route_index", 0)),
                }
                for sd in best.get("step_details", [])
            ],
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
            "route_sustainability_index": best.get("route_sustainability_index", best.get("sustainability_score", 0.75)),
            "avg_speed_kmh": best["sustainability_metrics"]["avg_speed_kmh"],
            "remaining_trips_before_charge": best["soc"]["remaining_trips_before_charge"],
            "recommended_route": best["route_id"],
            "recommendation_basis": best["recommendation_basis"],
            "energy_model_calibration": ENERGY_MODEL_CALIBRATION,
            "confidence_score_pct": best.get("confidence_score_pct"),
            "confidence_label": best.get("confidence_label"),
            "eta_confidence_label": best.get("eta_confidence_label"),
            "charge_policy": charge_policy,
            "sequential_plan": sequential_plan,
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
            "step_details": [],
        }

        enriched = _enrich_route_metrics(
            route=fallback_route,
            passengers=passengers,
            depart_dt=depart_dt,
            selected_stops=selected_stops,
            current_soc_pct=request_data.get("current_soc_pct"),
            saved_trip=saved_trip,
        )
        enriched["arrival_time"] = (depart_dt + datetime.timedelta(seconds=duration_s)).strftime("%H:%M")

        queue_risk = "Unknown"
        enriched.update(_route_confidence_payload(enriched))
        enriched["route_sustainability_index"] = 0.75
        charging_schedule = _charging_schedule_from_soc(enriched["soc"]["end_soc_pct"], 0)
        charge_policy = _build_charge_policy(
            selected_trip=saved_trip,
            best_route=enriched,
            all_ranked_routes=[enriched],
            start_soc_pct=float(enriched["soc"]["start_soc_pct"]),
        )
        sequential_plan = _build_sequential_plan(
            saved_trip=saved_trip,
            ranked_routes=[enriched],
            requested_legs=max(int(request_data.get("trip_number", 1) or 1), 1),
            start_soc_pct=float(enriched["soc"]["start_soc_pct"]),
        )

        return {
            "selected_route": {
                "route_id": "R1",
                "travel_time_min": round(duration_min, 1),
                "distance_km": round(total_distance_km, 2),
                "arrival_time": arrival_dt.strftime("%H:%M"),
                "tolls": "Unknown",
                "sustainability_score": 0.75,
                "traffic_delay_min": enriched["sustainability_metrics"].get("traffic_delay_min", 0.0),
                "traffic_level": enriched["sustainability_metrics"].get("traffic_level", "Unknown"),
                "energy_saving_vs_highest_energy_route_pct": 0.0,
                "confidence_score_pct": enriched.get("confidence_score_pct"),
                "confidence_label": enriched.get("confidence_label"),
                "eta_confidence_label": enriched.get("eta_confidence_label"),
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
            "step_details": [],
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
            "energy_model_calibration": ENERGY_MODEL_CALIBRATION,
            "confidence_score_pct": enriched.get("confidence_score_pct"),
            "confidence_label": enriched.get("confidence_label"),
            "eta_confidence_label": enriched.get("eta_confidence_label"),
            "charge_policy": charge_policy,
            "sequential_plan": sequential_plan,
            "all_routes": [
                _route_payload(
                    enriched,
                    route_kind="selected_route",
                    selected_stops=selected_stops,
                )
            ],
            "alternatives_note": "",
        }
