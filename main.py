from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional

from route_model_runner import run_route_model

app = FastAPI(title="Smart Shuttle Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TripRequest(BaseModel):
    saved_trip: str
    selected_stops: List[str] = Field(default_factory=list)
    trip_type: str = "single"
    passengers: int = 0
    avoid_tolls: bool = False
    nearby_chargers: bool = False
    fastest_route_only: bool = False
    sequential_trips: bool = False
    trip_number: int = 1
    departure_date: str
    departure_time: str
    current_lat: Optional[float] = None
    current_lng: Optional[float] = None

    @field_validator("trip_type")
    @classmethod
    def validate_trip_type(cls, value: str) -> str:
        value = value.strip().lower()
        if value not in {"single", "round"}:
            raise ValueError("trip_type must be 'single' or 'round'")
        return value

    @field_validator("passengers")
    @classmethod
    def validate_passengers(cls, value: int) -> int:
        if value < 0:
            raise ValueError("passengers cannot be negative")
        return value

    @field_validator("trip_number")
    @classmethod
    def validate_trip_number(cls, value: int) -> int:
        if value < 1:
            raise ValueError("trip_number must be at least 1")
        return value

    @field_validator("departure_date")
    @classmethod
    def validate_departure_date(cls, value: str) -> str:
        parts = value.split("/")
        if len(parts) != 3:
            raise ValueError("departure_date must be DD/MM/YYYY")
        return value

    @field_validator("departure_time")
    @classmethod
    def validate_departure_time(cls, value: str) -> str:
        parts = value.split(":")
        if len(parts) != 2:
            raise ValueError("departure_time must be HH:MM")
        return value

    @field_validator("current_lng")
    @classmethod
    def validate_current_lng(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and not (-180.0 <= value <= 180.0):
            raise ValueError("current_lng must be between -180 and 180")
        return value

    @field_validator("current_lat")
    @classmethod
    def validate_current_lat(cls, value: Optional[float]) -> Optional[float]:
        if value is not None and not (-90.0 <= value <= 90.0):
            raise ValueError("current_lat must be between -90 and 90")
        return value

    def to_runner_payload(self) -> dict:
        payload = self.model_dump()

        payload["selected_stops"] = [s.strip() for s in self.selected_stops if s.strip()]
        payload["saved_trip"] = self.saved_trip.strip()

        if payload["current_lat"] is None or payload["current_lng"] is None:
            payload["current_lat"] = None
            payload["current_lng"] = None

        return payload


@app.get("/")
def root():
    return {
        "message": "Backend is running",
        "service": "Smart Shuttle Backend",
        "status": "ok",
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
    }


def _run_request(data: TripRequest, endpoint_name: str):
    try:
        payload = data.to_runner_payload()
        result = run_route_model(payload)
        return result
    except Exception as e:
        import traceback
        print(f"\n========== {endpoint_name} ERROR TRACE ==========")
        traceback.print_exc()
        print("=" * (len(endpoint_name) + 31) + "\n")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/plan-route")
def plan_route(data: TripRequest):
    return _run_request(data, "PLAN-ROUTE")


@app.post("/reroute")
def reroute(data: TripRequest):
    if data.current_lat is None or data.current_lng is None:
        raise HTTPException(
            status_code=400,
            detail="current_lat and current_lng are required for /reroute",
        )
    return _run_request(data, "REROUTE")


@app.post("/traffic-refresh")
def traffic_refresh(data: TripRequest):
    return _run_request(data, "TRAFFIC-REFRESH")