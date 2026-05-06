from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from route_model_runner import run_route_model

app = FastAPI(title="Smart Shuttle Router Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "ok", "message": "Smart Shuttle Router backend is running"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/plan-route")
def plan_route(request_data: dict):
    return run_route_model(request_data)

@app.post("/reroute")
def reroute(request_data: dict):
    return run_route_model(request_data)

@app.post("/traffic-refresh")
def traffic_refresh(request_data: dict):
    return run_route_model(request_data)

@app.post("/route")
def route_alias(request_data: dict):
    return run_route_model(request_data)

@app.post("/calculate-route")
def calculate_route_alias(request_data: dict):
    return run_route_model(request_data)

@app.post("/plan-trip")
def plan_trip_alias(request_data: dict):
    return run_route_model(request_data)
