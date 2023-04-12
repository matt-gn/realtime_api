from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import sqlite3

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/maxmin/{variable}")
def maxmin(variable: str):
    if variable in ("temperature", "pressure", "wind_speed", "wind_direction", "humidity"):
        with sqlite3.connect("realtime.db") as connection:
            max = connection.execute(f"SELECT station_name, max({variable}), latitude, longitude FROM aws_realtime").fetchall()[0]
            min = connection.execute(f"SELECT station_name, min({variable}), latitude, longitude FROM aws_realtime").fetchall()[0]
            mean = connection.execute(f"SELECT round(avg({variable}), 2) FROM aws_realtime").fetchall()[0]
        return JSONResponse(content={"max": max, "min": min, "mean": mean})

@app.get("/station_list")
def station_list():
    with sqlite3.connect("realtime.db") as connection:
        query_results = connection.execute("SELECT station_name, region FROM aws_realtime").fetchall()
    sorted_list = sorted(query_results, key=lambda x: x[0])
    sorted_list = sorted(sorted_list, key=lambda x: x[1])
    station_dict = {}
    for station, region in sorted_list:
        station_dict.setdefault(region, []).append(station)
    return JSONResponse(content=station_dict)

@app.get("/station/{station_name}")
def station_data(station_name: str):
    with sqlite3.connect("realtime.db") as connection:
        station_name = station_name.replace('%20', ' ')
        query_results = connection.execute("SELECT * FROM aws_realtime WHERE station_name=?", (station_name,)).fetchall()
        return JSONResponse(content=query_results[0])

@app.get("/daily/{variable}")
def daily(variable: str):
    with sqlite3.connect("realtime.db") as connection:
        daily_aggregates = connection.execute("SELECT * from daily_aggregate_table WHERE variable=?", (variable,)).fetchall()
    max, min, mean = daily_aggregates
    daily_agg_dict = {}
    daily_agg_dict["max"] = (max[4], max[5], max[1])
    daily_agg_dict["min"] = (min[4], min[5], min[1])
    daily_agg_dict["mean"] = (mean[5])
    return JSONResponse(content=daily_agg_dict)

