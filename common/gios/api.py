import requests
import threading
import queue
import json
from typing import List, Optional, Dict, Any


feature_collection: Dict = {
    "type": "FeatureCollection",
    "features": []
}

PM10 = "PM10"
PM25 = "PM2.5"
NO2 = "NO2"

THREAD_COUNT = 4


def fetch_sensors_data_details(sensor_id: str, session: requests.Session) -> float:
    sensor_data = process_request(
        url=f"https://api.gios.gov.pl/pjp-api/rest/data/getData/{sensor_id}", session=session
    )
    result = 0
    c = 0
    for e in sensor_data["values"]:
        if e.get("value"):
            result += round(e["value"], 2)
            c += 1
    return result/c if c != 0 else None


def process_request(url: str, session: Optional[requests.Session] = requests):
    try:
        request = session.get(url=url)
    except requests.RequestException as exc:
        raise exc
    if 200 <= request.status_code < 300:
        try:
            response_data = request.json()
        except (json.JSONDecodeError, json.JSONDecoder) as exc:
            raise exc
    else:
        response_data = {}
    return response_data


def station_find_all(stations, worker_num: int, q: queue) -> None:
    with requests.Session() as session:
        for station in stations:
            station_id = station.get("id")
            sensors = process_request(
                url=f'https://api.gios.gov.pl/pjp-api/rest/station/sensors/{station_id}', session=session
            )
            pm10_id = filter(lambda x: x["id"] if x["param"]["paramCode"] == PM10 else None, sensors)
            try:
                pm10 = fetch_sensors_data_details(next(pm10_id)["id"], session=session)
            except StopIteration:
                continue
            feature: Dict[str, Any[str, Dict[str, str]]] = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [station.get("gegrLon"), station.get("gegrLat")]
                },
                "properties": {
                    "id": station_id,
                    "name": station.get("name"),
                    "PM10": pm10
                }
            }
            feature_collection["features"].append(feature)
    q.task_done()


def chunks(lst: List[dict], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main() -> feature_collection:
    stations = process_request(url='https://api.gios.gov.pl/pjp-api/rest/station/findAll')
    chunked = chunks(stations, round(len(stations)/4))
    q = queue.Queue()
    for i in range(1, 5):
        q.put(("GIOS_API_FETCH", i))
    for i in range(1, THREAD_COUNT+1):
        threading.Thread(
            target=station_find_all, args=(next(chunked), i, q), daemon=True
        ).start()
    q.join()
    return feature_collection
