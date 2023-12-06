import json
from datetime import datetime
from flask import Flask, Response
from flask_cors import CORS


from werkzeug.middleware.dispatcher import DispatcherMiddleware
from prometheus_client import make_wsgi_app, Counter, Gauge, generate_latest, REGISTRY

app = Flask(__name__)
CORS(app)

step_counter = Counter("step_count", "step count")
pv_counter = Counter("PV", "page visit number")
gauge = Gauge("step_count_2", "step_count metric in step_count", ["timestamp"])

@app.route("/incCnt", methods=["GET"])
def incr_pv():
    pv_counter.inc()
    return "ok", 200

@app.route("/autoexpo", methods=["GET", "POST"])
def auto_expo():
    healthkit_data = []
    res = []
    with open("sample.json", "r") as f:
        healthkit_data = json.load(f)

    for metric in healthkit_data.get("data", {}).get("metrics", []):
        #
        metric_name = metric.get("name")
        metric_units = metric.get("units")
        for d in metric.get("data"):
            date = d["date"]
            quantity = d["qty"]
            # step_counter.inc(quantity)
            timestamp = datetime.strptime(
                date, "%Y-%m-%d %H:%M:%S %z"
            ).timestamp()
            gauge.labels(timestamp=timestamp).set(quantity)
    return "OK", 200


@app.route("/")
def hello_world():
    # Increment the Counter metric on each request
    requests_counter.inc()

    # Set the value of the Gauge metric
    active_users_gauge.set(43)

    return "Hello, World!"


@app.route("/metrics")
def metrics():
    # Expose metrics in Prometheus format
    return Response(generate_latest(REGISTRY), mimetype="text/plain")


if __name__ == "__main__":
    app.run(debug=True)
# Add prometheus wsgi middleware to route /metrics requests
# app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/metrics": make_wsgi_app()})
