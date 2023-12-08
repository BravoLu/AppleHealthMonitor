import json
import sys
import logging
from datetime import datetime
from flask import request, Flask
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

DATAPOINTS_CHUNK = 80000
# Log to a file
logging.basicConfig(filename='./logs/flask.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("file-output")

app = Flask(__name__)
app.debug = True
bucket = "AutoExporter"
token = "fM2Ti-w-S3BYVFb7nD-JdCXI-vkdJ3xlJvN3OWoyfAuXPq_Glwje3zKTt8pZM1QFZpLSHE9v4CVKwaiKZ4_lSg=="
org = "BravoStudio"
url = "http://localhost:8086"

write_client = InfluxDBClient(url=url, token=token, org=org)
write_api = write_client.write_api(write_options=SYNCHRONOUS)

@app.route("/collect", methods=["POST", "GET"])
def collect():
    logger.info(f"Request received")
    healthkit_data = None
    transformed_data = []
    try:
        # load from json file, for debug
        with open("./example/sample.json", "r") as f:
            healthkit_data = json.load(f)
        # healthkit_data = json.loads(request.data)
    except:
        return "Invalid JSON Received", 400
    try:
        for metric in healthkit_data["data"]["metrics"]:
            number_fields = []
            string_fields = []
            for datapoint in metric["data"]:
                metric_fields = set(datapoint.keys())  # ("qty", "date")
                metric_fields.remove("date")  # ("qty")
                for mfield in metric_fields:  # metric_fields = ["qty"]
                    if (
                        type(datapoint[mfield]) == int
                        or type(datapoint[mfield]) == float
                    ):
                        number_fields.append(mfield)
                    else:
                        string_fields.append(mfield)
                ts = datetime.strptime(datapoint["date"], "%Y-%m-%d %H:%M:%S %z")
                point = Point(metric["name"]).time(ts, WritePrecision.MS)
                for nfield in string_fields:
                    point = point.tag(str(nfield), str(datapoint[nfield]))
                for nfield in number_fields:
                    point = point.field(str(nfield), float(datapoint[nfield]))

                transformed_data.append(point)
                number_fields.clear()
                string_fields.clear()

        for i in range(0, len(transformed_data), DATAPOINTS_CHUNK):
            write_api.write(
                bucket=bucket,
                org=org,
                record=transformed_data[i : i + DATAPOINTS_CHUNK],
            )
        logger.info(f"DB Metrics Write Complete")
    except:
        logger.exception("Caught Exception. See stacktrace for details.")
        return "Server Error", 500
    return "Success", 200

if __name__ == "__main__":
    # host to set as "0.0.0.0" is neccessary.
    app.run(host="0.0.0.0", debug=True, port=9000)
