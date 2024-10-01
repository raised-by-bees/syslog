from flask import Flask, jsonify
import json
import os

app = Flask(__name__)

# Path to your data file
data_file_path = r"C:\syslog\syslog2.0\memory_usage_data.txt"

@app.route('/data', methods=['GET'])
def get_data():
    try:
        with open(data_file_path, 'r') as file:
            data = [json.loads(line) for line in file.readlines()]
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({"error": "Data file not found"}), 404

if __name__ == '__main__':
    app.run(debug=True, port=5000)
