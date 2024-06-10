# Databricks notebook source
# MAGIC %pip install Flask
# MAGIC

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

def embedded_model_function(customer_id, question):
    # Your function logic here
    # For demonstration, let's return some dummy image URLs
    return {
        'images': [
            {'url': 'https://via.placeholder.com/150/0000FF/808080?text=Image1'},
            {'url': 'https://via.placeholder.com/150/FF0000/FFFFFF?text=Image2'},
            {'url': 'https://via.placeholder.com/150/FFFF00/000000?text=Image3'}
        ]
    }


# COMMAND ----------

import threading
from flask import Flask, request, render_template_string, jsonify
from werkzeug.serving import make_server

app = Flask(__name__)

@app.route('/')
def index():
    return render_template_string('''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Product Query</title>
        </head>
        <body>
            <h1>Ask a Question About a Product</h1>
            <form id="query-form">
                <label for="customer_id">Customer ID:</label>
                <input type="text" id="customer_id" name="customer_id" required><br><br>
                <label for="question">Question:</label>
                <textarea id="question" name="question" required></textarea><br><br>
                <button type="submit">Submit</button>
            </form>
            <div id="result"></div>

            <script>
                document.getElementById('query-form').addEventListener('submit', function(event) {
                    event.preventDefault();

                    const formData = new FormData(event.target);
                    const data = {};
                    formData.forEach((value, key) => {
                        data[key] = value;
                    });

                    fetch('/submit', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(data),
                    })
                    .then(response => response.json())
                    .then(data => {
                        const resultDiv = document.getElementById('result');
                        resultDiv.innerHTML = '';
                        data.images.forEach(image => {
                            const img = document.createElement('img');
                            img.src = image.url;
                            resultDiv.appendChild(img);
                        });
                    });
                });
            </script>
        </body>
        </html>
    ''')

@app.route('/submit', methods=['POST'])
def submit():
    data = request.get_json()
    customer_id = data['customer_id']
    question = data['question']

    # Call the embedded model function
    result = embedded_model_function(customer_id, question)

    return jsonify(result)

class ThreadedFlaskServer(threading.Thread):
    def __init__(self, app):
        threading.Thread.__init__(self)
        self.srv = make_server('0.0.0.0', 5000, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()

server = ThreadedFlaskServer(app)
server.start()

print("Flask server started on port 5000...")


# COMMAND ----------

http://dbc-c91a7871-73b3.cloud.databricks.com>/driver-proxy/o/3268838268167459/281535298081063/5000/
#https://dbc-c91a7871-73b3.cloud.databricks.com/settings/user/profile?o=3268838268167459
#https://dbc-c91a7871-73b3.cloud.databricks.com/?o=3268838268167459#notebook/281535298080972/command/281535298081063


# COMMAND ----------

@app.route('/shutdown', methods=['POST'])
def shutdown():
    server.shutdown()
    return 'Server shutting down...'

