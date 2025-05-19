from flask import Flask, request, Response
import requests

app = Flask(__name__)

@app.route('/api/datasets')
def proxy():
    url = 'https://huggingface.co/api/datasets'
    # Forward all headers except Host
    headers = {k: v for k, v in request.headers if k.lower() != 'host'}
    # Forward all query params
    resp = requests.get(url, headers=headers, params=request.args)
    # Return the response with status and headers
    return Response(resp.content, status=resp.status_code, headers=dict(resp.headers))

if __name__ == '__main__':
    app.run(port=8080) 