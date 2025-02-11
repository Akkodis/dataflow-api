#!/usr/bin/env python3

import connexion

from swagger_server import encoder

from waitress import serve

app = connexion.App(__name__, specification_dir='./swagger/')
app.app.json_encoder = encoder.JSONEncoder
app.add_api('swagger.yaml', arguments={'title': 'Data flow 5G Meta API'}, pythonic_params=True)

def main():
    # Production
    # serve(app, host="0.0.0.0", port=8080)
    # Development
    app.run(port=8080)

if __name__ == '__main__':
    main()
