from flask import Flask, request, send_from_directory
from flask_cors import CORS
from flask_restful import Resource, Api
from tex_mysql_client import TexMySQLClient
import logging
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',
                    filename='./app.log', filemode='w')

application = Flask(__name__)
CORS(application)
api = Api(application)

texDbClient = TexMySQLClient()


@application.route('/online/')
def test_server_online():
    return "Server is online"


@application.route('/docx/')
def download_docx_file():
    return send_from_directory(os.path.join(application.root_path), "test.docx", as_attachment=True, attachment_filename="input.docx")


class TexProduct(Resource):
    def get(self, productId):
        return texDbClient.get_product_by_id(productId)

    def put(self, productId):
        productInfo = request.get_json(force=True)
        if productInfo['id'] != int(productId):
            return "Error: Invalid request, reference to different object found in request.body['id'] param than productId in URL", 405
        else:
            return texDbClient.update_product_by_id(productInfo)

    def delete(self, productId):
        return texDbClient.delete_product_by_id(productId)


class TexProducts(Resource):
    def get(self):
        return texDbClient.get_all_products()

    def post(self):
        productInfo = request.get_json(force=True)
        return texDbClient.insert_product_in_products_table(productInfo)


api.add_resource(TexProduct, '/tex/<string:productId>')
api.add_resource(TexProducts, '/tex/')

if __name__ == "__main__":
    # texDbClient.create_products_table_if_not_exists()
    application.debug = True
    application.run(host="0.0.0.0", port=8000)
