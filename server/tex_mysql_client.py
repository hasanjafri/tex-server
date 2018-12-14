from datetime import datetime
from docx import Document
from flask import make_response, jsonify
import logging
import sqlalchemy.pool as pool
import pymysql
import pytz


class TexMySQLClient():
    def __init__(self, *args, **kwargs):
        self.dbPool = pool.QueuePool(
            self.connect_to_db, max_overflow=10, pool_size=5)

    def connect_to_db(self):
        try:
            dbConnection = pymysql.connect(
                host="localhost", user="root", password="dabura45", db="texdb", autocommit=True)
        except pymysql.Error as e:
            return make_response(jsonify(Error="Error %d: %s" % (e.args[0], e.args[1])), 400)

        return dbConnection

    # def create_db(self):
    #     try:
    #         with self.dbConnection.cursor() as cursor:
    #             cursor.execute("CREATE DATABASE IF NOT EXISTS texdb")
    #         self.dbConnection.commit()
    #     except Exception as e:
    #         print("Error: %s" % (e))
    #     finally:
    #         self.dbConnection.close()

    #     self.dbConnection = pymysql.connect(host="localhost",)

    def get_product_by_id(self, productId):
        if not productId:
            return make_response(jsonify(Error="Error: no product id provided"), 400)

        try:
            conn = self.dbPool.connect()
            with conn.cursor() as cursor:
                sql = "SELECT * FROM productrecords WHERE `id` = %s"
                cursor.execute(sql, (productId))
                response = cursor.fetchone()
        except pymysql.Error as e:
            return make_response(jsonify(Error="Error %d: %s" % (e.args[0], e.args[1])), 400)
        finally:
            conn.close()

        print(response)

        if response != None:
            return make_response(jsonify(product=self.generate_responses_dict(response)), 200)
        else:
            return make_response(jsonify(Error="Error: no product found with productId: {}".format(productId)), 404)

    def get_all_products(self):
        try:
            conn = self.dbPool.connect()
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM productrecords")
                results = cursor.fetchall()
        except pymysql.Error as e:
            return make_response(jsonify(Error="Error %d: %s" % (e.args[0], e.args[1])), 400)
        finally:
            conn.close()

        if results != None:
            return self.generate_responses_dict(results)
        else:
            return make_response(jsonify(Error="Error: no products found in this table"), 404)

    def update_product_by_id(self, productInfo):
        if not productInfo:
            return make_response(jsonify(Error="Error: no productInfo provided in body to update resource"), 400)

        try:
            conn = self.dbPool.connect()
            with conn.cursor() as cursor:
                sql = "UPDATE productrecords SET p_id=%s, p_description=%s, p_datetime=%s, longitude=%s, latitude=%s, elevation=%s WHERE id=%s"
                cursor.execute(
                    sql, self.generate_product_info_list(productInfo))
        except pymysql.Error as e:
            return make_response(jsonify(Error="Error %d: %s" % (e.args[0], e.args[1])), 400)
        finally:
            conn.close()

        return make_response(jsonify(resp="Successfully updated Product with ID: {}".format(productInfo['id'])), 200)

    def delete_product_by_id(self, productId):
        if not productId:
            return make_response(jsonify(Error="Error: No productId provided to identify resource to DELETE"), 400)

        try:
            conn = self.dbPool.connect()
            with conn.cursor() as cursor:
                sql = "DELETE FROM productrecords WHERE `id` = %s"
                cursor.execute(sql, (productId))
        except pymysql.Error as e:
            return make_response(jsonify(Error="Error %d: %s" % (e.args[0], e.args[1])), 400)
        finally:
            conn.close()

        return make_response(jsonify(resp="Resource with productId : {} was successfully deleted".format(productId)), 200)

    def create_products_table_if_not_exists(self):
        try:
            conn = self.dbPool.connect()
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE IF NOT EXISTS productrecords (id INT AUTO_INCREMENT PRIMARY KEY, p_id INT NOT NULL, p_description VARCHAR(32) NOT NULL, p_datetime TIMESTAMP NOT NULL, longitude DECIMAL(10,7) NOT NULL, latitude DECIMAL(9,7) NOT NULL, elevation INT NOT NULL)")
        except pymysql.Error as e:
            return make_response(jsonify(Error="Error %d: %s" % (e.args[0], e.args[1])), 400)
        finally:
            conn.close()

    def generate_datetime_from_str(self, date_str):
        date_str = date_str.split("T")
        date = date_str[0].split("-")
        time = date_str[1].split(":")
        date_ts = datetime(int(date[0]), int(date[1]), int(date[2]), int(
            time[0]), int(time[1]))
        return date_ts

    def convert_datetime_to_original_date_str(self, datetimeObj):
        myTimezone = pytz.timezone("EST")
        datetimeStr = str(myTimezone.localize(datetimeObj, is_dst=None))
        return datetimeStr.replace(" ", "T")

    def insert_product_in_products_table(self, product_info):
        if "p_id" not in product_info:
            return make_response(jsonify(Error="Error: no id provided for this product"), 400)
        if "p_description" not in product_info:
            return make_response(jsonify(Error="Error: no description provided for this product"), 400)
        if "p_datetime" not in product_info:
            return make_response(jsonify(Error="Error: no datetime provided for this product"), 400)
        if "longitude" not in product_info:
            return make_response(jsonify(Error="Error: no longitude provided for this product"), 400)
        if "latitude" not in product_info:
            return make_response(jsonify(Error="Error: no latitude provided for this product"), 400)
        if "elevation" not in product_info:
            return make_response(jsonify(Error="Error: no elevation provided for this product"), 400)

        try:
            conn = self.dbPool.connect()
            with conn.cursor() as cursor:
                sql = "INSERT INTO productrecords (p_id, p_description, p_datetime, longitude, latitude, elevation) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(
                    sql, self.generate_product_info_list(product_info))
        except pymysql.Error as e:
            return make_response(jsonify(Error="Error %d: %s" % (e.args[0], e.args[1])), 400)
        finally:
            conn.close()

        return make_response(jsonify(resp="Product with description: {} was successfully added!".format(product_info['p_description'])), 200)

    def generate_responses_dict(self, productInfoList):
        productsList = []
        for product in productInfoList:
            if isinstance(product, tuple):
                productInfo = {"id": product[0], "p_id": product[1], "p_description": product[2], "p_datetime":
                               str(product[3]).replace(" ", "T"), "longitude": str(product[4]), "latitude": str(product[5]), "elevation": product[6]}
                productsList.append(productInfo)
            else:
                return {"id": productInfoList[0], "p_id": productInfoList[1], "p_description": productInfoList[2], "p_datetime":
                        str(productInfoList[3]), "longitude": str(productInfoList[4]), "latitude": str(productInfoList[5]), "elevation": productInfoList[6]}
        return productsList

    def generate_docx_dict(self):
        try:
            conn = self.dbPool.connect()
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM productrecords")
                results = cursor.fetchall()
        except pymysql.Error as e:
            return make_response(jsonify(Error="Error %d: %s" % (e.args[0], e.args[1])), 400)
        finally:
            conn.close()

        if results != None:
            productsList = []
            for product in results:
                if isinstance(product, tuple):
                    productInfo = {"id": product[0], "p_id": product[1], "p_description": product[2], "p_datetime":
                                   self.convert_datetime_to_original_date_str(product[3]), "longitude": str(product[4]), "latitude": str(product[5]), "elevation": product[6]}
                    productsList.append(productInfo)
                else:
                    return {"id": results[0], "p_id": results[1], "p_description": results[2], "p_datetime":
                            self.convert_datetime_to_original_date_str(results[3]), "longitude": str(results[4]), "latitude": str(results[5]), "elevation": results[6]}
            return productsList

    def generate_product_info_list(self, product_info):
        if 'id' in product_info:
            return (product_info['p_id'], product_info['p_description'], self.generate_datetime_from_str(product_info['p_datetime']), product_info['longitude'], product_info['latitude'], product_info['elevation'], product_info['id'])
        else:
            return (product_info['p_id'], product_info['p_description'], self.generate_datetime_from_str(product_info['p_datetime']), product_info['longitude'], product_info['latitude'], product_info['elevation'])

    def generate_docx_table(self):
        products = self.generate_docx_dict()
        print(products)
        docxTable = Document()
        table = docxTable.add_table(rows=1, cols=6)
        table.style = 'LightShading-Accent1'
        hdr_cells = table.rows[0].cells
        hdr_cells[0].text = 'id'
        hdr_cells[1].text = 'description'
        hdr_cells[2].text = 'datetime'
        hdr_cells[3].text = 'longitude'
        hdr_cells[4].text = 'latitude'
        hdr_cells[5].text = 'elevation'

        for product in range(len(products)):
            row_cells = table.add_row().cells
            row_cells[0].text = str(products[product]['p_id'])
            row_cells[1].text = str(products[product]['p_description'])
            row_cells[2].text = str(products[product]['p_datetime'])
            row_cells[3].text = str(products[product]['longitude'])
            row_cells[4].text = str(products[product]['latitude'])
            row_cells[5].text = str(products[product]['elevation'])

        docxTable.save('test.docx')

        return docxTable
