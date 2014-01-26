__author__ = 'danielkershaw'
from pprint import pprint
import pymongo
from pymongo import MongoClient
from Geo import Geo

if __name__ == '__main__':
    g = Geo()
    client = MongoClient('mongodb://apple:apple@linus.mongohq.com:10054/app21390246')
    db = client["app21390246"]
    collection = db["geolocations"]

    for postcode in g.data:
        pprint(postcode)
        collection.insert(postcode)
