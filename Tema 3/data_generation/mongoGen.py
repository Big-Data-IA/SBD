from faker import Faker
import json
import pymongo
import random
import string


fake = Faker()

def generate_tournament():
    tournament = {
        "Evento": fake.sentence(nb_words=4),
        "Fecha": fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
        "Descripcion": fake.sentence(nb_words=8)
    }
    return tournament

tournament_list = [generate_tournament() for _ in range(100)]

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["pokemon_events_db"]  

users_collection = db["events_collection"]

users_collection.insert_many(tournament_list)

with open("Tema 5/data_bda/mongodb/tournament_data.json", "w") as file:
    json.dump(tournament_list, file, indent=2)


