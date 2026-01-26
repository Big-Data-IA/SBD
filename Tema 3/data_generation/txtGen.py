
import json
from faker import Faker
import random

def read_json_file(filename):
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        return None
    
    
dataPokemon=read_json_file("Tema 5/data_bda/json/pokemon_data.json")

dataEvents=read_json_file("Tema 5/data_bda/mongodb/tournament_data.json")


nombresPokemon = [node["Nombre"] for node in dataPokemon]

nombresEventos = [node["Evento"] for node in dataEvents]

fake = Faker()

def generate_battle_result():
    evento = random.choice(nombresEventos)
    entrenador = fake.name()
    equipo = [random.choice(nombresPokemon) for _ in range(6)]  
    resultado = random.choice(["Gano","Perdio"])  
    text = f"Evento: {evento}\nNombre del entrenador: {entrenador}\nEquipo de Pokemon: {equipo}\nResultado de la batalla: {resultado}\n"
    return text


battle_results = [generate_battle_result() for _ in range(100)]

with open("Tema 5/data_bda/text/battle_records.txt", "w") as file:
    file.writelines(battle_results)
