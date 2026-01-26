from faker import Faker
import json
import random

fake = Faker()

def generate_pokemon():
    pokemon = {
        "Nombre": fake.name(),
        "Tipo": fake.word().capitalize() + "/" + fake.word().capitalize(),
        "Estadísticas": {
            "HP": random.randint(1, 100),
            "Ataque": random.randint(1, 100),
            "Defensa": random.randint(1, 100),
            "Velocidad": random.randint(1, 100)
        },
        "Habilidades": [ random.choice(["Fuerza Bruta", "Sosegao", "Falso", "Jamoncio","LLoroso"])],
        "Evoluciones": [
            {"Nombre": fake.name(), "Tipo": fake.word().capitalize() + "/" + fake.word().capitalize()} for _ in range(random.randint(0, 3))
        ]
    }
    return pokemon

pokemon_list = [generate_pokemon() for _ in range(100)]

with open("Tema 5/data_bda/json/pokemon_data.json", "w") as file:
    json.dump(pokemon_list, file, indent=2)
