from flask import Flask, render_template
import random

app = Flask(__name__)

# list of cat images
images = [
   "https://media.giphy.com/media/3o6Zt481isNVuQI1l6/giphy.gif",
    "https://media.giphy.com/media/BzyTuYCmvSORqs1ABM/giphy.gif",
    "https://media.giphy.com/media/mlvseq9yvZhba/giphy.gif",
    "https://media.giphy.com/media/MDJ9IbxxvDUQM/giphy.gif",
    "https://media.giphy.com/media/13CoXDiaCcCoyk/giphy.gif",
    "https://media.giphy.com/media/jpbnoe3UIa8TU8LM13/giphy.gif",
    "https://media.giphy.com/media/VbnUQpnihPSIgIXuZv/giphy.gif",
    "https://media.giphy.com/media/nR4L10XlJcSeQ/giphy.gif",
    "https://media.giphy.com/media/Lq0h93752f6J9tijrh/giphy.gif",
    "https://media.giphy.com/media/JIX9t2j0ZTN9S/giphy.gif",
    "https://media.giphy.com/media/C9x8gX02SnMIoAClXa/giphy.gif",
    "https://media.giphy.com/media/vFKqnCdLPNOKc/giphy.gif"
    ]

@app.route('/')
def index():
    url = random.choice(images)
    return render_template('index.html', url=url)

if __name__ == "__main__":
    app.run(host="0.0.0.0")
