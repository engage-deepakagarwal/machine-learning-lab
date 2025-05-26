import modal
from modal import Image

# Setup

app = modal.App("hello")
image = Image.debian_slim().pip_install("requests") # Infrastructure as code
# This is a simple Modal app that returns a greeting with the location of the server.

# Hello!

@app.function(image=image)
def hello() -> str:
    import requests
    
    response = requests.get('https://ipinfo.io/json') # Get the server's location based on its IP address
    data = response.json()
    city, region, country = data['city'], data['region'], data['country']
    return f"Hello from {city}, {region}, {country}!!"
