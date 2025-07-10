# Do not forget to create a variable in your console with the export command
# You can choose the city of your choice, you only need to write the name in english

# You will need to create a variable WEATHER with your key of OpenWeatherMap

import os 
import requests

KEY = os.getenv("WEATHER")
CITY = "london"

print(KEY)

r = requests.get(
    url="http://api.openweathermap.org/geo/1.0/direct?q={}&limit=5&appid=={}".format(
        CITY, KEY
    )
)

print(r.status_code)
print(r.json())

#data = r.json()

#print(data)

#clean_data = {i: data[i] for i in ["weather", "main"]}

#clean_data["weather"] = clean_data["weather"][0]
