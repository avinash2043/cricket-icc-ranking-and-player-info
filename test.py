import requests

def get_sports_data():
    url = "https://hs-consumer-api.espncricinfo.com/v1/pages/matches/current?lang=en&latest=true"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an error for 4xx/5xx
        print(response.json())
    except requests.exceptions.RequestException as error:
        print("Error fetching data:", error)

get_sports_data()
