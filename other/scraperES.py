import requests
import json

business_id = '4AErMBEoNzbk7Q8g45kKaQ'
unix_time = 1546047836
filename = "/Users/anurag/Desktop/gpt.json"

api_key = 'ViYxOHxzCLRrO-zEzJIkak-LPmRl6Mw0uTu2EjcwHk_yKMgRZiX1U5yIu_QGR4TRGsZ8ma5hPjLwvDh_rdeD9-HQ1Nes7BCaQad5ACgGjCxYAQDsJaAYP9g9ENbjY3Yx'
endpoint = 'https://api.yelp.com/v3/businesses/search'
headers = {'Authorization': f'Bearer {api_key}'}

data = {'business': []}

cuisines = ["mexican", "italian", "indian", "thai", "chinese", "american", "Mediterranean"]
j = 0

with open(filename, "w") as file_object:
    file_object.write(json.dumps(""))

for c in cuisines:
    for i in range(50, 1000, 50):
        print(i)

        params = {'term': f'{c} restaurants',
                  'limit': 50,
                  'offset': i,
                  'location': 'Manhatten'}

        response = requests.get(url=endpoint,
                                params=params,
                                headers=headers)

        business_data = response.json()

        businesses = business_data.get('businesses', [])

        for business in businesses:
            j += 1

            temp = {"_index": "dineout", "_id": str(j)}

            a = {"index": temp}

            b = {'id': business.get('id', ''), 'category': c}

            with open(filename, "a") as file_object:
                file_object.write(json.dumps(a))
                file_object.write("\n")
                file_object.write(json.dumps(b))
                file_object.write("\n")

data['business'].append(b)

