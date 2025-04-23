import requests
import pandas as pd
import time
import boto3

locations = [
    {"city": "Delhi", "lat": 28.6100, "lon": 77.2300},
    {"city": "Mumbai", "lat": 19.0761, "lon": 72.8775},
    {"city": "Kolkāta", "lat": 22.5675, "lon": 88.3700},
    {"city": "Bangalore", "lat": 12.9789, "lon": 77.5917},
    {"city": "Chennai", "lat": 13.0825, "lon": 80.2750},
    {"city": "Hyderābād", "lat": 17.3617, "lon": 78.4747},
    {"city": "Pune", "lat": 18.5203, "lon": 73.8567},
    {"city": "Ahmedabad", "lat": 23.0225, "lon": 72.5714},
    {"city": "Sūrat", "lat": 21.1702, "lon": 72.8311},
    {"city": "Lucknow", "lat": 26.8500, "lon": 80.9500},
    {"city": "Jaipur", "lat": 26.9000, "lon": 75.8000},
    {"city": "Kanpur", "lat": 26.4499, "lon": 80.3319},
    {"city": "Mirzāpur", "lat": 25.1460, "lon": 82.5690},
    {"city": "Nāgpur", "lat": 21.1497, "lon": 79.0806},
    {"city": "Ghāziābād", "lat": 28.6700, "lon": 77.4200},
    {"city": "Supaul", "lat": 26.1260, "lon": 86.6050},
    {"city": "Vadodara", "lat": 22.3000, "lon": 73.2000},
    {"city": "Rājkot", "lat": 22.3000, "lon": 70.7833},
    {"city": "Vishākhapatnam", "lat": 17.7042, "lon": 83.2978},
    {"city": "Indore", "lat": 22.7167, "lon": 75.8472},
    {"city": "Thāne", "lat": 19.1972, "lon": 72.9722},
    {"city": "Bhopāl", "lat": 23.2599, "lon": 77.4126},
    {"city": "Pimpri-Chinchwad", "lat": 18.6186, "lon": 73.8037},
    {"city": "Patna", "lat": 25.5940, "lon": 85.1376},
    {"city": "Bilāspur", "lat": 22.0900, "lon": 82.1500},
    {"city": "Ludhiāna", "lat": 30.9100, "lon": 75.8500},
    {"city": "Āgra", "lat": 27.1800, "lon": 78.0200},
    {"city": "Madurai", "lat": 9.9252, "lon": 78.1198},
    {"city": "Jamshedpur", "lat": 22.7925, "lon": 86.1842},
    {"city": "Prayagraj", "lat": 25.4358, "lon": 81.8464},
    {"city": "Nāsik", "lat": 19.9975, "lon": 73.7898}
]

#Extract Data
def extract_data(lat , lon):
    #The API Endpoint
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    #A GET request to the api
    response = requests.get(url)
    #Print the response
    return response.json()

# Transform Data
def transform_data(raw_data , city):
    records = []
    latitude = raw_data['latitude']
    longitude = raw_data['longitude']
    units = raw_data['current_weather_units']
    current = raw_data['current_weather']
    records.append({
        'city':city,
        'latitude':latitude,
        'longitude':longitude,
        'temperature_unit':units['temperature'],
        'windspeed_unit':units['windspeed'],
        'time':current['time'],
        'temperature':current['temperature'],
        'windspeed':current['windspeed']
                })
    return records

#Load Data
def Load(data , filename='weather_multiple_cities.csv' , bucket_name='weatherdata10'):
    df = pd.DataFrame(data)
    df.to_csv(filename , index=False)
    s3 = boto3.client('s3')
    s3.upload_file(filename, bucket_name, 'weather.csv')
    print(f"Data saved to s3")


if __name__ == '__main__':
    all_records = []
    for loc in locations:
        raw_data = extract_data(loc['lat'], loc['lon'])
        city_records = transform_data(raw_data , loc['city'])
        all_records.extend(city_records)
        time.sleep(1) #respectful delay for API
    print(raw_data)

    Load(all_records)



