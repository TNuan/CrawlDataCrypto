import csv
import requests
import json


page = requests.get(
    'https://api-dev.nestquant.com/data/symbols?category=crypto')
data = json.loads(page.text)
symbols = data['symbols']
# symbols = ['AXS', 'BTC','QNT','SOL','WOO','ZEC','TRX']

symbol_map = dict(map(lambda x: (x, []), symbols))

for symbol in symbols:
    arrayNumberTransactions = symbol_map[symbol]
    with open('combined_data/'+symbol+'.csv') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        headers = next(csv_reader)
        indexs = list(headers)
        preNumber = 0
        headerArray = list(csv_reader)

        for i in range(0, len(headerArray)):
            row = headerArray[i]
            if row[indexs[-1]] == "------":
                arrayNumberTransactions.append(
                    int(float(headerArray[i-1]["NUMBER_OF_TRADES"])))
            if row[indexs[-2]] == "------":
                arrayNumberTransactions.append(
                    int(float(headerArray[i-1]["NUMBER_OF_TRADES"])))
            if row[indexs[-3]] == "------":
                arrayNumberTransactions.append(
                    int(float(headerArray[i-1]["NUMBER_OF_TRADES"])))
            if row[indexs[-4]] == "------":
                arrayNumberTransactions.append(
                    int(float(headerArray[i-1]["NUMBER_OF_TRADES"])))
            if row[indexs[-5]] == "------":
                arrayNumberTransactions.append(
                    int(float(headerArray[i-1]["NUMBER_OF_TRADES"])))
for i in range(5):
    array = []
    for symbol in symbols:
        array.append({'name': symbol,
                      'numberTransactions': symbol_map[symbol][i]})
    sorted_array = sorted(
        array, key=lambda x: x['numberTransactions'], reverse=True)
    top_5_elements = sorted_array[:5]
    print("Khoang thoi gian thu ",i+1)
    for tmp in top_5_elements:
        print(tmp['name'])
        
    
