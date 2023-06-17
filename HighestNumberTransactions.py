import csv

symbols = ['ZIL', 'DASH', 'AAVE', 'BTC', 'DOGE']
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
        arrayNumberTransactions.append(
            int(float(headerArray[i-1]["NUMBER_OF_TRADES"])))

for i in range(5):
    max = 0
    tmpSymbol = ''
    for symbol in symbols:
        if symbol_map[symbol][i] > max:
            max = symbol_map[symbol][i]
            tmpSymbol = symbol
    print(tmpSymbol, "", max)
