import csv

with open('ord.txt', 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    next(reader, None)
    read_lines = []
    included = [0, 1]
    for row in reader:
        content = list(row[i] for i in included)
        read_lines.append(content)
    print (read_lines)
