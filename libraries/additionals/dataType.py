def check(data):
    for letter in data:
        if letter == '-' or letter == ':':
            return 'TIMESTAMP'
        if letter == '.':
            return 'FLOAT'
        if 'A' <= letter <= 'Z' or 'a' <= letter <= 'z':
            return 'VARCHAR(20)'
    if len(data):
        return 'INTEGER'
    return 'FLOAT'
