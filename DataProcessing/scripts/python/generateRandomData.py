import csv
import random
import string

#function to generate random records.
def generate_random_records(num_records):
    records = []
    for _ in range(num_records):
        record = {
            'ID': ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
            'Name': ''.join(random.choices(string.ascii_letters, k=random.randint(5, 15))),
            'Age': random.randint(18, 80),
            'City': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Delhi', 'Mumbai', 'Pune', 'Banglore', 'Mysore', 'Goa', 'Thane', 'Palghar', 'selum'])
        }
        records.append(record)
    return records

#Writing the records to the csv file.
def write_records_to_csv(records, filename):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['ID', 'Name', 'Age', 'City']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

#main function: code execution starts here
if __name__ == "__main__":
    num_records = 10000000
    records = generate_random_records(num_records)
    write_records_to_csv(records, r'C:\Workspace\data file\\random_records.csv')

