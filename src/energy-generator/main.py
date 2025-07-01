import csv,json,os

# Readsing the CSV file
def read_energy_data():
    csv_path = '../../data/energy_data.csv'

    # Checks if the CSV file exists , Shows an error and stop execution if file doesn't exist
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f" The CSV file was not found at: {csv_path}")
    # reading the csv file
    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)
        data = list(reader)
        return data

# Main function
def main():
    try:
        data = read_energy_data()
        # Save the data into a JSON file
        with open('../../data/output1.json', 'w') as json_file:
            json.dump(data, json_file, indent=2) 
        print("Container 1: Generated", len(data), "records.")
    except FileNotFoundError as e:
        # Catch the error and print it
        print(e)

if __name__ == "__main__":
    main()