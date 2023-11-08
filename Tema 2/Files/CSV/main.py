import csv
import os

# Function to create a new CSV file with sample data
def create_csv_file(filename):
    data = [
        ["Name", "Age", "City"],
        ["Alice", 30, "New York"],
        ["Bob", 25, "Los Angeles"],
        ["Charlie", 35, "Chicago"],
    ]

    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

    print(f"Created {filename}")

# Function to read and display the content of a CSV file
def read_csv_file(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            print(row)

# Function to modify a CSV file by adding a new row
def modify_csv_file(filename, new_data):
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(new_data)
    print(f"Modified {filename} by adding a new row")

# Function to delete a CSV file
def delete_csv_file(filename):
    if os.path.exists(filename):
        os.remove(filename)
        print(f"Deleted {filename}")
    else:
        print(f"{filename} does not exist.")

# Function to override a CSV file with new data
def override_csv_file(filename, new_data):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(new_data)
    print(f"Overridden {filename} with new data")

# Usage examples
create_csv_file("sample.csv")
read_csv_file("sample.csv")
modify_csv_file("sample.csv", ["David", 28, "Houston"])
read_csv_file("sample.csv")
override_csv_file("sample.csv", [["Eve", 40, "San Francisco"]])
read_csv_file("sample.csv")
delete_csv_file("sample.csv")
