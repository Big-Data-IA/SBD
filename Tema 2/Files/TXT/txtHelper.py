# Function to create a new text file
def create_text_file(filename, content):
    with open(filename, 'w') as file:
        file.write(content)
    print(f"File '{filename}' created successfully!")

# Function to modify an existing text file
def append_data_text_file(filename, new_content):
    with open(filename, 'a') as file:
        file.write(new_content)
    print(f"File '{filename}' modified successfully!")

# Function to read and display the contents of a text file
def read_text_file(filename):
    try:
        with open(filename, 'r') as file:
            content = file.read()
            print(f"Contents of '{filename}':\n{content}")
    except FileNotFoundError:
        print(f"File '{filename}' not found.")

# Function to delete a text file
def delete_text_file(filename):
    import os
    try:
        os.remove(filename)
        print(f"File '{filename}' deleted successfully!")
    except FileNotFoundError:
        print(f"File '{filename}' not found.")
    except PermissionError:
        print(f"Permission denied for file '{filename}'.")
