from txtHelper import *

# Example usage
while True:
    print("\nMenu:")
    print("1. Create a new text file")
    print("2. Modify an existing text file")
    print("3. Read the contents of a text file")
    print("4. Delete a text file")
    print("5. Exit")
    
    choice = input("Enter your choice (1/2/3/4/5): ")
    
    if choice == '1':
        filename = input("Enter the filename for the new text file: ")
        content = input("Enter the content for the new file: ")
        create_text_file(filename, content)
    elif choice == '2':
        filename = input("Enter the filename of the text file: ")
        new_content = input("Enter the content to append to the file: ")
        append_data_text_file(filename, new_content)
    elif choice == '3':
        filename = input("Enter the filename to read: ")
        read_text_file(filename)
    elif choice == '4':
        filename = input("Enter the filename to delete: ")
        delete_text_file(filename)
    elif choice == '5':
        print("Goodbye!")
        break
    else:
        print("Invalid choice. Please select a valid option.")
