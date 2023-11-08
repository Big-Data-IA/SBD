from Models.BookStore import Bookstore
def main():
    bookstore = Bookstore()

    while True:
        print("\nBookstore Console")
        print("1. Add a book")
        print("2. List books")
        print("3. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            title = input("Enter the book title: ")
            author = input("Enter the author's name: ")
            isbn = input("Enter the ISBN: ")
            bookstore.add_book(title, author, isbn)
            print("Book added successfully!")
        elif choice == "2":
            if not bookstore.books:
                print("No books in the store yet.")
            else:
                print("List of books:")
                bookstore.list_books()
        elif choice == "3":
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()