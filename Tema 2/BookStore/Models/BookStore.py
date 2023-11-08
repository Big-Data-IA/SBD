from Models.Book import Book
class Bookstore:
    def __init__(self):
        self.books = []

    def add_book(self, title, author, isbn):
        book = Book(title, author, isbn)
        self.books.append(book)

    def list_books(self):
        for i, book in enumerate(self.books):
            print(f"{i+1}. Title: {book.title}, Author: {book.author}, ISBN: {book.isbn}")