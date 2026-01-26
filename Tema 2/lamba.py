numbers = [1, 2, 3, 4, 5]
# Return the squares of each number using a nested lambda function
squares = list(map(lambda x: (lambda y: y * y)(x), numbers))
print(squares)  # Expected: [1, 4, 9, 16, 25]