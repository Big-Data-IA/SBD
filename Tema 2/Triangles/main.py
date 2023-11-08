def is_triangle(a, b, c):
    return a + b > c and a + c > b and b + c > a

def classify_triangle(a, b, c):
    if a == b == c:
        return "Equilateral"
    elif a == b or a == c or b == c:
        return "Isosceles"
    else:
        return "Scalene"

def main():
    while True:
        try:
            a = int(input("Enter the length of the first side: "))
            b = int(input("Enter the length of the second side: "))
            c = int(input("Enter the length of the third side:"))

            if a <= 0 or b <= 0 or c <= 0:
                print("Side lengths must be positive natural numbers.")
                continue

            if not is_triangle(a, b, c):
                print("These side lengths cannot form a triangle.")
            else:
                triangle_type = classify_triangle(a, b, c)
                print(f"This is an {triangle_type} triangle.")
            
        except ValueError:
            print("Please enter valid natural numbers.")

        another = input("Do you want to check another triangle (y/n)? ").strip().lower()
        if another != 'y':
            break

if __name__ == "__main__":
    main()
