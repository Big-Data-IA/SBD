#requires pip install Pillow
from PIL import Image
import os

# Function to read and display an image
def read_and_display_image(image_path):
    try:
        img = Image.open(image_path)
        img.show()
        return img
    except Exception as e:
        print(f"Error: {e}")
        return None

# Function to delete an image
def delete_image(image_path):
    try:
        os.remove(image_path)
        print(f"Image '{image_path}' has been deleted.")
    except FileNotFoundError:
        print(f"Image '{image_path}' does not exist.")
    except Exception as e:
        print(f"Error: {e}")

# Input image path
image_path = "example.jpg"  # Replace with the path to your image

# Read and display the image
image = read_and_display_image(image_path)

if image:
    # Prompt the user to confirm deletion
    user_input = input("Do you want to delete this image? (yes/no): ").strip().lower()

    if user_input == "yes":
        # Delete the image
        delete_image(image_path)
    else:
        print("Image not deleted.")