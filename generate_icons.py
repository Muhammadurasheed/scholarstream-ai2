
from PIL import Image, ImageDraw

import os

def create_icon(size, filename):
    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    img = Image.new('RGBA', (size, size), color = (0, 0, 0, 0)) # Transparent bg
    d = ImageDraw.Draw(img)
    
    # Blue circle
    d.ellipse([0, 0, size, size], fill=(59, 130, 246)) # Tailwind blue-500
    
    # "SS" text (simplified as a white rect for small sizes)
    if size >= 48:
        # Draw rough "S" shape or just white box
        d.rectangle([size//4, size//4, size*3//4, size*3//4], fill="white")
    else:
        d.rectangle([size//3, size//3, size*2//3, size*2//3], fill="white")
        
    img.save(filename)
    print(f"Created {filename}")

if __name__ == "__main__":
    create_icon(16, "extension/public/icons/icon16.png")
    create_icon(48, "extension/public/icons/icon48.png")
    create_icon(128, "extension/public/icons/icon128.png")
