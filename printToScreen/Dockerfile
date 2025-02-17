# השתמש בתמונה רשמית של Python
FROM python:3.8-slim

# הגדרת תיקיית עבודה בתוך הקונטיינר
WORKDIR /app

# העתק את כל הקבצים מהתיקייה המקומית לתוך הקונטיינר
COPY . /app

# התקנת התלויות (אם יש לך קובץ requirements.txt)
RUN pip install --no-cache-dir -r requirements.txt

# הפעל את הסקריפט של הפייתון
CMD ["python3", "printToScreen.py"]
