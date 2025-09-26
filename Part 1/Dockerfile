# Use a lightweight official Python image as a base
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code and the trained model
COPY main.py .
COPY diabetes_model_xgb.pkl .

# Expose the port the API runs on (default for Uvicorn)
EXPOSE 8000

# Define the command to run the application when the container starts
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]