# main.py

from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np

# Define the input data schema using Pydantic
# This ensures that the incoming data has the correct format and types.
class BiophysicalData(BaseModel):
    age: float
    sex: float
    bmi: float
    bp: float
    s1: float
    s2: float
    s3: float
    s4: float
    s5: float
    s6: float

# Initialize FastAPI app with metadata
app = FastAPI(
    title="Diabetes Risk Prediction API",
    description="A simple API to predict diabetes risk based on biophysical data.",
    version="1.0.0"
)

# Load the pre-trained, optimized XGBoost model
try:
    model = joblib.load("diabetes_model_xgb.pkl")
    print("Model loaded successfully.")
except FileNotFoundError:
    print("Error: The 'diabetes_model_xgb.pkl' file was not found. Please ensure it is in the same directory as main.py.")
    model = None

# Define the prediction endpoint
@app.post("/predict")
def predict_diabetes(data: BiophysicalData):
    """
    Predicts diabetes progression based on a patient's biophysical data.

    Args:
        data (BiophysicalData): A Pydantic model containing the patient's data.

    Returns:
        dict: A dictionary containing the predicted diabetes progression value.
    """
    if not model:
        return {"error": "Model not loaded. Please check server logs."}

    # Convert the Pydantic object to a numpy array for the model
    # The order of features here must match the order the model was trained on.
    input_data = np.array([
        data.age, data.sex, data.bmi, data.bp, data.s1,
        data.s2, data.s3, data.s4, data.s5, data.s6
    ]).reshape(1, -1)
    
    # Make the prediction using the loaded model
    prediction = model.predict(input_data)
    
    # Return the prediction as a JSON response
    return {"predicted_progression": float(prediction[0])}

