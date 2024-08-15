import pickle
from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# Load the saved Prophet model
with open('src/modelling/prophet.pkl', 'rb') as f:
    model = pickle.load(f)

@app.route('/predict', methods=['POST'])
def predict()->jsonify:
    """
    Endpoint to make predictions using the Prophet model.

    Returns:
        jsonify: JSON response containing the forecast with columns 'ds', 'yhat', 
        'yhat_lower', and 'yhat_upper'.
    """
    
    # Get the JSON data from the request
    data = request.get_json()

    # Convert the input data into a DataFrame
    df = pd.DataFrame(data['timestamp'], columns=['ds'])

    # Make predictions
    forecast = model.predict(df)

    # Convert the forecast result into a JSON serializable format
    forecast_json = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_dict(orient='records')

    # Return the forecast as JSON
    return jsonify(forecast_json)

if __name__ == '__main__':
    app.run(debug=True)
