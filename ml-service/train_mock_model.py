import pandas as pd
from sklearn.linear_model import LinearRegression
import pickle

def train():
    # Pollachi specific mock data
    data = {
        'sales_import_tonnes': [40, 50, 60, 70, 80],
        'population_millions': [0.11, 0.12, 0.13, 0.14, 0.15],
        'disposal_amount_tonnes': [15, 20, 25, 28, 30]
    }
    df = pd.DataFrame(data)
    X = df[['sales_import_tonnes', 'population_millions']]
    y = df['disposal_amount_tonnes']

    model = LinearRegression()
    model.fit(X, y)

    with open('model.pkl', 'wb') as f:
        pickle.dump(model, f)
    print("AI Model Trained for Pollachi, Tamil Nadu")

if __name__ == "__main__":
    train()
