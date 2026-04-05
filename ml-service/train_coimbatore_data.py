import pandas as pd
from sklearn.linear_model import LinearRegression
import pickle
import os
import glob

def train_on_coimbatore_data():
    # Use the sample files we created in the root sample_data folder
    # Assuming we are running this from root or ml-service
    
    # Let's find the sample_data directory
    # If running from ml-service, its parent is root
    # If running from root, it's ./sample_data
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(current_dir)
    sample_dir = os.path.join(root_dir, 'sample_data')
    
    csv_files = glob.glob(os.path.join(sample_dir, '*.csv'))
    
    if not csv_files:
        print(f"Error: No sample data files found in {sample_dir}")
        return

    frames = []
    for f in csv_files:
        df = pd.read_csv(f)
        frames.append(df)
        print(f"Loading {os.path.basename(f)} - {len(df)} records")
        
    all_data = pd.concat(frames, ignore_index=True)
    
    # Target features
    X = all_data[['sales_import_tonnes', 'population_millions']]
    y = all_data['disposal_amount_tonnes']

    model = LinearRegression()
    model.fit(X, y)

    # Save to the same folder as app.py
    model_path = os.path.join(current_dir, 'model.pkl')
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
        
    print(f"AI Model Trained on {len(all_data)} total records from Coimbatore and surrounding areas.")
    print(f"Model saved to {model_path}")

if __name__ == "__main__":
    train_on_coimbatore_data()
