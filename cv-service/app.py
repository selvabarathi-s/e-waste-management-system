import os
import io
import base64
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS
from PIL import Image
import torch
import torch.nn as nn
import torch.nn.functional as F
from torchvision import transforms

app = Flask(__name__)
CORS(app)

EWASTE_CLASSES = [
    "mobile_phone",
    "laptop",
    "circuit_board",
    "battery",
    "cable_wire",
    "monitor_screen",
    "appliance",
    "non_ewaste",
]

DEVICE_CATEGORIES = {
    "mobile_phone": "Mobile",
    "laptop": "IT_equipment",
    "circuit_board": "IT_equipment",
    "battery": "Batteries",
    "cable_wire": "IT_equipment",
    "monitor_screen": "IT_equipment",
    "appliance": "Large_appliances",
    "non_ewaste": None,
}

MODEL_PATH = os.path.join(os.path.dirname(__file__), "ewaste_classifier.pth")

transform = transforms.Compose(
    [
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ]
)


class EwasteClassifier(nn.Module):
    def __init__(self, num_classes=8):
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(3, 32, 3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(32, 64, 3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(64, 128, 3, padding=1),
            nn.BatchNorm2d(128),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(128, 256, 3, padding=1),
            nn.BatchNorm2d(256),
            nn.ReLU(),
            nn.AdaptiveAvgPool2d(1),
        )
        self.classifier = nn.Sequential(
            nn.Dropout(0.4),
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, num_classes),
        )

    def forward(self, x):
        x = self.features(x)
        x = torch.flatten(x, 1)
        x = self.classifier(x)
        return x


model = None
device = torch.device("cpu")


def load_model():
    global model
    model = EwasteClassifier(num_classes=len(EWASTE_CLASSES))
    if os.path.exists(MODEL_PATH):
        model.load_state_dict(
            torch.load(MODEL_PATH, map_location=device, weights_only=True)
        )
        print(f"Loaded CV model from {MODEL_PATH}")
    else:
        print(
            "No pretrained model found. Using random weights — train first for accurate results."
        )
    model.eval()


def predict_image(image_bytes):
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    input_tensor = transform(image).unsqueeze(0)

    with torch.no_grad():
        outputs = model(input_tensor)
        probs = F.softmax(outputs, dim=1)
        confidence, predicted = torch.max(probs, 1)

    idx = predicted.item()
    class_name = EWASTE_CLASSES[idx]
    confidence_val = confidence.item()

    all_probs = {cls: float(probs[0][i]) for i, cls in enumerate(EWASTE_CLASSES)}

    is_ewaste = class_name != "non_ewaste"
    device_category = DEVICE_CATEGORIES.get(class_name)

    return {
        "predicted_class": class_name,
        "confidence": round(confidence_val, 4),
        "is_ewaste": is_ewaste,
        "device_category": device_category,
        "all_probabilities": {k: round(v, 4) for k, v in all_probs.items()},
        "estimated_weight_kg": estimate_weight(class_name),
    }


def estimate_weight(class_name):
    estimates = {
        "mobile_phone": 0.2,
        "laptop": 2.5,
        "circuit_board": 0.3,
        "battery": 0.5,
        "cable_wire": 0.4,
        "monitor_screen": 5.0,
        "appliance": 10.0,
        "non_ewaste": 0,
    }
    return estimates.get(class_name, 0)


@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        {
            "ok": True,
            "model_loaded": model is not None,
            "classes": EWASTE_CLASSES,
            "device": str(device),
        }
    )


@app.route("/classify", methods=["POST"])
def classify():
    if "image" not in request.files and "image_b64" not in request.json:
        return jsonify(
            {"error": "Provide an image file or base64 in image_b64 field"}
        ), 400

    if "image" in request.files:
        image_bytes = request.files["image"].read()
    else:
        b64_str = request.json["image_b64"]
        if "," in b64_str:
            b64_str = b64_str.split(",")[1]
        image_bytes = base64.b64decode(b64_str)

    try:
        result = predict_image(image_bytes)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/classify/batch", methods=["POST"])
def classify_batch():
    files = request.files.getlist("images")
    if not files:
        return jsonify({"error": "Provide images as multipart files"}), 400

    results = []
    for f in files:
        try:
            result = predict_image(f.read())
            result["filename"] = f.filename
            results.append(result)
        except Exception as e:
            results.append({"filename": f.filename, "error": str(e)})

    ewaste_count = sum(1 for r in results if r.get("is_ewaste"))
    return jsonify(
        {
            "total": len(results),
            "ewaste_detected": ewaste_count,
            "non_ewaste": len(results) - ewaste_count,
            "results": results,
        }
    )


if __name__ == "__main__":
    load_model()
    app.run(host="0.0.0.0", port=5002, debug=True)
