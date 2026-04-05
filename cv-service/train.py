import os
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
from torchvision import transforms
from PIL import Image
import json

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


class EwasteDataset(Dataset):
    def __init__(self, image_dir, transform=None):
        self.samples = []
        self.transform = transform
        for class_idx, class_name in enumerate(EWASTE_CLASSES):
            class_dir = os.path.join(image_dir, class_name)
            if not os.path.exists(class_dir):
                continue
            for fname in os.listdir(class_dir):
                if fname.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                    self.samples.append((os.path.join(class_dir, fname), class_idx))

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        path, label = self.samples[idx]
        image = Image.open(path).convert("RGB")
        if self.transform:
            image = self.transform(image)
        return image, label


def generate_synthetic_dataset(output_dir, samples_per_class=50):
    import numpy as np

    print("Generating synthetic training images for CV model...")
    for class_idx, class_name in enumerate(EWASTE_CLASSES):
        class_dir = os.path.join(output_dir, class_name)
        os.makedirs(class_dir, exist_ok=True)
        for i in range(samples_per_class):
            h, w = np.random.randint(100, 300, 2)
            if class_name == "mobile_phone":
                color = (
                    30 + np.random.randint(20),
                    30 + np.random.randint(20),
                    30 + np.random.randint(20),
                )
            elif class_name == "laptop":
                color = (
                    180 + np.random.randint(40),
                    180 + np.random.randint(40),
                    180 + np.random.randint(40),
                )
            elif class_name == "circuit_board":
                color = (
                    20 + np.random.randint(30),
                    100 + np.random.randint(50),
                    20 + np.random.randint(30),
                )
            elif class_name == "battery":
                color = (
                    30 + np.random.randint(20),
                    30 + np.random.randint(20),
                    150 + np.random.randint(50),
                )
            elif class_name == "cable_wire":
                color = (
                    20 + np.random.randint(20),
                    20 + np.random.randint(20),
                    20 + np.random.randint(20),
                )
            elif class_name == "monitor_screen":
                color = (
                    10 + np.random.randint(20),
                    10 + np.random.randint(20),
                    10 + np.random.randint(20),
                )
            elif class_name == "appliance":
                color = (
                    200 + np.random.randint(40),
                    200 + np.random.randint(40),
                    200 + np.random.randint(40),
                )
            else:
                color = tuple(np.random.randint(50, 200, 3))

            img = Image.new("RGB", (w, h), color)
            img.save(os.path.join(class_dir, f"synthetic_{i:03d}.jpg"))
    print(
        f"Generated {samples_per_class * len(EWASTE_CLASSES)} synthetic images in {output_dir}"
    )


def train_model(data_dir, epochs=10, batch_size=16, save_path="ewaste_classifier.pth"):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Training on: {device}")

    if not os.path.exists(data_dir) or len(os.listdir(data_dir)) == 0:
        print("No data found. Generating synthetic dataset...")
        generate_synthetic_dataset(data_dir)

    transform_train = transforms.Compose(
        [
            transforms.Resize((224, 224)),
            transforms.RandomHorizontalFlip(),
            transforms.RandomRotation(15),
            transforms.ColorJitter(brightness=0.2, contrast=0.2),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )

    dataset = EwasteDataset(data_dir, transform=transform_train)
    if len(dataset) == 0:
        print("No images found. Exiting.")
        return

    train_size = int(0.8 * len(dataset))
    val_size = len(dataset) - train_size
    train_dataset, val_dataset = torch.utils.data.random_split(
        dataset, [train_size, val_size]
    )

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=batch_size)

    model = EwasteClassifier(num_classes=len(EWASTE_CLASSES)).to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    scheduler = optim.lr_scheduler.StepLR(optimizer, step_size=5, gamma=0.5)

    best_val_acc = 0
    for epoch in range(epochs):
        model.train()
        running_loss = 0
        correct = 0
        total = 0
        for images, labels in train_loader:
            images, labels = images.to(device), labels.to(device)
            optimizer.zero_grad()
            outputs = model(images)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
            _, predicted = torch.max(outputs, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()

        train_acc = correct / total
        scheduler.step()

        model.eval()
        val_correct = 0
        val_total = 0
        with torch.no_grad():
            for images, labels in val_loader:
                images, labels = images.to(device), labels.to(device)
                outputs = model(images)
                _, predicted = torch.max(outputs, 1)
                val_total += labels.size(0)
                val_correct += (predicted == labels).sum().item()

        val_acc = val_correct / val_total if val_total > 0 else 0
        print(
            f"Epoch {epoch + 1}/{epochs} | Loss: {running_loss / len(train_loader):.4f} | Train Acc: {train_acc:.4f} | Val Acc: {val_acc:.4f}"
        )

        if val_acc > best_val_acc:
            best_val_acc = val_acc
            torch.save(model.state_dict(), save_path)
            print(f"  -> Saved best model (val_acc: {val_acc:.4f})")

    print(f"Training complete. Best val accuracy: {best_val_acc:.4f}")
    return model


if __name__ == "__main__":
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    model_path = os.path.join(os.path.dirname(__file__), "..", "ewaste_classifier.pth")
    train_model(data_dir, epochs=15, batch_size=16, save_path=model_path)
