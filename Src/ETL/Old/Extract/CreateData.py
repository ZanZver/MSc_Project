import json
from faker import Faker
from faker_vehicle import VehicleProvider
import os


def create_fake_data(seedNumber=42, numberOfCars=10):  # fakeCars:[]
    # Initialize Faker instance
    fake = Faker("en_GB")
    fake.add_provider(VehicleProvider)
    Faker.seed(seedNumber)

    # Generate 10 consistent fake cars
    fakeCars = []
    for _ in range(numberOfCars):
        vehicleInfo = fake.vehicle_object()
        carData = {
            "vin": fake.vin(),
            "license_plate": fake.license_plate(),
            "vehicle_make": vehicleInfo["Make"],
            "vehicle_model": vehicleInfo["Model"],
            "vehicle_year": vehicleInfo["Year"],
            "full_vehicleInfo": vehicleInfo,
            "vehicle_category": vehicleInfo["Category"],
            "vehicle_make_model": f"{vehicleInfo['Make']} {vehicleInfo['Model']}",
            "vehicle_year_make_model": f"{vehicleInfo['Year']} {vehicleInfo['Make']} {vehicleInfo['Model']}",
            "vehicle_year_make_model_cat": f"{vehicleInfo['Year']} {vehicleInfo['Make']} {vehicleInfo['Model']} ({vehicleInfo['Category']})",
        }
        fakeCars.append(carData)

    return fakeCars


def save_file(output_path, fake_cars):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save consistent fake cars to the specified JSON file
    with open(output_path, "w") as file:
        json.dump(fake_cars, file, indent=4)

    print(f"Fake car data has been saved to {output_path}")


def main():
    fake_cars = create_fake_data(seedNumber=42, numberOfCars=5000)

    # Define the output path
    output_path = "../../Data/Extract/Medium/data.json"

    save_file(output_path, fake_cars)


if __name__ == "__main__":
    main()
