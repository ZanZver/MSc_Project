import json
from faker import Faker
from faker_vehicle import VehicleProvider
import os


def create_fake_data(size: str, seed: int = 42):
    fake = Faker("en_GB")
    fake.add_provider(VehicleProvider)
    Faker.seed(seed)

    match size.lower():
        case "small":
            num_cars = 1000
        case "medium":
            num_cars = 5000
        case "large":
            num_cars = 10000
        case _:
            num_cars = 1000

    fake_cars = []
    for _ in range(num_cars):
        vehicle_info = fake.vehicle_object()
        car_data = {
            "vin": fake.vin(),
            "license_plate": fake.license_plate(),
            "vehicle_make": vehicle_info["Make"],
            "vehicle_model": vehicle_info["Model"],
            "vehicle_year": vehicle_info["Year"],
            "full_vehicleInfo": vehicle_info,
            "vehicle_category": vehicle_info["Category"],
            "vehicle_make_model": f"{vehicle_info['Make']} {vehicle_info['Model']}",
            "vehicle_year_make_model": f"{vehicle_info['Year']} {vehicle_info['Make']} {vehicle_info['Model']}",
            "vehicle_year_make_model_cat": f"{vehicle_info['Year']} {vehicle_info['Make']} {vehicle_info['Model']} ({vehicle_info['Category']})",
        }
        fake_cars.append(car_data)

    output_path = f"../Data/Extract/{size}/data.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(fake_cars, f, indent=4)
