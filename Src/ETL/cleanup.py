import os


def cleanup_data(context):
    data_size = context.op_config["data_size"]

    # Define paths for different steps
    paths_to_remove = [
        f"../Data/Extract/{data_size}/data.json",
        f"../Data/Transform/{data_size}/data.parquet",
        f"../Data/Load/{data_size}/data.parquet",
    ]

    # Attempt to remove each file
    for path in paths_to_remove:
        if os.path.exists(path):
            try:
                os.remove(path)
                context.log.info(f"Successfully removed file: {path}")
            except Exception as e:  # pragma: no cover
                context.log.error(f"Failed to remove file: {path}. Error: {e}")
        else:
            context.log.warning(f"File not found (nothing to remove): {path}")
