from imports import Final, Path, load_dotenv, os

try:
    # Load environment variables from the '.env' file
    env_path = Path(__file__).parent / '.env'
    load_dotenv(dotenv_path=env_path, override=True)
except FileNotFoundError:
    print("Could not find .env file")
    raise
# Assign the environment variables to constants

try:
    TOKEN: Final[str] = os.environ.get("TOKEN")

except Exception as env_error:
    raise print(
        "Missing environment variables." "Make sure to set:" "TOKEN."
    ) from env_error
