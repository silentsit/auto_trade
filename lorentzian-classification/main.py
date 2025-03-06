import os
import sys
import subprocess

# Install dependencies first
dependencies = [
    "fastapi",
    "uvicorn",
    "aiohttp",
    "pydantic",
    "pydantic-settings", 
    "python-dotenv",
    "prometheus-client",
    "redis",
    "pytz",
    "holidays"
]

print("Installing dependencies...")
subprocess.check_call([sys.executable, "-m", "pip", "install"] + dependencies)

# Now run the original script
print("Starting application...")
os.system("python python_bridge.py")
