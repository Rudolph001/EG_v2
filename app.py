
import os
import logging
from flask import Flask
from database import init_database

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Create the app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")

# Initialize database
init_database()

# Import routes after app creation to avoid circular imports
from routes import *

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
