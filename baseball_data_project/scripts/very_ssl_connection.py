import ssl
import socket
import requests
import ssl
import socket

url = "https://retrosheet.org/events/2023eve.zip"

try:
    response = requests.get(url)
    # Continue with processing the response...
except requests.exceptions.RequestException as e:
    print(f"Error: {e}")