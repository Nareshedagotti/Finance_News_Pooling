# tls_client.py — import this in db_loader.py and your FastAPI app

import os
from pymongo import MongoClient

try:
    import certifi
    TLS_CA_FILE = certifi.where()
except Exception:
    TLS_CA_FILE = None

def connect_mongo(uri: str) -> MongoClient:
    allow_invalid = os.getenv("MONGO_ALLOW_INVALID_CERTS", "0") == "1"
    kwargs = dict(
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        retryWrites=True,
    )
    is_tls = uri.startswith("mongodb+srv://") or "tls=true" in uri.lower() or "ssl=true" in uri.lower()
    if is_tls:
        if allow_invalid:
            kwargs["tlsAllowInvalidCertificates"] = True
        elif TLS_CA_FILE:
            kwargs["tlsCAFile"] = TLS_CA_FILE
    return MongoClient(uri, **kwargs)
