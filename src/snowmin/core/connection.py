import snowflake.connector
from pathlib import Path
from typing import Dict, Any, Optional
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def get_private_key(key_path: str, password: Optional[str] = None) -> bytes:
    """Load and deserialize a private key."""
    path = Path(key_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Private key file not found: {path}")

    with open(path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password.encode() if password else None,
            backend=default_backend(),
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pkb


class ConnectionManager:
    _connection = None
    _current_config = None

    @classmethod
    def get_connection(cls, conn_config: Optional[Dict[str, Any]] = None):
        """
        Get or create a Snowflake connection using provided config.
        Singleton pattern for the active session.

        Args:
            conn_config: Connection configuration dict with keys like:
                account, user, password, private_key_file, private_key_passphrase,
                role, warehouse, database, schema
        """
        # If config provided and different from current, close existing connection
        if conn_config and conn_config != cls._current_config:
            cls.close()

        if cls._connection is None:
            if not conn_config:
                raise ValueError("Connection config must be provided")

            cls._current_config = conn_config

            # Build connection args
            conn_args = {
                "account": conn_config.get("account"),
                "user": conn_config.get("user"),
            }

            # Add optional connection parameters
            if conn_config.get("role"):
                conn_args["role"] = conn_config["role"]
            if conn_config.get("warehouse"):
                conn_args["warehouse"] = conn_config["warehouse"]
            if conn_config.get("database"):
                conn_args["database"] = conn_config["database"]
            if conn_config.get("schema") or conn_config.get("schema_name"):
                conn_args["schema"] = conn_config.get("schema") or conn_config.get(
                    "schema_name"
                )

            # Authentication: priority is private_key > password > externalbrowser
            private_key_file = conn_config.get("private_key_file")
            private_key_passphrase = conn_config.get("private_key_passphrase")
            password = conn_config.get("password")

            if private_key_file:
                conn_args["private_key"] = get_private_key(
                    private_key_file, private_key_passphrase
                )
            elif password:
                conn_args["password"] = password
            else:
                # Default to external browser if no password or key provided
                conn_args["authenticator"] = "externalbrowser"

            try:
                cls._connection = snowflake.connector.connect(**conn_args)
            except Exception as e:
                raise RuntimeError(f"Failed to connect to Snowflake: {e}")

        return cls._connection

    @classmethod
    def execute(
        cls, query: str, params=None, conn_config: Optional[Dict[str, Any]] = None
    ):
        """Execute a query and return the cursor."""
        conn = cls.get_connection(conn_config)
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor
        except Exception as e:
            cursor.close()
            raise e

    @classmethod
    def fetch_all(
        cls, query: str, params=None, conn_config: Optional[Dict[str, Any]] = None
    ):
        """Execute and return all results as a list of dicts."""
        cursor = cls.execute(query, params, conn_config)
        try:
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    @classmethod
    def close(cls):
        if cls._connection:
            cls._connection.close()
            cls._connection = None
            cls._current_config = None
