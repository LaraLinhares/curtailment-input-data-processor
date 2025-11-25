import snowflake.connector
import pandas as pd
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import base64


class SnowflakeSession:
    def __init__(self, snowflake_credentials):
        self.credentials = snowflake_credentials
        self.private_key = self.__prepare_private_key(self.credentials['private'], self.credentials['passphrase'])
        self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __prepare_private_key(self, private_key, passphrase=None):
        if isinstance(private_key, str):
            if private_key.startswith('-----BEGIN'):
                # PEM format - convert to DER
                password = passphrase.encode() if passphrase else None
                key = load_pem_private_key(private_key.encode(), password=password)
                der_key = key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                return der_key
            else:
                # Assume it's already base64-encoded DER
                return base64.b64decode(private_key)
        return private_key

    def connect(self):
        if self.connection is not None:
            return {"success": True, "message": "Already connected", "connection_id": str(id(self.connection))}

        try:
            self.connection = snowflake.connector.connect(
                user=self.credentials['username'],
                account=self.credentials['account'],
                private_key=self.private_key,
            )

            # Test the connection by executing a simple query
            cursor = self.connection.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                connection_id = str(id(self.connection))
                return {"success": True, "message": "Snowflake connection established successfully", "connection_id": connection_id}
            finally:
                cursor.close()

        except Exception as e:
            self.connection = None
            raise ConnectionError(f"Failed to connect to Snowflake: {str(e)}") from e 

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def query_to_dataframe(self, sql_query):
        if self.connection is None:
            raise RuntimeError("No active connection. Call connect() first.")
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        finally:
            cursor.close()



