import json

import boto3


class SSM:
    def __init__(self):
        self.__client = boto3.client("ssm")

    def get_parameter(self, name: str, decrypted: bool):
        response = self.__client.get_parameters(Names=[name], WithDecryption=decrypted)
        return json.loads(response["Parameters"][0]["Value"])